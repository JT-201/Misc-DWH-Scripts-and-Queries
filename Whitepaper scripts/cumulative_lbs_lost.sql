SELECT
    COUNT(DISTINCT bw.user_id)                                                      AS broad_n,
    ROUND(SUM(bw.baseline_lbs - bw.current_lbs), 1)                                AS broad_total_lbs_net,
    ROUND(SUM(CASE WHEN bw.baseline_lbs > bw.current_lbs
              THEN bw.baseline_lbs - bw.current_lbs ELSE 0 END), 1)                AS broad_total_lbs_losers_only,
    ROUND(AVG(bw.baseline_lbs - bw.current_lbs), 2)                                AS broad_avg_lbs_per_member,

    COUNT(DISTINCT CASE WHEN bw.is_consistent = 1 THEN bw.user_id END)             AS strict_n,
    ROUND(SUM(CASE WHEN bw.is_consistent = 1
              THEN bw.baseline_lbs - bw.current_lbs ELSE 0 END), 1)                AS strict_total_lbs_net,
    ROUND(SUM(CASE WHEN bw.is_consistent = 1 AND bw.baseline_lbs > bw.current_lbs
              THEN bw.baseline_lbs - bw.current_lbs ELSE 0 END), 1)                AS strict_total_lbs_losers_only,
    ROUND(AVG(CASE WHEN bw.is_consistent = 1
              THEN bw.baseline_lbs - bw.current_lbs END), 2)                        AS strict_avg_lbs_per_member

FROM (
    SELECT
        cohort.user_id,
        baseline_w.baseline_lbs,
        curr_w.current_lbs,
        CASE WHEN streak.max_streak >= 6 THEN 1 ELSE 0 END  AS is_consistent

    FROM (
        SELECT s.user_id, MIN(s.start_date) AS start_date
        FROM subscriptions s
        JOIN user_program_memberships upm ON upm.user_id = s.user_id
        JOIN (
            SELECT user_id
            FROM bmi_values_cleaned
            GROUP BY user_id
            HAVING MIN(value) >= 30
        ) bmi ON bmi.user_id = s.user_id
        WHERE s.status = 'ACTIVE'
          AND upm.program IN ('HEALTHY_WEIGHT_JOURNEY', 'weightloss')
        GROUP BY s.user_id
    ) cohort
    JOIN (
        SELECT user_id, value * 2.20462 AS baseline_lbs, effective_date AS baseline_dt
        FROM (
            SELECT
                bwc.user_id,
                bwc.value,
                bwc.effective_date,
                ROW_NUMBER() OVER (
                    PARTITION BY bwc.user_id
                    ORDER BY bwc.effective_date ASC
                ) AS rn
            FROM body_weight_values_cleaned bwc
            JOIN (
                SELECT s.user_id, MIN(s.start_date) AS start_date
                FROM subscriptions s
                JOIN user_program_memberships upm ON upm.user_id = s.user_id
                WHERE s.status = 'ACTIVE'
                  AND upm.program IN ('HEALTHY_WEIGHT_JOURNEY', 'weightloss')
                GROUP BY s.user_id
            ) sub ON sub.user_id = bwc.user_id
                AND bwc.effective_date >= DATE_SUB(sub.start_date, INTERVAL 30 DAY)
        ) ranked
        WHERE rn = 1
    ) baseline_w ON baseline_w.user_id = cohort.user_id
    JOIN (
        SELECT user_id, value * 2.20462 AS current_lbs, effective_date AS current_dt
        FROM (
            SELECT
                bwc.user_id,
                bwc.value,
                bwc.effective_date,
                ROW_NUMBER() OVER (
                    PARTITION BY bwc.user_id
                    ORDER BY bwc.effective_date DESC
                ) AS rn
            FROM body_weight_values_cleaned bwc
            JOIN (
                -- re-derive baseline date to enforce the 30-day gap
                SELECT bwc2.user_id, MIN(bwc2.effective_date) AS baseline_dt
                FROM body_weight_values_cleaned bwc2
                JOIN (
                    SELECT s.user_id, MIN(s.start_date) AS start_date
                    FROM subscriptions s
                    JOIN user_program_memberships upm ON upm.user_id = s.user_id
                    WHERE s.status = 'ACTIVE'
                      AND upm.program IN ('HEALTHY_WEIGHT_JOURNEY', 'weightloss')
                    GROUP BY s.user_id
                ) sub2 ON sub2.user_id = bwc2.user_id
                    AND bwc2.effective_date >= DATE_SUB(sub2.start_date, INTERVAL 30 DAY)
                GROUP BY bwc2.user_id
            ) bl ON bl.user_id = bwc.user_id
                AND DATEDIFF(bwc.effective_date, bl.baseline_dt) >= 30
        ) ranked
        WHERE rn = 1
    ) curr_w ON curr_w.user_id = cohort.user_id    LEFT JOIN (
        SELECT user_id, MAX(consecutive_months) AS max_streak
        FROM (
            SELECT user_id, streak_id, COUNT(*) AS consecutive_months
            FROM (
                SELECT
                    user_id,
                    activity_month,
                    SUM(is_new_streak) OVER (
                        PARTITION BY user_id ORDER BY activity_month
                    ) AS streak_id
                FROM (
                    SELECT
                        user_id,
                        DATE_FORMAT(effective_date, '%Y-%m-01')  AS activity_month,
                        CASE
                            WHEN DATEDIFF(
                                DATE_FORMAT(effective_date, '%Y-%m-01'),
                                LAG(DATE_FORMAT(effective_date, '%Y-%m-01'))
                                    OVER (PARTITION BY user_id ORDER BY DATE_FORMAT(effective_date, '%Y-%m-01'))
                            ) > 32
                            OR LAG(effective_date)
                                OVER (PARTITION BY user_id ORDER BY DATE_FORMAT(effective_date, '%Y-%m-01'))
                                IS NULL
                            THEN 1 ELSE 0
                        END AS is_new_streak
                    FROM body_weight_values_cleaned
                    GROUP BY user_id, DATE_FORMAT(effective_date, '%Y-%m-01')
                ) monthly
            ) streaks
            GROUP BY user_id, streak_id
        ) streak_lengths
        GROUP BY user_id
    ) streak ON streak.user_id = cohort.user_id
) bw
