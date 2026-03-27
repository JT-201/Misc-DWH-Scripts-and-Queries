WITH eligible_members AS (
    SELECT 
        s.user_id,
        DATE_FORMAT(DATE_ADD(DATE_FORMAT(s.start_date, '%Y-%m-01'), INTERVAL 1 MONTH), '%Y-%m-01') AS eligible_start_month
    FROM subscriptions s
    INNER JOIN partner_employers pe ON s.user_id = pe.user_id
    WHERE s.status = 'ACTIVE'
      AND pe.name = 'Thermo Fisher'
),
eligible_months AS (
    SELECT
        em.user_id,
        em.eligible_start_month,
        DATE_FORMAT(m.month_date, '%Y-%m-01') AS expected_month
    FROM eligible_members em
    JOIN (
        SELECT DATE_FORMAT(DATE_ADD('2026-01-01', INTERVAL seq.n MONTH), '%Y-%m-01') AS month_date
        FROM (
            SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
            UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7
            UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11
            UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15
            UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19
            UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23
        ) seq
    ) m ON m.month_date >= em.eligible_start_month
       AND m.month_date <= '2026-02-01'
),
monthly_activity AS (
    SELECT
        ba.user_id,
        DATE_FORMAT(ba.created_at, '%Y-%m-01') AS activity_month,
        COUNT(*) AS interaction_count
    FROM billable_activities ba
    INNER JOIN eligible_members em ON ba.user_id = em.user_id
    WHERE ba.created_at >= em.eligible_start_month
      AND ba.created_at < '2026-03-01'
      AND ba.type IN (
          'WEIGHT_SCALE_PROVIDED',
          'VOICE_MESSAGE_CARE_ONLY',
          'VOICE_MESSAGE_CARE_AND_PCA',
          'USER_RECEIVED_MANUAL_MESSAGE',
          'TEXT_MESSAGE_CARE_ONLY',
          'TEXT_MESSAGE_CARE_AND_PCA',
          'SUBSCRIPTION_STARTED',
          'SELF_RECORD_HEMOGLOBIN_A1C',
          'SELF_RECORD_BODY_WEIGHT',
          'SELF_RECORD_BLOOD_PRESSURE',
          'SELF_RECORD_BLOOD_GLUCOSE',
          'REGISTRATION',
          'RECORD_WAIST_CIRCUMFERENCE_WITH_REVIEW',
          'RECORD_WAIST_CIRCUMFERENCE',
          'RECORD_STEPS',
          'RECORD_BODY_WEIGHT',
          'RECORD_BLOOD_PRESSURE_WITH_REVIEW',
          'RECORD_BLOOD_PRESSURE',
          'RECORD_BLOOD_GLUCOSE_WITH_REVIEW',
          'RECORD_BLOOD_GLUCOSE',
          'QUESTIONNAIRE_ANSWERED',
          'MEDICATION_CHANGE',
          'MEDICAL_QUESTIONNAIRE_ANSWERED_WITH_REVIEW',
          'MEDICAL_QUESTIONNAIRE_ANSWERED',
          'MEAL_PLAN_GENERATED',
          'MEAL_PLAN_COMPLETED',
          'MEAL_INSIGHT_CREATED',
          'LIPID_MEDICATION_PRESCRIBED',
          'HEALTH_QUESTIONNAIRE_ANSWERED',
          'GLUCOMETER_PROVIDED',
          'CONSUMED_DIGITAL_CONTENT_WITH_REVIEW',
          'CONSUMED_DIGITAL_CONTENT',
          'COMPLETED_LAB_TEST',
          'COMPLETED_CONSULTATION',
          'CHART_REVIEW_WITH_CARE_TEAM_MESSAGE',
          'CHART_REVIEW',
          'CGM_PROVIDED',
          'BLOODPRESSURE_MONITOR_PROVIDED',
          'APPOINTMENT_COMPLETED'
      )
    GROUP BY ba.user_id, DATE_FORMAT(ba.created_at, '%Y-%m-01')
),
member_compliance AS (
    SELECT
        em.user_id,
        COUNT(DISTINCT em.expected_month) AS total_expected_months,
        COUNT(DISTINCT CASE WHEN COALESCE(ma.interaction_count, 0) >= 4 THEN em.expected_month END) AS months_meeting_threshold
    FROM eligible_months em
    LEFT JOIN monthly_activity ma 
        ON em.user_id = ma.user_id 
        AND em.expected_month = ma.activity_month
    GROUP BY em.user_id
),
summary AS (
    SELECT
        COUNT(DISTINCT user_id) AS total_members,
        COUNT(DISTINCT CASE WHEN months_meeting_threshold = total_expected_months THEN user_id END) AS members_meeting_threshold
    FROM member_compliance
)
SELECT
    total_members,
    members_meeting_threshold,
    ROUND(members_meeting_threshold / total_members * 100, 2) AS pct_members_meeting_threshold
FROM summary;
