// Custom Sql Query 1 (clinical member data)
WITH 
-- 1. DEFINE REPORT DATE
vars AS (
    SELECT CAST('2026-02-01' AS DATE) AS report_date
),

-- 2. ELIGIBLE USERS
base_users AS (
    SELECT 
        u.id AS user_id,
        u.readable_id,
        s.start_date,
        TIMESTAMPDIFF(DAY, s.start_date, v.report_date) AS days_since_start,
        CASE WHEN TIMESTAMPDIFF(DAY, s.start_date, v.report_date) >= 365 THEN 1 ELSE 0 END AS is_12mo_member
    FROM users u
    JOIN subscriptions s ON u.id = s.user_id
    CROSS JOIN vars v
    WHERE s.status = 'active' 
      AND s.cancellation_date IS NULL
      AND s.start_date <= v.report_date
      AND EXISTS (
          SELECT 1 FROM user_program_memberships upm 
          WHERE upm.user_id = u.id 
          AND upm.program IN ('HEALTHY_WEIGHT_JOURNEY', 'weightloss')
      )
),

-- 3. BILLABLE ENGAGEMENT
billable_stats_6mo AS (
    SELECT 
        b.user_id,
        COUNT(DISTINCT DATE_FORMAT(b.date, '%Y-%m')) AS billable_months_last_6mo
    FROM billable_user_statuses b
    JOIN vars v ON 1=1
    WHERE b.is_billable = 1
      AND b.date >= DATE_SUB(v.report_date, INTERVAL 6 MONTH)
      AND b.date <= v.report_date
    GROUP BY b.user_id
),

billable_stats_lifetime AS (
    SELECT 
        b.user_id,
        COUNT(DISTINCT DATE_FORMAT(b.date, '%Y-%m')) AS total_billable_months_lifetime
    FROM billable_user_statuses b
    JOIN vars v ON 1=1
    WHERE b.is_billable = 1
      AND b.date <= v.report_date
    GROUP BY b.user_id
),

-- 4. GLP-1 STATS
glp_stats AS (
    SELECT 
        p.patient_user_id AS user_id,
        SUM(p.days_of_supply * (1 + COALESCE(p.total_refills, 0))) AS total_days_covered
    FROM prescriptions p
    JOIN medication_ndcs mn ON p.prescribed_ndc = mn.ndc
    JOIN medications m ON mn.medication_id = m.id
    JOIN medication_drug_classes mdc ON m.id = mdc.medication_id
    JOIN vars v ON 1=1
    WHERE mdc.drug_class_name = 'GLP1'
      AND p.prescribed_at <= v.report_date
    GROUP BY p.patient_user_id
),

-- 5. VITALS (Ranks)
weight_ranks AS (
    SELECT user_id, effective_date, value AS weight,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY effective_date ASC) as rn_asc,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY effective_date DESC) as rn_desc
    FROM body_weight_values_cleaned b JOIN vars v ON 1=1 WHERE b.effective_date <= v.report_date
),
bmi_ranks AS (
    SELECT user_id, effective_date, value AS bmi,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY effective_date ASC) as rn_asc,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY effective_date DESC) as rn_desc
    FROM bmi_values_cleaned b JOIN vars v ON 1=1 WHERE b.effective_date <= v.report_date
),
bp_ranks AS (
    SELECT user_id, effective_date, systolic, diastolic,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY effective_date ASC) as rn_asc,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY effective_date DESC) as rn_desc
    FROM blood_pressure_values b JOIN vars v ON 1=1 WHERE b.effective_date <= v.report_date
),
a1c_ranks AS (
    SELECT user_id, effective_date, value AS a1c,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY effective_date ASC) as rn_asc,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY effective_date DESC) as rn_desc
    FROM a1c_values b JOIN vars v ON 1=1 WHERE b.effective_date <= v.report_date
),

-- 6. COMPLIANCE COUNTS
weight_compliance AS (
    SELECT user_id, COUNT(*) as months_with_10plus_weights
    FROM (
        SELECT b.user_id, DATE_FORMAT(b.effective_date, '%Y-%m') as mth, COUNT(*) as cnt
        FROM body_weight_values_cleaned b
        JOIN vars v ON 1=1
        WHERE b.effective_date <= v.report_date
        GROUP BY 1, 2
    ) sub
    WHERE cnt >= 10
    GROUP BY user_id
),
bp_compliance AS (
    SELECT user_id, COUNT(*) as months_with_5plus_bp
    FROM (
        SELECT b.user_id, DATE_FORMAT(b.effective_date, '%Y-%m') as mth, COUNT(*) as cnt
        FROM blood_pressure_values b
        JOIN vars v ON 1=1
        WHERE b.effective_date <= v.report_date
        GROUP BY 1, 2
    ) sub
    WHERE cnt >= 5
    GROUP BY user_id
),
a1c_counts AS (
    SELECT b.user_id, COUNT(*) as total_a1c_records
    FROM a1c_values b
    JOIN vars v ON 1=1
    WHERE b.effective_date <= v.report_date
    GROUP BY b.user_id
),

-- 7. NPS
nps_latest AS (
    SELECT qr.user_id, npsr.score, npsr.submitted_at,
           ROW_NUMBER() OVER(PARTITION BY qr.user_id ORDER BY npsr.submitted_at DESC) as rn
    FROM nps_response_records npsr
    JOIN questionnaire_records qr ON npsr.questionnaire_id = qr.questionnaire_id
    JOIN vars v ON 1=1
    WHERE npsr.submitted_at <= v.report_date
),

-- 8. CSAT SCORES (Normalized to 10-point scale)
csat_stats AS (
    SELECT 
        qr.user_id,
        COUNT(*) as csat_response_count,
        AVG(
            CASE 
                -- If it's the 10-point question, take raw value
                WHEN qr.question_id = 'LJ7hYbzFyc3o' AND qr.answer_text REGEXP '^[0-9]+(\.[0-9]+)?$' 
                     THEN CAST(qr.answer_text AS DECIMAL(4,2))
                
                -- If it's any of the other 5-point questions, Multiply by 2
                WHEN qr.question_id IN ('g4rg786D9C2q', 'UeNt46PgUv4A', 'AaIbKyE6VRkL', 'NRGXIAUDGjRv') 
                     AND qr.answer_text REGEXP '^[0-9]+(\.[0-9]+)?$' 
                     THEN CAST(qr.answer_text AS DECIMAL(4,2)) * 2
                ELSE NULL 
            END
        ) as average_csat_score
    FROM questionnaire_records qr
    JOIN vars v ON 1=1
    WHERE qr.questionnaire_id = 'fnZKqImJ'
      AND qr.question_id IN ('g4rg786D9C2q', 'UeNt46PgUv4A', 'AaIbKyE6VRkL', 'NRGXIAUDGjRv', 'LJ7hYbzFyc3o')
      AND qr.answered_at <= v.report_date
    GROUP BY qr.user_id
)

-- =======================================================
-- FINAL SELECT
-- =======================================================
SELECT 
    v.report_date AS report_generated_date,
    u.user_id,
    u.readable_id,
    pe.name AS employer_name,
    u.days_since_start,
    u.is_12mo_member,
    
    -- BILLING / ENGAGEMENT STATS
    COALESCE(bs.billable_months_last_6mo, 0) AS billable_months_last_6mo,
    COALESCE(bl.total_billable_months_lifetime, 0) AS total_billable_months_lifetime,

    -- COMPLIANCE COUNTS
    COALESCE(wc.months_with_10plus_weights, 0) AS count_months_10plus_weights,
    COALESCE(bpc.months_with_5plus_bp, 0) AS count_months_5plus_bp,
    COALESCE(ac.total_a1c_records, 0) AS count_total_a1c,

    -- NPS
    nps.score AS latest_nps_score,
    nps.submitted_at AS latest_nps_date,
    CASE 
        WHEN nps.score >= 9 THEN 'Promoter'
        WHEN nps.score >= 7 THEN 'Passive'
        WHEN nps.score <= 6 THEN 'Detractor'
        ELSE 'No Score'
    END AS nps_category,

    -- CSAT (Average Score is OUT OF 10)
    csat.average_csat_score,
    COALESCE(csat.csat_response_count, 0) AS csat_response_count,

    -- Meds
    COALESCE(gs.total_days_covered, 0) AS glp1_days_covered,
    CASE 
        WHEN u.days_since_start > 0 THEN (1 - (COALESCE(gs.total_days_covered, 0) / u.days_since_start)) 
        ELSE 0 
    END AS glp1_gap_percentage,

    -- Vitals
    w_base.weight AS weight_baseline, w_base.effective_date AS weight_base_date,
    w_curr.weight AS weight_current, w_curr.effective_date AS weight_curr_date,
    bmi_base.bmi AS bmi_baseline, bmi_curr.bmi AS bmi_current,
    bp_base.systolic AS sys_baseline, bp_base.diastolic AS dia_baseline,
    bp_curr.systolic AS sys_current, bp_curr.diastolic AS dia_current,
    a1c_base.a1c AS a1c_baseline, a1c_curr.a1c AS a1c_current,

    -- Flags
    CASE WHEN DATEDIFF(w_curr.effective_date, w_base.effective_date) >= 30 THEN 1 ELSE 0 END AS flag_weight_30days,
    CASE WHEN DATEDIFF(bp_curr.effective_date, bp_base.effective_date) >= 30 THEN 1 ELSE 0 END AS flag_bp_30days,
    CASE WHEN DATEDIFF(a1c_curr.effective_date, a1c_base.effective_date) >= 30 THEN 1 ELSE 0 END AS flag_a1c_30days,
    CASE WHEN u.is_12mo_member = 1 AND w_curr.effective_date >= DATE_SUB(v.report_date, INTERVAL 30 DAY) THEN 1 ELSE 0 END AS flag_recent_weight_after_12mo,

    -- COHORT ASSIGNMENT
    CASE
        -- 1. Obese GLP
        WHEN (COALESCE(gs.total_days_covered, 0) >= 90)  
             AND ((1 - (COALESCE(gs.total_days_covered, 0) / GREATEST(u.days_since_start, 1))) <= 0.10) 
             AND (bmi_base.bmi > 30)
             AND (DATEDIFF(w_curr.effective_date, w_base.effective_date) >= 30)
        THEN 'obese_glp_cohort'

        -- 2. Obese Lifestyle
        WHEN (COALESCE(gs.total_days_covered, 0) < 90 OR (1 - (COALESCE(gs.total_days_covered, 0) / GREATEST(u.days_since_start, 1))) > 0.25)
             AND (bmi_base.bmi > 30)
             AND (DATEDIFF(w_curr.effective_date, w_base.effective_date) >= 30)
        THEN 'obese_lifestyle_cohort'

        -- 3. HPTN
        WHEN (bp_base.systolic > 140 OR bp_base.diastolic > 90)
             AND (DATEDIFF(bp_curr.effective_date, bp_base.effective_date) >= 30)
        THEN 'hptn_cohort'

        -- 4. High A1C
        WHEN (a1c_base.a1c >= 5.7)
             AND (COALESCE(ac.total_a1c_records, 0) >= 2) 
        THEN 'high_a1c_cohort'

        -- 5. Not Obese
        WHEN (bmi_base.bmi < 30)
             AND (COALESCE(a1c_base.a1c, 0) < 6.5)
             AND (DATEDIFF(w_curr.effective_date, w_base.effective_date) >= 30)
        THEN 'not_obese_cohort'

        ELSE 'Unassigned / Non-Compliant'
    END AS assigned_cohort

FROM base_users u
CROSS JOIN vars v 
LEFT JOIN partner_employers pe ON u.user_id = pe.user_id

INNER JOIN billable_stats_6mo bs ON u.user_id = bs.user_id AND bs.billable_months_last_6mo >= 6
LEFT JOIN billable_stats_lifetime bl ON u.user_id = bl.user_id
LEFT JOIN glp_stats gs ON u.user_id = gs.user_id
LEFT JOIN weight_compliance wc ON u.user_id = wc.user_id
LEFT JOIN bp_compliance bpc ON u.user_id = bpc.user_id
LEFT JOIN a1c_counts ac ON u.user_id = ac.user_id
LEFT JOIN weight_ranks w_base ON u.user_id = w_base.user_id AND w_base.rn_asc = 1
LEFT JOIN weight_ranks w_curr ON u.user_id = w_curr.user_id AND w_curr.rn_desc = 1
LEFT JOIN bmi_ranks bmi_base ON u.user_id = bmi_base.user_id AND bmi_base.rn_asc = 1
LEFT JOIN bmi_ranks bmi_curr ON u.user_id = bmi_curr.user_id AND bmi_curr.rn_desc = 1
LEFT JOIN bp_ranks bp_base ON u.user_id = bp_base.user_id AND bp_base.rn_asc = 1
LEFT JOIN bp_ranks bp_curr ON u.user_id = bp_curr.user_id AND bp_curr.rn_desc = 1
LEFT JOIN a1c_ranks a1c_base ON u.user_id = a1c_base.user_id AND a1c_base.rn_asc = 1
LEFT JOIN a1c_ranks a1c_curr ON u.user_id = a1c_curr.user_id AND a1c_curr.rn_desc = 1
LEFT JOIN nps_latest nps ON u.user_id = nps.user_id AND nps.rn = 1
LEFT JOIN csat_stats csat ON u.user_id = csat.user_id

// Custom Sql 2 (Questionnaire responses)
WITH 
-- 1. DEFINE REPORT DATE
vars AS (
    SELECT CAST('2026-02-01' AS DATE) AS report_date
),

-- 2. ACTIVE MEMBERS SNAPSHOT
active_members AS (
    SELECT 
        u.id AS user_id,
        u.readable_id,
        pe.name AS employer_name
    FROM users u
    JOIN subscriptions s ON u.id = s.user_id
    LEFT JOIN partner_employers pe ON u.id = pe.user_id
    CROSS JOIN vars v
    WHERE s.status = 'active' 
      AND s.cancellation_date IS NULL
      AND s.start_date <= v.report_date
      AND EXISTS (
          SELECT 1 FROM user_program_memberships upm 
          WHERE upm.user_id = u.id 
          AND upm.program IN ('HEALTHY_WEIGHT_JOURNEY', 'weightloss')
      )
)
-- 3.  SURVEY RECORDS
SELECT
    am.readable_id,
    am.employer_name,
    qr.questionnaire_id,
    qr.questionnaire_id AS questionnaire_title, 
    qr.question_id,
    qr.answered_at,
    qr.answer_text AS answer_value
FROM questionnaire_records qr
JOIN active_members am ON qr.user_id = am.user_id
JOIN vars v ON 1=1
WHERE qr.question_id IN (
    'IO6j6rqY1RZe', 
    'LJ7hYbzFyc3o', 
    'NRGXIAUDGjRv', 
    'AaIbKyE6VRkL', 
    'UeNt46PgUv4A', 
    'g4rg786D9C2q'
)
AND qr.answered_at <= v.report_date
