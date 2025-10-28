------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_base;

        CREATE TEMPORARY TABLE tmp_amazon_users_base AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers pe ON pe.user_id = s.user_id
        WHERE pe.name = 'Amazon'
          AND s.status = 'ACTIVE'
          AND s.start_date <= '2025-09-30';

   CREATE INDEX idx_tmp_amazon_users_base_user_id ON tmp_amazon_users_base(user_id);



-- Metformin users temp table
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_metformin_users;
CREATE TEMPORARY TABLE tmp_amazon_metformin_users AS
WITH metformin_prescriptions AS (
    SELECT 
        au.user_id,
        p.prescribed_at,
        p.days_of_supply,
        p.total_refills,
        (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
        DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
    FROM tmp_amazon_users_base au
    JOIN prescriptions p ON au.user_id = p.patient_user_id
    JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
    JOIN medications m ON m.id = ndcs.medication_id
    WHERE m.name LIKE '%metformin%'
    AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-09-30', INTERVAL 30 DAY)
),
user_prescription_coverage AS (
    SELECT 
        user_id,
        MIN(prescribed_at) as first_prescription_date,
        MAX(prescription_end_date) as last_prescription_end_date,
        SUM(total_prescription_days) as total_covered_days,
        DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days
    FROM metformin_prescriptions
    GROUP BY user_id
)
SELECT 
    user_id,
    first_prescription_date as prescribed_at,
    last_prescription_end_date as prescription_end_date,
    total_covered_days,
    total_period_days
FROM user_prescription_coverage;

CREATE INDEX idx_amazon_metformin_user_id ON tmp_amazon_metformin_users(user_id);




-- Statin users temp table
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_statins_users;
CREATE TEMPORARY TABLE tmp_amazon_statins_users AS
WITH statin_prescriptions AS (
    SELECT 
        au.user_id,
        p.prescribed_at,
        p.days_of_supply,
        p.total_refills,
        (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
        DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
    FROM tmp_amazon_users_base au
    JOIN prescriptions p ON au.user_id = p.patient_user_id
    JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
    JOIN medications m ON m.id = ndcs.medication_id
    WHERE m.name LIKE '%statin%'
    AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-09-30', INTERVAL 30 DAY)
),
user_prescription_coverage AS (
    SELECT 
        user_id,
        MIN(prescribed_at) as first_prescription_date,
        MAX(prescription_end_date) as last_prescription_end_date,
        SUM(total_prescription_days) as total_covered_days,
        DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days
    FROM statin_prescriptions
    GROUP BY user_id
)
SELECT 
    user_id,
    first_prescription_date as prescribed_at,
    last_prescription_end_date as prescription_end_date,
    total_covered_days,
    total_period_days
FROM user_prescription_coverage;

CREATE INDEX idx_amazon_statins_user_id ON tmp_amazon_statins_users(user_id);


-- Insulin users temp table
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_insulin_users;
CREATE TEMPORARY TABLE tmp_amazon_insulin_users AS
WITH insulin_prescriptions AS (
    SELECT 
        au.user_id,
        p.prescribed_at,
        p.days_of_supply,
        p.total_refills,
        (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
        DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
    FROM tmp_amazon_users_base au
    JOIN prescriptions p ON au.user_id = p.patient_user_id
    JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
    JOIN medications m ON m.id = ndcs.medication_id
    WHERE m.name LIKE '%insulin%'
    AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-09-30', INTERVAL 30 DAY)
),
user_prescription_coverage AS (
    SELECT 
        user_id,
        MIN(prescribed_at) as first_prescription_date,
        MAX(prescription_end_date) as last_prescription_end_date,
        SUM(total_prescription_days) as total_covered_days,
        DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days
    FROM insulin_prescriptions
    GROUP BY user_id
)
SELECT 
    user_id,
    first_prescription_date as prescribed_at,
    last_prescription_end_date as prescription_end_date,
    total_covered_days,
    total_period_days
FROM user_prescription_coverage;

CREATE INDEX idx_amazon_insulin_user_id ON tmp_amazon_insulin_users(user_id);

-- Therapy type users temp table
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_therapy_users;
CREATE TEMPORARY TABLE tmp_amazon_therapy_users AS
WITH therapy_prescriptions AS (
    SELECT 
        au.user_id,
        p.prescribed_at,
        p.days_of_supply,
        p.total_refills,
        (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
        DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
    FROM tmp_amazon_users_base au
    JOIN prescriptions p ON au.user_id = p.patient_user_id
    JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
    JOIN medications m ON m.id = ndcs.medication_id
    WHERE m.therapy_type IN ('DM', 'HTN', 'HLD', 'TD')
    AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-09-30', INTERVAL 30 DAY)
),
user_prescription_coverage AS (
    SELECT 
        user_id,
        MIN(prescribed_at) as first_prescription_date,
        MAX(prescription_end_date) as last_prescription_end_date,
        SUM(total_prescription_days) as total_covered_days,
        DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days
    FROM therapy_prescriptions
    GROUP BY user_id
)
SELECT 
    user_id,
    first_prescription_date as prescribed_at,
    last_prescription_end_date as prescription_end_date,
    total_covered_days,
    total_period_days
FROM user_prescription_coverage;

CREATE INDEX idx_amazon_therapy_user_id ON tmp_amazon_therapy_users(user_id);

DROP TEMPORARY TABLE IF EXISTS tmp_amazon_glp1_users_base;

        CREATE TEMPORARY TABLE tmp_amazon_glp1_users_base AS
        WITH glp1_prescriptions AS (
            SELECT 
                au.user_id,
                p.prescribed_at,
                p.days_of_supply,
                p.total_refills,
                (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
                DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
            FROM tmp_amazon_users_base au
            JOIN prescriptions p ON au.user_id = p.patient_user_id
            JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
            JOIN medications m ON m.id = ndcs.medication_id
            WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
            AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-09-30', INTERVAL 30 DAY)
        ),
        user_prescription_coverage AS (
            SELECT 
                user_id,
                MIN(prescribed_at) as first_prescription_date,
                MAX(prescription_end_date) as last_prescription_end_date,
                SUM(total_prescription_days) as total_covered_days,
                DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days
            FROM glp1_prescriptions
            GROUP BY user_id
        )
        SELECT 
            user_id,
            first_prescription_date as prescribed_at,
            last_prescription_end_date as prescription_end_date,
            total_covered_days,
            total_period_days
        FROM user_prescription_coverage;

    CREATE INDEX idx_tmp_amazon_glp1_users_base_user_id ON tmp_amazon_glp1_users_base(user_id);

-- Get distinct user counts from each medication table
SELECT 
    'metformin' as medication_type,
    COUNT(DISTINCT user_id) as user_count
FROM tmp_amazon_metformin_users

UNION ALL

SELECT 
    'statins' as medication_type,
    COUNT(DISTINCT user_id) as user_count
FROM tmp_amazon_statins_users

UNION ALL

SELECT 
    'insulin' as medication_type,
    COUNT(DISTINCT user_id) as user_count
FROM tmp_amazon_insulin_users

UNION ALL

SELECT 
    'therapy_types' as medication_type,
    COUNT(DISTINCT user_id) as user_count
FROM tmp_amazon_therapy_users

UNION ALL

SELECT 
    'glp1' as medication_type,
    COUNT(DISTINCT user_id) as user_count
FROM tmp_amazon_glp1_users_base

UNION ALL

SELECT 
    'base_users' as medication_type,
    COUNT(DISTINCT user_id) as user_count
FROM tmp_amazon_users_base

ORDER BY medication_type;