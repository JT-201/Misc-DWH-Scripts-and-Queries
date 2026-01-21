-------------   User here for 60 Days  --------------------
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_60;
    CREATE TEMPORARY TABLE tmp_amazon_users_60 AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name = 'Amazon'
        AND s.status = 'ACTIVE'
        AND s.start_date <= DATE_SUB('2025-09-30', INTERVAL 60 DAY);

CREATE INDEX idx_apple_users_user_id ON tmp_amazon_users_60(user_id);
-------------   User here for 120 Days  --------------------
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_120;
    CREATE TEMPORARY TABLE tmp_amazon_users_120 AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name = 'Amazon'
          AND s.status = 'ACTIVE'
          AND s.start_date <= DATE_SUB('2025-09-30', INTERVAL 120 DAY);
CREATE INDEX idx_apple_users_user_id ON tmp_amazon_users_120(user_id);

    -------------   User here for 180 Days  --------------------
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_180;
    CREATE TEMPORARY TABLE tmp_amazon_users_180 AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name = 'Amazon'
          AND s.status = 'ACTIVE'
         AND s.start_date <= DATE_SUB('2025-09-30', INTERVAL 180 DAY);

CREATE INDEX idx_apple_users_user_id ON tmp_amazon_users_180(user_id);


select * from tmp_amazon_users_60;
select * from tmp_amazon_users_120;
select * from tmp_amazon_users_180;


--------------- GLP1 Filter -----------------
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_glp1_users_60;
        CREATE TEMPORARY TABLE tmp_amazon_glp1_users_60 AS
        WITH glp1_prescriptions AS (
            SELECT 
                au.user_id,
                p.prescribed_at,
                p.days_of_supply,
                p.total_refills,
                (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
                DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
            FROM tmp_amazon_users_60 au
            JOIN prescriptions p ON au.user_id = p.patient_user_id
            JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
            JOIN medications m ON m.id = ndcs.medication_id
            WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
            AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-10-01', INTERVAL 30 DAY)  -- Ensure prescription end date is not in the future
        ),
        user_prescription_coverage AS (
            SELECT 
                user_id,
                MIN(prescribed_at) as first_prescription_date,
                MAX(prescription_end_date) as last_prescription_end_date,
                SUM(total_prescription_days) as total_covered_days,
                DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days,
                -- Calculate gap percentage !!! if user changes prescriptions this can cause prescription overlap !!!!!!
                CASE 
                    WHEN DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) > 0 
                    THEN ((DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) - SUM(total_prescription_days)) * 100.0 / 
                          DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)))
                    ELSE 0 
                END as gap_percentage
            FROM glp1_prescriptions
            GROUP BY user_id
        )
        SELECT 
            user_id,
            first_prescription_date as prescribed_at,
            last_prescription_end_date as prescription_end_date,
            total_covered_days,
            total_period_days,
            gap_percentage
        FROM user_prescription_coverage;
        

    CREATE INDEX idx_amazon_glp1_user_id ON tmp_amazon_glp1_users_60(user_id);

------------------ GLP1 120 Days -------------------

DROP TEMPORARY TABLE IF EXISTS tmp_amazon_glp1_users_120;
        CREATE TEMPORARY TABLE tmp_amazon_glp1_users_120 AS
        WITH glp1_prescriptions AS (
            SELECT 
                au.user_id,
                p.prescribed_at,
                p.days_of_supply,
                p.total_refills,
                (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
                DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
            FROM tmp_amazon_users_120 au
            JOIN prescriptions p ON au.user_id = p.patient_user_id
            JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
            JOIN medications m ON m.id = ndcs.medication_id
            WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
            AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-10-01', INTERVAL 30 DAY)  -- Ensure prescription end date is not in the future
        ),
        user_prescription_coverage AS (
            SELECT 
                user_id,
                MIN(prescribed_at) as first_prescription_date,
                MAX(prescription_end_date) as last_prescription_end_date,
                SUM(total_prescription_days) as total_covered_days,
                DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days,
                -- Calculate gap percentage !!! if user changes prescriptions this can cause prescription overlap !!!!!!
                CASE 
                    WHEN DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) > 0 
                    THEN ((DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) - SUM(total_prescription_days)) * 100.0 / 
                          DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)))
                    ELSE 0 
                END as gap_percentage
            FROM glp1_prescriptions
            GROUP BY user_id
        )
        SELECT 
            user_id,
            first_prescription_date as prescribed_at,
            last_prescription_end_date as prescription_end_date,
            total_covered_days,
            total_period_days,
            gap_percentage
        FROM user_prescription_coverage;
        

    CREATE INDEX idx_amazon_glp1_user_id ON tmp_amazon_glp1_users_120(user_id);


------------------ GLP1 180 Days -------------------
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_glp1_users_180;
        CREATE TEMPORARY TABLE tmp_amazon_glp1_users_180 AS
        WITH glp1_prescriptions AS (
            SELECT 
                au.user_id,
                p.prescribed_at,
                p.days_of_supply,
                p.total_refills,
                (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
                DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
            FROM tmp_amazon_users_180 au
            JOIN prescriptions p ON au.user_id = p.patient_user_id
            JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
            JOIN medications m ON m.id = ndcs.medication_id
            WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
            AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-10-01', INTERVAL 30 DAY)  -- Ensure prescription end date is not in the future
        ),
        user_prescription_coverage AS (
            SELECT 
                user_id,
                MIN(prescribed_at) as first_prescription_date,
                MAX(prescription_end_date) as last_prescription_end_date,
                SUM(total_prescription_days) as total_covered_days,
                DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days,
                -- Calculate gap percentage !!! if user changes prescriptions this can cause prescription overlap !!!!!!
                CASE 
                    WHEN DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) > 0 
                    THEN ((DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) - SUM(total_prescription_days)) * 100.0 / 
                          DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)))
                    ELSE 0 
                END as gap_percentage
            FROM glp1_prescriptions
            GROUP BY user_id
        )
        SELECT 
            user_id,
            first_prescription_date as prescribed_at,
            last_prescription_end_date as prescription_end_date,
            total_covered_days,
            total_period_days,
            gap_percentage
        FROM user_prescription_coverage;

    CREATE INDEX idx_amazon_glp1_user_id ON tmp_amazon_glp1_users_180(user_id);





------------- Demographics -------------

SELECT
    COUNT(CASE WHEN u.age >= 18 AND u.age <= 19 THEN u.id END) AS active_users_18_19,
    COUNT(CASE WHEN u.age >= 20 AND u.age <= 29 THEN u.id END) AS active_users_20_29,
    COUNT(CASE WHEN u.age >= 30 AND u.age <= 39 THEN u.id END) AS active_users_30_39,
    COUNT(CASE WHEN u.age >= 40 AND u.age <= 49 THEN u.id END) AS active_users_40_49,
    COUNT(CASE WHEN u.age >= 50 AND u.age <= 59 THEN u.id END) AS active_users_50_59,
    COUNT(CASE WHEN u.age >= 60 AND u.age <= 69 THEN u.id END) AS active_users_60_69,
    COUNT(CASE WHEN u.age >= 70 THEN u.id END) AS active_users_70_plus,
    COUNT(CASE WHEN u.sex = 'MALE' THEN u.id END) AS active_users_male,
    COUNT(CASE WHEN u.sex = 'FEMALE' THEN u.id END) AS active_users_female,
    COUNT(CASE WHEN u.ethnicity = 'WHITE' THEN u.id END) AS active_users_white,
    COUNT(CASE WHEN u.ethnicity = 'HISPANIC_LATINO' THEN u.id END) AS active_users_hispanic_latino,
    COUNT(CASE WHEN u.ethnicity = 'BLACK_OR_AFRICAN_AMERICAN' THEN u.id END) AS active_users_black_african_american,
    COUNT(CASE WHEN u.ethnicity = 'ASIAN' THEN u.id END) AS active_users_asian,
    COUNT(CASE WHEN u.ethnicity = 'AMERICAN_NATIVE_OR_ALASKAN' THEN u.id END) AS active_users_american_native_alaskan,
    COUNT(CASE WHEN u.ethnicity = 'OTHER' OR (
        u.ethnicity IS NOT NULL AND
        u.ethnicity NOT IN (
            'WHITE',
            'HISPANIC_LATINO',
            'BLACK_OR_AFRICAN_AMERICAN',
            'ASIAN',
            'AMERICAN_NATIVE_OR_ALASKAN'
        )
    ) THEN u.id END) AS active_users_other,
    COUNT(CASE WHEN u.ethnicity IS NULL THEN u.id END) AS active_users_unknown
FROM users u
JOIN tmp_amazon_users_180 ru ON u.id = ru.user_id;


-------------  Weight Loss  --------------

select question_id, questionnaire_title , question_title, answer_text, answer_value from questionnaire_records
where answer_type='number'
and question_title like '%weight%'
GROUP BY question_id;

SELECT 
    question_id,
    questionnaire_title,
    question_title,
    COUNT(DISTINCT user_id) AS users_answered
FROM questionnaire_records
WHERE question_id IN ('CIh3gKh8vZ6e','L5ft6jIJz26B')
  AND user_id IN (SELECT user_id FROM tmp_amazon_users_180)
  AND (answer_text IS NOT NULL OR answer_value IS NOT NULL)
GROUP BY question_id, questionnaire_title, question_title;

SELECT  user_id, answer_value, answered_at
FROM (
    SELECT 
        qr.*,
        ROW_NUMBER() OVER (PARTITION BY qr.user_id ORDER BY qr.answered_at DESC) AS rn
    FROM questionnaire_records qr
    JOIN tmp_amazon_users_180 au ON au.user_id = qr.user_id
    WHERE qr.question_id IN ('L5ft6jIJz26B')
) latest
WHERE latest.rn = 1;



---------- Base no interval ----------------
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_base;
    CREATE TEMPORARY TABLE tmp_amazon_users_base AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name = 'Amazon'
        AND s.start_date <= '2025-09-30';

CREATE INDEX idx_amazon_users_base_user_id ON tmp_amazon_users_base(user_id);

-------------   User here for 60 Days  --------------------
-- Count of newly prescribed users (Wegovy or Zepbound) in last 90 days, first prescription only,
-- and no prior Wegovy or Zepbound prescriptions before 2025-07-01
SELECT 
    COUNT(DISTINCT p.patient_user_id) AS newly_prescribed_users
FROM tmp_amazon_users_base au
JOIN prescriptions p ON au.user_id = p.patient_user_id
JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
JOIN medications m ON m.id = ndcs.medication_id
WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
  AND p.prescribed_at BETWEEN DATE_SUB('2025-09-30', INTERVAL 90 DAY) AND '2025-09-30'
  AND IFNULL(p.refill_count, 0) = 0
  AND NOT EXISTS (
      SELECT 1
      FROM prescriptions p2
      JOIN medication_ndcs ndcs2 ON p2.prescribed_ndc = ndcs2.ndc
      JOIN medications m2 ON m2.id = ndcs2.medication_id
      WHERE p2.patient_user_id = p.patient_user_id
        AND (m2.name LIKE '%Wegovy%' OR m2.name LIKE '%Zepbound%')
        AND p2.prescribed_at < '2025-07-01'
  );



select * from medical_conditions
where icd10 is NOT NULL
and name like '%Chronic kidney%'
group by icd10;


select count(distinct au.user_id)
from tmp_amazon_users_180 au
join medical_conditions mc on mc.user_id = au.user_id
where mc.icd10 in ('R03.0',
'O13.9',
'O16',
'G93.2',
'H40.05',
'I1A',
'I27.20')
and mc.icd10 = 'E28.2';


select count(distinct au.user_id)
from tmp_amazon_users_base au
join medical_conditions mc on mc.user_id = au.user_id
where mc.icd10 in ('R73.03','I10','R03.0','O13.9','O16','G93.2','H40.05','I1A','I27.20','E78.5','E78.00','E78.01','E78.0','G47.33','G47.3','G47.30','K76.0','I50.0','I50.2','I50.22','I50.20','I50.32','I25.1','I25.10',
'I25.84','I25.11','I25.110','Z95.1','Z95.5','I21.09',
'I25.81','I73.9','G90.0','I73','D36.10','G90.09','I63.9','Z82.3','N18','N18.5','N18.9');

-- Create temp table of users with exactly one condition from your list
DROP TEMPORARY TABLE IF EXISTS tmp_single_condition_users;
CREATE TEMPORARY TABLE tmp_single_condition_users AS
SELECT DISTINCT au.user_id, mc.icd10
FROM tmp_amazon_users_base au
JOIN medical_conditions mc ON mc.user_id = au.user_id
WHERE mc.icd10 IN ('R73.03','I10','R03.0','O13.9','O16','G93.2','H40.05','I1A','I27.20','E78.5','E78.00','E78.01','E78.0','G47.33','G47.3','G47.30','K76.0','I50.0','I50.2','I50.22','I50.20','I50.32','I25.1','I25.10','I25.84','I25.11','I73.9','G90.0','I73','D36.10','G90.09','I63.9')
GROUP BY au.user_id
HAVING COUNT(DISTINCT mc.icd10) = 2;

CREATE INDEX idx_single_condition_user_id ON tmp_single_condition_users(user_id);

-- Now your query becomes much simpler:
SELECT 
    COUNT(DISTINCT qr.user_id) as user_count
FROM tmp_single_condition_users scu
JOIN questionnaire_records qr ON qr.user_id = scu.user_id
JOIN bmi_values bv ON bv.user_id = scu.user_id
WHERE qr.question_title LIKE 'Are you currently living with any of these conditions%'
  AND qr.answer_text LIKE '%PCOS%'
  AND bv.value >= 35;



DROP TEMPORARY TABLE IF EXISTS tmp_amazon_glp1_users_60;
        CREATE TEMPORARY TABLE tmp_amazon_glp1_users_60 AS
        WITH glp1_prescriptions AS (
            SELECT 
                au.user_id,
                p.prescribed_at,
                p.days_of_supply,
                p.total_refills,
                (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
                DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
            FROM tmp_amazon_users_60 au
            JOIN prescriptions p ON au.user_id = p.patient_user_id
            JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
            JOIN medications m ON m.id = ndcs.medication_id
            WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
            AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('2025-10-01', INTERVAL 30 DAY)  -- Ensure prescription end date is not in the future
        ),
        user_prescription_coverage AS (
            SELECT 
                user_id,
                MIN(prescribed_at) as first_prescription_date,
                MAX(prescription_end_date) as last_prescription_end_date,
                SUM(total_prescription_days) as total_covered_days,
                DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days,
                -- Calculate gap percentage !!! if user changes prescriptions this can cause prescription overlap !!!!!!
                CASE 
                    WHEN DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) > 0 
                    THEN ((DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) - SUM(total_prescription_days)) * 100.0 / 
                          DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)))
                    ELSE 0 
                END as gap_percentage
            FROM glp1_prescriptions
            GROUP BY user_id
        )
        SELECT 
            user_id,
            first_prescription_date as prescribed_at,
            last_prescription_end_date as prescription_end_date,
            total_covered_days,
            total_period_days,
            gap_percentage
        FROM user_prescription_coverage;
        

    CREATE INDEX idx_amazon_glp1_user_id ON tmp_amazon_glp1_users_60(user_id);






-- Create Amazon users base table
DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_base;
CREATE TEMPORARY TABLE tmp_amazon_users_base AS
    SELECT DISTINCT s.user_id, s.start_date
    FROM subscriptions s
    JOIN partner_employers bus ON bus.user_id = s.user_id
    WHERE bus.name = 'Amazon'
    AND s.start_date <= '2025-09-30';

CREATE INDEX idx_amazon_users_base_user_id ON tmp_amazon_users_base(user_id);

-- Create temp table with baseline BMI for each user from Amazon base users
DROP TEMPORARY TABLE IF EXISTS tmp_baseline_bmi;
CREATE TEMPORARY TABLE tmp_baseline_bmi AS
SELECT 
    bv.user_id,
    bv.value,
    bv.effective_date
FROM bmi_values bv
JOIN tmp_amazon_users_base au ON bv.user_id = au.user_id
WHERE bv.effective_date = (
    SELECT MIN(bv2.effective_date)
    FROM bmi_values bv2
    WHERE bv2.user_id = bv.user_id
);

CREATE INDEX idx_baseline_bmi_user_id ON tmp_baseline_bmi(user_id);

-- Create temp table of users with exactly X condition(s) from your list
-- MODIFY THESE VALUES AS NEEDED:
-- Change HAVING COUNT = 1 (for exactly 1), = 2 (for exactly 2), or remove HAVING (for any number)
-- Change bv.value >= 35 to >= 40 for different BMI threshold
DROP TEMPORARY TABLE IF EXISTS tmp_single_condition_users;
CREATE TEMPORARY TABLE tmp_single_condition_users AS
SELECT DISTINCT au.user_id, mc.icd10
FROM tmp_amazon_users_base au
JOIN medical_conditions mc ON mc.user_id = au.user_id
WHERE mc.icd10 IN ('R73.03',
'I10',
'E78.5',
'G47.33',
'K76.0',
'I50.0',
'I25.1',
'I73.9',
'I63.9')
GROUP BY au.user_id
HAVING COUNT(DISTINCT mc.icd10) = 2;  -- ← CHANGE THIS: = 1 (exactly 1), = 2 (exactly 2), or remove line (any number)

CREATE INDEX idx_single_condition_user_id ON tmp_single_condition_users(user_id);

-- Final query using both temp tables
-- MODIFY BMI THRESHOLD HERE:
with user_eligibility as (
    select al.user_id, alf.value as eligibility_status
    from audit_logs al
    join audit_log_payload_fields alf on al.id = alf.audit_log_id and alf.`key` = 'eligibility'
    where al.event_name = 'program.generic.medical_eligibility_determined'
)
SELECT 
    COUNT(DISTINCT qr.user_id) as user_count,
    ue.eligibility_status
FROM tmp_single_condition_users scu
JOIN questionnaire_records qr ON qr.user_id = scu.user_id
JOIN tmp_baseline_bmi bv ON bv.user_id = scu.user_id
LEFT JOIN user_eligibility ue ON ue.user_id = scu.user_id
WHERE qr.question_title LIKE 'Are you currently living with any of these conditions%'
  AND qr.answer_text LIKE '%PCOS%'
  AND bv.value >= 40
GROUP BY ue.eligibility_status;


----------=-=----- med questionnaire answered ------------

SELECT 
    COUNT(DISTINCT ba.user_id) as member_count,
    ba.type,
    CASE
        WHEN QUARTER(ba.activity_timestamp) = 1 THEN 'Q1 (Jan-Mar)'
        WHEN QUARTER(ba.activity_timestamp) = 2 THEN 'Q2 (Apr-Jun)'
        WHEN QUARTER(ba.activity_timestamp) = 3 THEN 'Q3 (Jul-Sep)'
        WHEN QUARTER(ba.activity_timestamp) = 4 THEN 'Q4 (Oct-Dec)'
        ELSE 'Other'
    END as quarter_name,
    QUARTER(ba.activity_timestamp) as quarter_number
FROM billable_activities ba
JOIN tmp_amazon_users_base au ON au.user_id = ba.user_id
WHERE ba.type IN ('MEDICAL_QUESTIONNAIRE_ANSWERED')
  AND ba.activity_timestamp >= '2025-01-01' 
  AND ba.activity_timestamp <= '2025-09-30'
GROUP BY QUARTER(ba.activity_timestamp)
ORDER BY QUARTER(ba.activity_timestamp);


----------------- prediabetes ----------------

DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_base;
CREATE TEMPORARY TABLE tmp_amazon_users_base AS
    SELECT DISTINCT s.user_id, s.start_date
    FROM subscriptions s
    JOIN partner_employers bus ON bus.user_id = s.user_id
    WHERE bus.name = 'Amazon'
    AND s.status = 'ACTIVE'
    AND s.start_date >= '2025-07-01'
    AND s.start_date <= '2025-09-30';

CREATE INDEX idx_amazon_users_base_user_id ON tmp_amazon_users_base(user_id);

WITH latest_a1c_per_user AS (
    SELECT
        a1.user_id,
        au.start_date,
        FIRST_VALUE(a1.value) OVER (
            PARTITION BY a1.user_id 
            ORDER BY a1.effective_date DESC
        ) as latest_a1c_value
    FROM a1c_values a1
    JOIN tmp_amazon_users_base au ON a1.user_id = au.user_id
    WHERE a1.effective_date >= au.start_date
)
SELECT 
    COUNT(DISTINCT qr.user_id) as user_count,
    qr.answer_text
FROM questionnaire_records qr
JOIN latest_a1c_per_user la ON qr.user_id = la.user_id
WHERE qr.question_title like 'Are you currently living with any of these conditions%'
  AND qr.answer_text NOT LIKE '%Prediabetes%' AND qr.answer_text NOT LIKE '%diabetes%'
  AND la.latest_a1c_value >= 5.7 
  AND la.latest_a1c_value <= 6.4
GROUP BY qr.answer_text;


with relevant_users as (
    select distinct s.user_id
    from subscriptions s
    JOIN partner_employers bus ON bus.user_id = s.user_id
    WHERE bus.name = 'Amazon'
    and status = 'ACTIVE'
    )
SELECT 
    ap.appointment_type,
    COUNT(DISTINCT ap.appointment_id) AS total_appointments,    
    ROUND(AVG(available_after_minutes_excl_weekend / 60 / 24), 2) AS avg_first_available_after_days_excl_weekend,
    ROUND(AVG(available_after_minutes / 60 / 24), 2) AS avg_first_available_after_days,
    ROUND(
        (
            SUM(
                CASE
                    WHEN DAYOFWEEK(ap.available_at) IN (1,7) OR DAYOFWEEK(a.start) IN (1,7)
                        THEN 0
                    ELSE TIMESTAMPDIFF(MINUTE, ap.available_at, a.start)
                END
            )
        ) / (60 * 24 * COUNT(*)), 2
    ) AS avg_difference_between_scheduling_and_start_days_excl_weekend
FROM appointments_availabilities ap
join relevant_users p on ap.user_id = p.user_id
join appointments a on ap.appointment_id = a.id
join appointments_participants ap2 on a.id = ap2.appointment_id
join providers pr on ap2.user_id = pr.user_id
WHERE ap.created_at >= '2025-07-01' AND ap.created_at <= '2025-09-30'
GROUP BY ap.appointment_type
ORDER BY ap.appointment_type, ROUND(AVG(available_after_minutes_excl_weekend / 60), 2) DESC;



------------ These Queries need to be ran in the Backend DB --------------- 
------ USERS With and Without PCP ------- 
select count(DISTINCT s.user_id) from subscription_subscriptions s
  join preferences_preferences pp ON pp.user_id = s.user_id
  where BIN_TO_UUID(s.plan_id) = 'a0a861eb-9bbe-4faf-88c7-2cb5b4ae2432'
  and status='active' 
  AND s.start_date <= '2026-01-01'
  AND s.start_date >= '2025-10-01'
  and pp.key = 'physicians.primary.first-name';

  SELECT COUNT(DISTINCT s.user_id)
FROM subscription_subscriptions s
WHERE BIN_TO_UUID(s.plan_id) = 'a0a861eb-9bbe-4faf-88c7-2cb5b4ae2432'
  AND s.status = 'active'
  AND s.start_date <= '2026-01-01'
  AND s.start_date >= '2025-10-01'
  AND NOT EXISTS (
      SELECT 1
      FROM preferences_preferences pp
      WHERE pp.user_id = s.user_id
        AND pp.key = 'physicians.primary.first-name'
  );

------------------ Physician sent a care summary ------------- 


SELECT count(distinct s.user_id) FROM lifeline_user_items lli
JOIN subscription_subscriptions s ON s.user_id = lli.user_id
WHERE s.status = 'ACTIVE'
    AND BIN_TO_UUID(s.plan_id) = 'a0a861eb-9bbe-4faf-88c7-2cb5b4ae2432'
  AND s.start_date <= '2026-01-01'
  AND s.start_date >= '2025-10-01'
  AND lli.item_type = 'communication.sent'
ORDER BY lli.timestamp DESC;