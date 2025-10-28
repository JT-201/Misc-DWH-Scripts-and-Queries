



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
HAVING COUNT(DISTINCT mc.icd10) = 1;  -- ← CHANGE THIS: = 1 (exactly 1), = 2 (exactly 2), or remove line (any number)

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