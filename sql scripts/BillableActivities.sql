
DROP TEMPORARY TABLE IF EXISTS tmp_apple_users;
        CREATE TEMPORARY TABLE tmp_apple_users AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name in ('Amazon')
          AND s.status = 'ACTIVE'
          AND s.start_date <= CURRENT_DATE;

          CREATE INDEX idx_tmp_apple_users ON tmp_apple_users (user_id);

--------------------- by month count all care team interactions ---------------------

SELECT 
    COUNT(DISTINCT ba.user_id) as member_count,
    'Care_Team_Interaction' as activity_type,
    CASE
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 1 THEN 'January'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 2 THEN 'February'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 3 THEN 'March'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 4 THEN 'April'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 5 THEN 'May'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 6 THEN 'June' 
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 7 THEN 'July'
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 8 THEN 'August'
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 9 THEN 'September'
        ELSE 'Other'
    END as month_name,
    CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) as month_number
FROM billable_activities ba
JOIN tmp_apple_users au ON au.user_id = ba.user_id
WHERE ba.type IN ('TEXT_MESSAGE_CARE_ONLY','VIDEO_CALL_COMPLETED', 'VOICE_MESSAGE_CARE_ONLY')
  AND ba.activity_timestamp >= '2025-01-01' 
  AND ba.activity_timestamp <= '2025-09-30'
GROUP BY CAST(MONTH(ba.activity_timestamp) AS UNSIGNED)  -- Use full expression
ORDER BY CAST(MONTH(ba.activity_timestamp) AS UNSIGNED);

--------------- Record Vital Signs 

SELECT 
    COUNT(DISTINCT ba.user_id) as member_count,
    'Vital Signs' as activity_type,
    CASE
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 1 THEN 'January'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 2 THEN 'February'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 3 THEN 'March'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 4 THEN 'April'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 5 THEN 'May'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 6 THEN 'June' 
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 7 THEN 'July'
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 8 THEN 'August'
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 9 THEN 'September'
        ELSE 'Other'
    END as month_name,
    CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) as month_number
FROM billable_activities ba
JOIN tmp_apple_users au ON au.user_id = ba.user_id
WHERE ba.type IN ('RECORD_BLOOD_GLUCOSE', 'RECORD_BODY_WEIGHT','RECORD_BLOOD_PRESSURE','RECORD_STEPS')
  AND ba.activity_timestamp >= '2025-07-01' 
  AND ba.activity_timestamp <= '2025-09-30'
GROUP BY CAST(MONTH(ba.activity_timestamp) AS UNSIGNED)  -- Use full expression
ORDER BY CAST(MONTH(ba.activity_timestamp) AS UNSIGNED);



--------------------- by month count by activity type ---------------------

SELECT 
    COUNT(DISTINCT ba.user_id) as member_count,
    ba.type,
    CASE
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 1 THEN 'January'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 2 THEN 'February'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 3 THEN 'March'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 4 THEN 'April'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 5 THEN 'May'
        -- WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 6 THEN 'June' 
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 7 THEN 'July'
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 8 THEN 'August'
        WHEN CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) = 9 THEN 'September'
        ELSE 'Other'
    END as month_name,
    CAST(MONTH(ba.activity_timestamp) AS UNSIGNED) as month_number
FROM billable_activities ba
JOIN tmp_apple_users au ON au.user_id = ba.user_id
WHERE ba.type IN ('MEDICAL_QUESTIONNAIRE_ANSWERED')
  AND ba.activity_timestamp >= '2025-07-01' 
  AND ba.activity_timestamp <= '2025-09-30'
GROUP BY ba.`type`, MONTH(ba.activity_timestamp)  -- Use full expression
ORDER BY MONTH(ba.activity_timestamp);

WHERE ba.type IN ('QUESTIONNAIRE_ANSWERED','COMPLETED_LAB_TEST', 'CONSUMED_DIGITAL_CONTENT','MEDICATION_CHANGE','MEAL_PLAN_GENERATED','TEXT_MESSAGE_CARE_ONLY','VIDEO_CALL_COMPLETED', 'VOICE_MESSAGE_CARE_ONLY', 'RECORD_BLOOD_GLUCOSE', 'RECORD_BODY_WEIGHT','RECORD_BLOOD_PRESSURE','RECORD_STEPS')


--------------------- Quarterly ---------------------
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
JOIN tmp_apple_users au ON au.user_id = ba.user_id
WHERE ba.type IN ('COMPLETED_LAB_TEST','MEAL_PLAN_GENERATED','MEDICATION_CHANGE','CONSUMED_DIGITAL_CONTENT')
  AND ba.activity_timestamp >= '2025-01-01' 
  AND ba.activity_timestamp <= '2025-09-30'
GROUP BY ba.type, QUARTER(ba.activity_timestamp)
ORDER BY QUARTER(ba.activity_timestamp);


select * from billable_activities ba GROUP BY ba.type;