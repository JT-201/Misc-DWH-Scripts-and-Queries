INSERT INTO bmi_values_cleaned
(id, user_id, value, effective, effective_date, effective_time, 
 intake, intake_date, intake_time, source)
SELECT 
    UUID_TO_BIN(UUID()) as id,
    bwv.user_id,
    (bwv.value / POWER(bhv.height_value / 100, 2)) as value,  -- BMI calculation
    bwv.effective,
    DATE(bwv.effective) as effective_date,
    TIME(bwv.effective) as effective_time,
    bwv.intake,
    DATE(bwv.intake) as intake_date,
    TIME(bwv.intake) as intake_time,
    bwv.source
FROM body_weight_values_cleaned bwv
INNER JOIN (
    SELECT 
        user_id,
        value as height_value,
        effective as height_effective
    FROM body_height_values
    WHERE value != 0 AND value IS NOT NULL
) bhv ON bwv.user_id = bhv.user_id
WHERE bwv.value IS NOT NULL 
AND bwv.effective IS NOT NULL
AND ABS(DATEDIFF(bhv.height_effective, bwv.effective)) = (
    SELECT MIN(ABS(DATEDIFF(bhv2.effective, bwv.effective)))
    FROM body_height_values bhv2
    WHERE bhv2.user_id = bwv.user_id 
    AND bhv2.value != 0
);