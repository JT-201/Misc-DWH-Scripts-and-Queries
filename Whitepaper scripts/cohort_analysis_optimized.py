import mysql.connector
import pandas as pd
import csv
import time
from typing import Dict, Any
from config import get_db_config  # Import the function instead

def connect_to_db():
    """Create database connection"""
    return mysql.connector.connect(**get_db_config())

def execute_with_timing(cursor, query: str, description: str = "Query"):
    """Execute a query with timing logging"""
    start_time = time.time()
    cursor.execute(query)
    end_time = time.time()
    duration = end_time - start_time
    print(f"  ⏱️  {description}: {duration:.2f}s")
    return duration

def execute_temp_table_creation(cursor):
    """Execute all temporary table creation queries with timing"""
    
    temp_table_queries = [
        # Apple Users
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_apple_users", "Drop Apple users table"),
            ("""CREATE TEMPORARY TABLE tmp_apple_users AS
            SELECT DISTINCT 
                bus.user_id,
                bus.partner
            FROM billable_user_statuses bus
            WHERE bus.partner IN ('Apple') AND bus.subscription_status = 'ACTIVE'""", "Create Apple users table"),
            ("CREATE INDEX idx_apple_users_user_id ON tmp_apple_users(user_id)", "Index Apple users table")
        ],
        
        # Amazon Users
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users", "Drop Amazon users table"),
            ("""CREATE TEMPORARY TABLE tmp_amazon_users AS
            SELECT DISTINCT 
                bus.user_id,
                bus.partner
            FROM billable_user_statuses bus
            WHERE bus.partner IN ('Amazon') AND bus.subscription_status = 'ACTIVE'""", "Create Amazon users table"),
            ("CREATE INDEX idx_amazon_users_user_id ON tmp_amazon_users(user_id)", "Index Amazon users table")
        ],
        
        # Apple + Amazon Users (base table - created early)
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_apple_and_amazon_users", "Drop Apple+Amazon union table"),
            ("""CREATE TEMPORARY TABLE tmp_apple_and_amazon_users AS
            SELECT user_id FROM tmp_apple_users
            UNION
            SELECT user_id FROM tmp_amazon_users""", "Create Apple+Amazon union table"),
            ("CREATE INDEX idx_apple_amazon_user_id ON tmp_apple_and_amazon_users(user_id)", "Index Apple+Amazon union table")
        ],
        
        # 6 Months Retention Users - ONLY for Apple+Amazon users
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_6_months_retention_users", "Drop 6-months retention table"),
            ("""CREATE TEMPORARY TABLE tmp_6_months_retention_users AS
            WITH user_subscription_days AS (
                SELECT aaau.user_id, SUM(CASE WHEN bus.subscription_status = 'ACTIVE' THEN 1 ELSE 0 END) as days_with_active_subscription
                FROM tmp_apple_and_amazon_users aaau
                JOIN billable_user_statuses bus ON aaau.user_id = bus.user_id
                WHERE bus.partner = 'Universal'
                GROUP BY aaau.user_id
            ),
            six_months_retention_users AS (
                SELECT us.user_id, us.days_with_active_subscription as days_with_active_subscriptions
                FROM user_subscription_days us
                WHERE us.days_with_active_subscription >= 180
                AND EXISTS (SELECT 1 FROM billable_activities ba WHERE ba.user_id = us.user_id)
            ),
            user_activity_summary AS (
                SELECT 
                    smru.user_id,
                    smru.days_with_active_subscriptions,
                    COUNT(DISTINCT DATE_FORMAT(ba.activity_timestamp, '%Y-%m')) as months_with_activity,
                    MIN(DATE(ba.activity_timestamp)) as first_activity_date,
                    MAX(DATE(ba.activity_timestamp)) as last_activity_date,
                    DATEDIFF(MAX(DATE(ba.activity_timestamp)), MIN(DATE(ba.activity_timestamp))) as activity_span_days
                FROM six_months_retention_users smru
                JOIN billable_activities ba ON smru.user_id = ba.user_id
                WHERE ba.activity_timestamp IS NOT NULL
                GROUP BY smru.user_id, smru.days_with_active_subscriptions
            ),
            user_monthly_activity_check AS (
                SELECT 
                    user_id,
                    days_with_active_subscriptions,
                    months_with_activity,
                    first_activity_date,
                    last_activity_date
                FROM user_activity_summary
                WHERE activity_span_days > 150  -- At least ~5 months span
                AND months_with_activity >= 6    -- Must have activity in at least 6 different months
            )
            SELECT 
                user_id,
                days_with_active_subscriptions,
                months_with_activity,
                first_activity_date,
                last_activity_date
            FROM user_monthly_activity_check""", "Create 6-months retention table with monthly activity requirement"),
            ("CREATE INDEX idx_6_months_retention_user_id ON tmp_6_months_retention_users(user_id)", "Index 6-months retention table")
        ],
        
        # GLP1 Users - Continuous Medication Only
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_weightloss_glp1_users", "Drop GLP1 users table"),
            ("""CREATE TEMPORARY TABLE tmp_weightloss_glp1_users AS
            WITH glp1_prescriptions AS (
                SELECT 
                    aaau.user_id,
                    p.prescribed_at,  -- ADD THIS LINE - was missing!
                    p.days_of_supply,
                    p.total_refills,
                    (p.days_of_supply + p.days_of_supply * COALESCE(p.total_refills, 0)) AS total_prescription_days,
                    DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * COALESCE(p.total_refills, 0)) DAY) as prescription_end_date
                FROM tmp_apple_and_amazon_users aaau
                JOIN prescriptions p ON aaau.user_id = p.patient_user_id
                JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
                JOIN medications m ON m.id = ndcs.medication_id
                JOIN medication_categories mc ON mc.medication_id = m.id
                WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
            ),
            user_prescription_coverage AS (
                SELECT 
                    user_id,
                    MIN(prescribed_at) as first_prescription_date,
                    MAX(prescription_end_date) as last_prescription_end_date,
                    SUM(total_prescription_days) as total_covered_days,
                    DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days,
                    -- Calculate gap percentage
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
            FROM user_prescription_coverage
            WHERE gap_percentage < 5.0
            AND total_covered_days >= 90 
            """, "Create GLP1 users table (continuous medication only)"),
            ("CREATE INDEX idx_weightloss_glp1_user_id ON tmp_weightloss_glp1_users(user_id)", "Index GLP1 users table")
        ],
        
        # Apple + Amazon Users EXCLUDING Weightloss GLP1 Users
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_apple_and_amazon_no_weightloss_glp1", "Drop Apple+Amazon no GLP1 table"),
            ("""CREATE TEMPORARY TABLE tmp_apple_and_amazon_no_weightloss_glp1 AS
                SELECT au.user_id
                FROM tmp_apple_and_amazon_users au
                LEFT JOIN (
                    SELECT DISTINCT p.patient_user_id AS user_id
                    FROM prescriptions p
                    JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
                    JOIN medications m ON m.id = ndcs.medication_id
                    WHERE m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%'
                ) glp1_any ON au.user_id = glp1_any.user_id
                WHERE glp1_any.user_id IS NULL
            """, "Create Apple + Amazon no GLP1 table (any GLP1 prescription)"),
            ("CREATE INDEX idx_apple_amazon_no_wl_user_id ON tmp_apple_and_amazon_no_weightloss_glp1(user_id)", "Index Apple+Amazon no GLP1 table")
        ],
        
        # Module Completion Info - ONLY for Apple+Amazon users
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_module_completion_info", "Drop module completion table"),
            ("""CREATE TEMPORARY TABLE tmp_module_completion_info AS
            WITH module_completion_raw AS (
                SELECT
                    t.user_id,
                    SUM(CASE WHEN t.slug = 'coaching03' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_coaching_03,
                    SUM(CASE WHEN t.slug = 'coaching05' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_coaching_05,
                    SUM(CASE WHEN t.slug = 'coaching08' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_coaching_08,
                    SUM(CASE WHEN t.slug = 'coaching12' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_coaching_12,
                    SUM(CASE WHEN t.slug = 'coaching05' AND status = 'SKIPPED' THEN 1 ELSE 0 END) > 0 as skipped_coaching_05,
                    SUM(CASE WHEN t.slug = 'coaching08' AND status = 'SKIPPED' THEN 1 ELSE 0 END) > 0 as skipped_coaching_08
                FROM tmp_apple_and_amazon_users aaau
                JOIN tasks t ON aaau.user_id = t.user_id
                WHERE t.program = 'path-to-healthy-weight'
                GROUP BY t.user_id
            ),
            module_completion AS (
                SELECT
                    *,
                    (completed_coaching_03 AND completed_coaching_05 AND completed_coaching_08 AND completed_coaching_12) as completed_3_5_8_12_coaching_visits,
                    (completed_coaching_03 AND completed_coaching_12 AND skipped_coaching_05 AND skipped_coaching_08) as skipped_optional_5_8_coaching_visits
                FROM module_completion_raw
            )
            SELECT * FROM module_completion""", "Create module completion table"),
            ("CREATE INDEX idx_module_completion_user_id ON tmp_module_completion_info(user_id)", "Index module completion table")
        ],
        
        # All remaining cohort tables
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_apple_and_amazon_completed_all_coaching_visits", "Drop completed coaching table"),
            ("""CREATE TEMPORARY TABLE tmp_apple_and_amazon_completed_all_coaching_visits AS
            SELECT * FROM tmp_module_completion_info WHERE completed_3_5_8_12_coaching_visits = true""", "Create completed coaching table"),
            ("CREATE INDEX idx_completed_coaching_user_id ON tmp_apple_and_amazon_completed_all_coaching_visits(user_id)", "Index completed coaching table")
        ],
        
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_apple_and_amazon_skipped_week_5_and_8_coaching_visits", "Drop skipped coaching table"),
            ("""CREATE TEMPORARY TABLE tmp_apple_and_amazon_skipped_week_5_and_8_coaching_visits AS
            SELECT * FROM tmp_module_completion_info WHERE skipped_optional_5_8_coaching_visits = true""", "Create skipped coaching table"),
            ("CREATE INDEX idx_skipped_coaching_user_id ON tmp_apple_and_amazon_skipped_week_5_and_8_coaching_visits(user_id)", "Index skipped coaching table")
        ],
        
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_apple_and_amazon_on_weight_glp1", "Drop Apple+Amazon on GLP1 table"),
            ("""CREATE TEMPORARY TABLE tmp_apple_and_amazon_on_weight_glp1 AS
            SELECT wgu.user_id
            FROM tmp_weightloss_glp1_users wgu
            JOIN tmp_apple_and_amazon_users aaau ON aaau.user_id = wgu.user_id""", "Create Apple+Amazon on GLP1 table"),
            ("CREATE INDEX idx_apple_amazon_on_wl_user_id ON tmp_apple_and_amazon_on_weight_glp1(user_id)", "Index Apple+Amazon on GLP1 table")
        ],
        
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_apple_on_weight_glp1", "Drop Apple on GLP1 table"),
            ("""CREATE TEMPORARY TABLE tmp_apple_on_weight_glp1 AS
            SELECT wgu.user_id
            FROM tmp_weightloss_glp1_users wgu
            JOIN tmp_apple_users au ON au.user_id = wgu.user_id""", "Create Apple on GLP1 table"),
            ("CREATE INDEX idx_apple_on_wl_user_id ON tmp_apple_on_weight_glp1(user_id)", "Index Apple on GLP1 table")
        ],
        
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_amazon_on_weight_glp1", "Drop Amazon on GLP1 table"),
            ("""CREATE TEMPORARY TABLE tmp_amazon_on_weight_glp1 AS
            SELECT wgu.user_id
            FROM tmp_weightloss_glp1_users wgu
            JOIN tmp_amazon_users au ON au.user_id = wgu.user_id""", "Create Amazon on GLP1 table"),
            ("CREATE INDEX idx_amazon_on_wl_user_id ON tmp_amazon_on_weight_glp1(user_id)", "Index Amazon on GLP1 table")
        ],
        
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_apple_no_weightloss_glp1", "Drop Apple no GLP1 table"),
            ("""CREATE TEMPORARY TABLE tmp_apple_no_weightloss_glp1 AS
                SELECT au.user_id
                FROM tmp_apple_users au
                LEFT JOIN (
                    SELECT DISTINCT p.patient_user_id AS user_id
                    FROM prescriptions p
                    JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
                    JOIN medications m ON m.id = ndcs.medication_id
                    WHERE m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%'
                ) glp1_any ON au.user_id = glp1_any.user_id
                WHERE glp1_any.user_id IS NULL
            """, "Create Apple no GLP1 table (any GLP1 prescription)"),
            ("CREATE INDEX idx_apple_no_wl_user_id ON tmp_apple_no_weightloss_glp1(user_id)", "Index Apple no GLP1 table")
        ],
        
        [
            ("DROP TEMPORARY TABLE IF EXISTS tmp_amazon_no_weightloss_glp1", "Drop Amazon no GLP1 table"),
            ("""CREATE TEMPORARY TABLE tmp_amazon_no_weightloss_glp1 AS
                SELECT au.user_id
                FROM tmp_amazon_users au
                LEFT JOIN (
                    SELECT DISTINCT p.patient_user_id AS user_id
                    FROM prescriptions p
                    JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
                    JOIN medications m ON m.id = ndcs.medication_id
                    WHERE m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%'
                ) glp1_any ON au.user_id = glp1_any.user_id
                WHERE glp1_any.user_id IS NULL
            """, "Create Amazon no GLP1 table (any GLP1 prescription)"),
            ("CREATE INDEX idx_amazon_no_wl_user_id ON tmp_amazon_no_weightloss_glp1(user_id)", "Index Amazon no GLP1 table")
        ]
    ]
    
    print("🚀 Creating temporary tables with timing...")
    total_start_time = time.time()
    
    for i, query_set in enumerate(temp_table_queries):
        table_start_time = time.time()
        print(f"\n📊 Creating table group {i+1}/{len(temp_table_queries)}:")
        
        try:
            # Execute each statement in the query set with timing
            for query, description in query_set:
                execute_with_timing(cursor, query, description)
            
            table_duration = time.time() - table_start_time
            print(f"  ✅ Table group {i+1} completed in {table_duration:.2f}s")
            
        except Exception as e:
            print(f"  ❌ Error creating table group {i+1}: {e}")
            raise
    
    total_duration = time.time() - total_start_time
    print(f"\n🎉 All temporary tables created in {total_duration:.2f}s")

def create_all_health_metrics_at_once(cursor):
    """Create health metrics ensuring exactly ONE row per user in each CTE table"""
    
    print("\n🏥 Creating unified health metrics with ONE row per user per table...")
    health_metrics_start_time = time.time()
    
    # Step 1: Create subscription starts
    print("\n📅 Step 1: User base with subscriptions")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_subscription_starts_all", "Drop subscription starts table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_subscription_starts_all AS
        SELECT aaau.user_id, s.start_date as subscription_start_date
        FROM tmp_apple_and_amazon_users aaau
        JOIN subscriptions s ON aaau.user_id = s.user_id
        WHERE s.status = 'ACTIVE'
        GROUP BY aaau.user_id
    """, "Create subscription starts table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_sub_starts_all_user_id ON tmp_subscription_starts_all(user_id)", "Index subscription starts table")
    
    # Step 2: Create filtered user base (like the first CTE)
    print("\n👥 Step 2: Filtered user base")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_user_base_filtered", "Drop user base filtered table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_user_base_filtered AS
        SELECT aaau.user_id, ss.subscription_start_date
        FROM tmp_apple_and_amazon_users aaau
        JOIN tmp_6_months_retention_users tr ON aaau.user_id = tr.user_id
        JOIN tmp_subscription_starts_all ss ON aaau.user_id = ss.user_id
    """, "Create filtered user base table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_user_base_filtered_user_id ON tmp_user_base_filtered(user_id)", "Index filtered user base table")
    
    # Step 3: Baseline weight - 30 days BEFORE to ANY TIME AFTER (first measurement)
    print("\n⚖️  Step 3: Baseline weight measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_baseline_weight", "Drop baseline weight CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_baseline_weight AS
        SELECT 
            user_id,
            baseline_weight_date,
            baseline_weight_lbs
        FROM (
            SELECT 
                ub.user_id,
                pbwv.effective as baseline_weight_date,
                pbwv.value * 2.20462 as baseline_weight_lbs,
                ROW_NUMBER() OVER (PARTITION BY ub.user_id ORDER BY pbwv.effective ASC) as rn  -- FIRST measurement
            FROM tmp_user_base_filtered ub
            JOIN body_weight_values_cleaned pbwv ON ub.user_id = pbwv.user_id
            WHERE pbwv.effective >= DATE_SUB(ub.subscription_start_date, INTERVAL 30 DAY)  -- 30 days BEFORE
            -- NO upper bound - any time after is OK
        ) ranked
        WHERE rn = 1
    """, "Create baseline weight CTE table (30 days before to any time after - first measurement)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_baseline_weight_user_id ON tmp_cte_baseline_weight(user_id)", "Index baseline weight CTE table")
    
    # Step 4: Latest weight - ONLY rn=1 rows (ONE per user)
    print("\n⚖️  Step 4: Latest weight measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_latest_weight", "Drop latest weight CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_latest_weight AS
        SELECT 
            user_id,
            latest_weight_lbs
        FROM (
            SELECT 
                bw.user_id,
                pbwv.value * 2.20462 as latest_weight_lbs,
                ROW_NUMBER() OVER (PARTITION BY bw.user_id ORDER BY pbwv.effective DESC) as rn
            FROM tmp_cte_baseline_weight bw
            JOIN body_weight_values_cleaned pbwv ON bw.user_id = pbwv.user_id
            WHERE pbwv.effective >= DATE_ADD(bw.baseline_weight_date, INTERVAL 180 DAY)
        ) ranked
        WHERE rn = 1
    """, "Create latest weight CTE table (ONE row per user)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_latest_weight_user_id ON tmp_cte_latest_weight(user_id)", "Index latest weight CTE table")
    
    # Step 5: Baseline BMI - 30 days BEFORE to ANY TIME AFTER (first measurement)
    print("\n🔢 Step 5: Baseline BMI measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_baseline_bmi", "Drop baseline BMI CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_baseline_bmi AS
        SELECT 
            user_id,
            baseline_bmi_date,
            baseline_bmi
        FROM (
            SELECT 
                ub.user_id,
                bmiv.effective_date as baseline_bmi_date,
                bmiv.value as baseline_bmi,
                ROW_NUMBER() OVER (PARTITION BY ub.user_id ORDER BY bmiv.effective_date ASC) as rn  -- FIRST measurement
            FROM tmp_user_base_filtered ub
            JOIN bmi_values bmiv ON ub.user_id = bmiv.user_id
            WHERE bmiv.effective_date >= DATE_SUB(ub.subscription_start_date, INTERVAL 30 DAY)  -- 30 days BEFORE
              AND bmiv.value IS NOT NULL
              AND bmiv.value <= 100
            -- NO upper bound - any time after is OK
        ) ranked
        WHERE rn = 1
          AND baseline_bmi IS NOT NULL
    """, "Create baseline BMI CTE table (30 days before to any time after - first measurement, not null)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_baseline_bmi_user_id ON tmp_cte_baseline_bmi(user_id)", "Index baseline BMI CTE table")
    
    # Step 6: Latest BMI - ONLY rn=1 rows (ONE per user) 
    print("\n🔢 Step 6: Latest BMI measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_latest_bmi", "Drop latest BMI CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_latest_bmi AS
        SELECT 
            user_id,
            latest_bmi
        FROM (
            SELECT 
                bb.user_id,
                bmiv.value as latest_bmi,
                ROW_NUMBER() OVER (PARTITION BY bb.user_id ORDER BY bmiv.effective_date DESC) as rn
            FROM tmp_cte_baseline_bmi bb
            JOIN bmi_values bmiv ON bb.user_id = bmiv.user_id
            WHERE bmiv.effective_date >= DATE_ADD(bb.baseline_bmi_date, INTERVAL 180 DAY)
              AND bmiv.value IS NOT NULL
              AND bmiv.value <= 100
        ) ranked
        WHERE rn = 1
          AND latest_bmi IS NOT NULL
    """, "Create latest BMI CTE table (ONE row per user, BMI <= 100)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_latest_bmi_user_id ON tmp_cte_latest_bmi(user_id)", "Index latest BMI CTE table")
    
    # Step 7: Baseline A1C - 30 days BEFORE to ANY TIME AFTER (first measurement)
    print("\n🩸 Step 7: Baseline A1C measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_baseline_a1c", "Drop baseline A1C CTE table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_baseline_a1c AS
        SELECT 
            user_id,
            baseline_a1c_date,
            baseline_a1c
        FROM (
            SELECT 
                ub.user_id,
                a1cv.effective_date as baseline_a1c_date,
                a1cv.value as baseline_a1c,
                ROW_NUMBER() OVER (PARTITION BY ub.user_id ORDER BY a1cv.effective_date ASC) as rn  -- FIRST measurement
            FROM tmp_user_base_filtered ub
            JOIN a1c_values a1cv ON ub.user_id = a1cv.user_id
            WHERE a1cv.effective_date >= DATE_SUB(ub.subscription_start_date, INTERVAL 30 DAY)  -- 30 days BEFORE
              AND a1cv.value IS NOT NULL
        ) ranked
        WHERE rn = 1
          AND baseline_a1c IS NOT NULL
    """, "Create baseline A1C CTE table (30 days before to any time after - first measurement, not null)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_baseline_a1c_user_id ON tmp_cte_baseline_a1c(user_id)", "Index baseline A1C CTE table")
    
    # Step 8: Latest A1C - Change to 180 days
    print("\n🩸 Step 8: Latest A1C measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_latest_a1c", "Drop latest A1C CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_latest_a1c AS
        SELECT 
            user_id,
            latest_a1c
        FROM (
            SELECT 
                ba.user_id,
                a1cv.value as latest_a1c,
                ROW_NUMBER() OVER (PARTITION BY ba.user_id ORDER BY a1cv.effective_date DESC) as rn
            FROM tmp_cte_baseline_a1c ba
            JOIN a1c_values a1cv ON ba.user_id = a1cv.user_id
            WHERE a1cv.effective_date >= DATE_ADD(ba.baseline_a1c_date, INTERVAL 180 DAY)
              AND a1cv.value IS NOT NULL
        ) ranked
        WHERE rn = 1
    """, "Create latest A1C CTE table (ONE row per user)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_latest_a1c_user_id ON tmp_cte_latest_a1c(user_id)", "Index latest A1C CTE table")
    
    # Step 9: Baseline BP - 30 days BEFORE to ANY TIME AFTER (first measurement)
    print("\n🫀 Step 9: Baseline blood pressure measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_baseline_bp", "Drop baseline BP CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_baseline_bp AS
        SELECT 
            user_id,
            baseline_bp_date,
            baseline_bp_systolic,
            baseline_bp_diastolic
        FROM (
            SELECT 
                ub.user_id,
                bpv.effective_date as baseline_bp_date,
                bpv.systolic as baseline_bp_systolic,
                bpv.diastolic as baseline_bp_diastolic,
                ROW_NUMBER() OVER (PARTITION BY ub.user_id ORDER BY bpv.effective_date ASC) as rn  -- FIRST measurement
            FROM tmp_user_base_filtered ub
            JOIN blood_pressure_values bpv ON ub.user_id = bpv.user_id
            WHERE bpv.effective_date >= DATE_SUB(ub.subscription_start_date, INTERVAL 30 DAY)  -- 30 days BEFORE
            -- NO upper bound - any time after is OK
        ) ranked
        WHERE rn = 1
    """, "Create baseline BP CTE table (30 days before to any time after - first measurement)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_baseline_bp_user_id ON tmp_cte_baseline_bp(user_id)", "Index baseline BP CTE table")
    
    # Step 10: Latest BP - Change to 180 days
    print("\n🫀 Step 10: Latest blood pressure measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_latest_bp", "Drop latest BP CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_latest_bp AS
        SELECT 
            user_id,
            latest_bp_systolic,
            latest_bp_diastolic
        FROM (
            SELECT 
                bbp.user_id,
                bpv.systolic as latest_bp_systolic,
                bpv.diastolic as latest_bp_diastolic,
                ROW_NUMBER() OVER (PARTITION BY bbp.user_id ORDER BY bpv.effective_date DESC) as rn
            FROM tmp_cte_baseline_bp bbp
            JOIN blood_pressure_values bpv ON bbp.user_id = bpv.user_id
            WHERE bpv.effective_date >= DATE_ADD(bbp.baseline_bp_date, INTERVAL 180 DAY)  -- CHANGED: 30 → 180
        ) ranked
        WHERE rn = 1
    """, "Create latest BP CTE table (ONE row per user)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_latest_bp_user_id ON tmp_cte_latest_bp(user_id)", "Index latest BP CTE table")
    
    # Step 11: Baseline Waist Circumference - 30 days BEFORE to ANY TIME AFTER (first measurement)
    print("\n📏 Step 11: Baseline waist circumference measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_baseline_waist_circ", "Drop baseline waist circumference CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_baseline_waist_circ AS
        SELECT 
            user_id,
            baseline_waist_circ_date,
            baseline_waist_circ_inches
        FROM (
            SELECT 
                ub.user_id,
                oo.effective_date as baseline_waist_circ_date,
                oo.value_quantity * 0.393701 as baseline_waist_circ_inches,  -- Convert cm to inches
                ROW_NUMBER() OVER (PARTITION BY ub.user_id ORDER BY oo.effective_date ASC) as rn
            FROM tmp_user_base_filtered ub
            JOIN observation_observations oo ON ub.user_id = oo.user_id
            WHERE oo.loinc = '56086-2'  -- Waist circumference LOINC
            AND oo.effective_date >= DATE_SUB(ub.subscription_start_date, INTERVAL 30 DAY)
        ) ranked
        WHERE rn = 1
    """, "Create baseline waist circumference CTE table")
    execute_with_timing(cursor, "CREATE INDEX idx_cte_baseline_waist_circ_user_id ON tmp_cte_baseline_waist_circ(user_id)", "Index baseline waist circumference CTE table")
    
    # Step 12: Latest Waist Circumference
    print("\n📏 Step 12: Latest waist circumference measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_latest_waist_circ", "Drop latest waist circumference CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_latest_waist_circ AS
        SELECT 
            user_id,
            latest_waist_circ_inches
        FROM (
            SELECT 
                bwc.user_id,
                oo.value_quantity * 0.393701 as latest_waist_circ_inches,
                ROW_NUMBER() OVER (PARTITION BY bwc.user_id ORDER BY oo.effective_date DESC) as rn
            FROM tmp_cte_baseline_waist_circ bwc
            JOIN observation_observations oo ON bwc.user_id = oo.user_id
            WHERE oo.loinc = '56086-2'
            AND oo.effective_date >= DATE_ADD(bwc.baseline_waist_circ_date, INTERVAL 180 DAY)
        ) ranked
        WHERE rn = 1
    """, "Create latest waist circumference CTE table")
    execute_with_timing(cursor, "CREATE INDEX idx_cte_latest_waist_circ_user_id ON tmp_cte_latest_waist_circ(user_id)", "Index latest waist circumference CTE table")
    
    # Step 13: Baseline Triglycerides - FIXED LOINC codes
    print("\n🩸 Step 13: Baseline triglyceride measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_baseline_triglycerides", "Drop baseline triglycerides CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_baseline_triglycerides AS
        SELECT 
            user_id,
            baseline_triglycerides_date,
            baseline_triglycerides_mg_dl
        FROM (
            SELECT 
                ub.user_id,
                oo.effective_date as baseline_triglycerides_date,
                oo.value_quantity as baseline_triglycerides_mg_dl,
                ROW_NUMBER() OVER (PARTITION BY ub.user_id ORDER BY oo.effective_date ASC) as rn
            FROM tmp_user_base_filtered ub
            JOIN observation_observations oo ON ub.user_id = oo.user_id
            WHERE oo.loinc IN ('2571-8', '96598-8')  -- CORRECTED: Only triglycerides codes
            AND oo.effective_date >= DATE_SUB(ub.subscription_start_date, INTERVAL 30 DAY)
        ) ranked
        WHERE rn = 1
    """, "Create baseline triglycerides CTE table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_baseline_triglycerides_user_id ON tmp_cte_baseline_triglycerides(user_id)", "Index baseline triglycerides CTE table")

    # Step 14: Latest Triglycerides - FIXED LOINC codes
    print("\n🩸 Step 14: Latest triglyceride measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_latest_triglycerides", "Drop latest triglycerides CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_latest_triglycerides AS
        SELECT 
            user_id,
            latest_triglycerides_mg_dl
        FROM (
            SELECT 
                bt.user_id,
                oo.value_quantity as latest_triglycerides_mg_dl,
                ROW_NUMBER() OVER (PARTITION BY bt.user_id ORDER BY oo.effective_date DESC) as rn
            FROM tmp_cte_baseline_triglycerides bt
            JOIN observation_observations oo ON bt.user_id = oo.user_id
            WHERE oo.loinc IN ('2571-8', '96598-8')  -- CORRECTED: Only triglycerides codes
            AND oo.effective_date >= DATE_ADD(bt.baseline_triglycerides_date, INTERVAL 180 DAY)
        ) ranked
        WHERE rn = 1
    """, "Create latest triglycerides CTE table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_latest_triglycerides_user_id ON tmp_cte_latest_triglycerides(user_id)", "Index latest triglycerides CTE table")

    # Step 15: Baseline HDL - FIXED LOINC codes
    print("\n🩸 Step 15: Baseline HDL measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_baseline_hdl", "Drop baseline HDL CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_baseline_hdl AS
        SELECT 
            user_id,
            baseline_hdl_date,
            baseline_hdl_mg_dl
        FROM (
            SELECT 
                ub.user_id,
                oo.effective_date as baseline_hdl_date,
                oo.value_quantity as baseline_hdl_mg_dl,
                ROW_NUMBER() OVER (PARTITION BY ub.user_id ORDER BY oo.effective_date ASC) as rn
            FROM tmp_user_base_filtered ub
            JOIN observation_observations oo ON ub.user_id = oo.user_id
            WHERE oo.loinc IN ('96596-2', '2085-9')  -- CORRECTED: Only HDL codes
            AND oo.effective_date >= DATE_SUB(ub.subscription_start_date, INTERVAL 30 DAY)
        ) ranked
        WHERE rn = 1
    """, "Create baseline HDL CTE table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_baseline_hdl_user_id ON tmp_cte_baseline_hdl(user_id)", "Index baseline HDL CTE table")

    # Step 16: Latest HDL - FIXED LOINC codes
    print("\n🩸 Step 16: Latest HDL measurements")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_cte_latest_hdl", "Drop latest HDL CTE table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_cte_latest_hdl AS
        SELECT 
            user_id,
            latest_hdl_mg_dl
        FROM (
            SELECT 
                bh.user_id,
                oo.value_quantity as latest_hdl_mg_dl,
                ROW_NUMBER() OVER (PARTITION BY bh.user_id ORDER BY oo.effective_date DESC) as rn
            FROM tmp_cte_baseline_hdl bh
            JOIN observation_observations oo ON bh.user_id = oo.user_id
            WHERE oo.loinc IN ('96596-2', '2085-9')  -- CORRECTED: Only HDL codes
            AND oo.effective_date >= DATE_ADD(bh.baseline_hdl_date, INTERVAL 180 DAY)
        ) ranked
        WHERE rn = 1
    """, "Create latest HDL CTE table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_cte_latest_hdl_user_id ON tmp_cte_latest_hdl(user_id)", "Index latest HDL CTE table")
    
    # Step 17: Final assembly - Add new metrics to master table
    print("\n🔗 Step 17: Final assembly with all metrics including new ones")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_master_health_metrics", "Drop master health metrics table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_master_health_metrics AS
        SELECT 
            ub.user_id,
            
            -- Existing baseline values WITH dates
            bw.baseline_weight_lbs,
            bw.baseline_weight_date,
            bb.baseline_bmi,
            bb.baseline_bmi_date,
            ba.baseline_a1c,
            ba.baseline_a1c_date,
            bbp.baseline_bp_systolic,
            bbp.baseline_bp_diastolic,
            bbp.baseline_bp_date,
            
            -- Existing latest values
            lw.latest_weight_lbs,
            lb.latest_bmi,
            la.latest_a1c,
            lbp.latest_bp_systolic,
            lbp.latest_bp_diastolic,
            
            -- NEW: Waist circumference values
            bwc.baseline_waist_circ_inches,
            bwc.baseline_waist_circ_date,
            lwc.latest_waist_circ_inches,
            
            -- NEW: Triglycerides values
            bt.baseline_triglycerides_mg_dl,
            bt.baseline_triglycerides_date,
            lt.latest_triglycerides_mg_dl,
            
            -- NEW: HDL values
            bh.baseline_hdl_mg_dl,
            bh.baseline_hdl_date,
            lh.latest_hdl_mg_dl
            
        FROM tmp_user_base_filtered ub
        LEFT JOIN tmp_cte_baseline_weight bw ON ub.user_id = bw.user_id
        LEFT JOIN tmp_cte_latest_weight lw ON ub.user_id = lw.user_id
        LEFT JOIN tmp_cte_baseline_bmi bb ON ub.user_id = bb.user_id
        LEFT JOIN tmp_cte_latest_bmi lb ON ub.user_id = lb.user_id
        LEFT JOIN tmp_cte_baseline_a1c ba ON ub.user_id = ba.user_id
        LEFT JOIN tmp_cte_latest_a1c la ON ub.user_id = la.user_id
        LEFT JOIN tmp_cte_baseline_bp bbp ON ub.user_id = bbp.user_id
        LEFT JOIN tmp_cte_latest_bp lbp ON ub.user_id = lbp.user_id
        -- NEW JOINs for new metrics
        LEFT JOIN tmp_cte_baseline_waist_circ bwc ON ub.user_id = bwc.user_id
        LEFT JOIN tmp_cte_latest_waist_circ lwc ON ub.user_id = lwc.user_id
        LEFT JOIN tmp_cte_baseline_triglycerides bt ON ub.user_id = bt.user_id
        LEFT JOIN tmp_cte_latest_triglycerides lt ON ub.user_id = lt.user_id
        LEFT JOIN tmp_cte_baseline_hdl bh ON ub.user_id = bh.user_id
        LEFT JOIN tmp_cte_latest_hdl lh ON ub.user_id = lh.user_id
    """, "Create master health metrics table with new metrics")
    
    execute_with_timing(cursor, "CREATE INDEX idx_master_health_user_id ON tmp_master_health_metrics(user_id)", "Index master health metrics table")
    
    # Step 18: Cleanup all intermediate CTE tables (including new ones)
    print("\n🧹 Step 18: Cleaning up intermediate CTE tables")
    intermediate_cte_tables = [
        'tmp_user_base_filtered',
        'tmp_cte_baseline_weight', 'tmp_cte_latest_weight',
        'tmp_cte_baseline_bmi', 'tmp_cte_latest_bmi', 
        'tmp_cte_baseline_a1c', 'tmp_cte_latest_a1c',
        'tmp_cte_baseline_bp', 'tmp_cte_latest_bp',
        # NEW: Add cleanup for new metric tables
        'tmp_cte_baseline_waist_circ', 'tmp_cte_latest_waist_circ',
        'tmp_cte_baseline_triglycerides', 'tmp_cte_latest_triglycerides',
        'tmp_cte_baseline_hdl', 'tmp_cte_latest_hdl'
    ]
    
    cleanup_start = time.time()
    for table in intermediate_cte_tables:
        try:
            cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {table}")
        except:
            pass
    cleanup_duration = time.time() - cleanup_start
    print(f"  ⏱️  Cleanup intermediate CTE tables: {cleanup_duration:.2f}s")
    
    total_health_duration = time.time() - health_metrics_start_time
    print(f"\n  ✅ Master health metrics completed in {total_health_duration:.2f}s")

def create_engagement_metrics(cursor):
    """Create engagement metric tables for care team interactions, module completion, and consultations"""
    
    print("\n🤝 Creating engagement metrics...")
    engagement_start_time = time.time()
    
    # Step 1: Average care team interactions per month
    print("\n💬 Step 1: Care team interactions per month")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_avg_care_team_interactions_per_6month_user", "Drop care team interactions table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_avg_care_team_interactions_per_6month_user AS
        SELECT 
            t6mru.user_id,
            COUNT(DISTINCT ba.id) as total_interactions,
            t6mru.days_with_active_subscriptions as active_days,
            COUNT(DISTINCT ba.id) / (t6mru.days_with_active_subscriptions/30.0) as avg_interactions_per_month
        FROM tmp_6_months_retention_users t6mru
        LEFT JOIN billable_activities ba ON t6mru.user_id = ba.user_id
            AND ba.type IN ('TEXT_MESSAGE_CARE_ONLY', 'VOICE_MESSAGE_CARE_ONLY', 'VIDEO_CALL_COMPLETED', 'COMPLETED_CONSULTATION', 'QUESTIONNAIRE_ANSWERED')
        GROUP BY t6mru.user_id, t6mru.days_with_active_subscriptions
    """, "Create care team interactions table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_care_team_interactions_user_id ON tmp_avg_care_team_interactions_per_6month_user(user_id)", "Index care team interactions table")
    
    # Step 2: Module completion metrics
    print("\n📚 Step 2: Module completion metrics")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_module_completion", "Drop module completion table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_module_completion AS
        WITH module_completion_raw AS (
            SELECT
                t.user_id,
                SUM(CASE WHEN t.`group` = 'module01' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_01,
                SUM(CASE WHEN t.`group` = 'module02' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_02,
                SUM(CASE WHEN t.`group` = 'module03' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_03,
                SUM(CASE WHEN t.`group` = 'module04' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_04,
                SUM(CASE WHEN t.`group` = 'module05' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_05,
                SUM(CASE WHEN t.`group` = 'module06' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_06,
                SUM(CASE WHEN t.`group` = 'module07' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_07,
                SUM(CASE WHEN t.`group` = 'module08' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_08,
                SUM(CASE WHEN t.`group` = 'module09' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_09,
                SUM(CASE WHEN t.`group` = 'module10' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_10,
                SUM(CASE WHEN t.`group` = 'module11' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_11,
                SUM(CASE WHEN t.`group` = 'module12' AND status = 'COMPLETED' THEN 1 ELSE 0 END) > 0 as completed_module_12
            FROM tasks t
            WHERE t.program = 'path-to-healthy-weight'
            GROUP BY t.user_id
        ),
        module_completion AS (
            SELECT
                *,
                (completed_module_01 AND completed_module_02 AND completed_module_03 AND completed_module_04 AND
                 completed_module_05 AND completed_module_06 AND completed_module_07 AND completed_module_08 AND
                 completed_module_09 AND completed_module_10 AND completed_module_11 AND completed_module_12) as completed_all_modules,
                (completed_module_01 + completed_module_02 + completed_module_03 + completed_module_04 +
                 completed_module_05 + completed_module_06 + completed_module_07 + completed_module_08 +
                 completed_module_09 + completed_module_10 + completed_module_11 + completed_module_12) as total_modules_completed
            FROM module_completion_raw
        )
        SELECT * FROM module_completion
    """, "Create module completion table"),
    execute_with_timing(cursor, "CREATE INDEX idx_module_completion_user_id ON tmp_module_completion(user_id)", "Index module completion table")
    
    # Step 3: Physician consultations (non-order-only)
    print("\n👨‍⚕️ Step 3: Physician consultations")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_completed_non_orderonly_consultations", "Drop consultations table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_completed_non_orderonly_consultations AS
        SELECT 
            c.user_id, 
            COUNT(*) as completed_consultations
        FROM consultations c
        WHERE c.consultation_type NOT IN ('ORDER_ONLY_CONSULTATION')
        AND c.status IN ('COMPLETED')
        GROUP BY c.user_id
    """, "Create consultations table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_consultations_user_id ON tmp_completed_non_orderonly_consultations(user_id)", "Index consultations table")
    
    total_engagement_duration = time.time() - engagement_start_time
    print(f"\n  ✅ Engagement metrics completed in {total_engagement_duration:.2f}s")

def get_engagement_metrics_query(cohort_table: str, cohort_name: str) -> str:
    """Get engagement metrics for a cohort"""
    return f"""
        SELECT 
            '{cohort_name}' as cohort,
            COUNT(DISTINCT ct.user_id) as total_users,
            COUNT(DISTINCT t6mru.user_id) as users_after_6_month_retention,
            
            -- Care team interaction metrics
            ROUND(AVG(IFNULL(ctm.avg_interactions_per_month, 0)), 2) as avg_care_team_interactions_per_month,
            COUNT(CASE WHEN ctm.avg_interactions_per_month IS NOT NULL THEN 1 END) as care_team_interactions_n,
            
            -- Module completion metrics  
            ROUND(AVG(IFNULL(mc.total_modules_completed, 0)), 2) as avg_modules_completed,
            COUNT(CASE WHEN mc.total_modules_completed IS NOT NULL THEN 1 END) as modules_completion_n,
            ROUND(SUM(CASE WHEN mc.completed_all_modules THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_completed_all_modules,
            SUM(CASE WHEN mc.completed_all_modules THEN 1 ELSE 0 END) as completed_all_modules_n,
            
            -- Physician consultation metrics
            ROUND(AVG(IFNULL(cnoc.completed_consultations, 0)), 2) as avg_completed_consultations,
            COUNT(CASE WHEN cnoc.completed_consultations IS NOT NULL THEN 1 END) as consultations_n
            
        FROM {cohort_table} ct
        LEFT JOIN tmp_6_months_retention_users t6mru ON ct.user_id = t6mru.user_id
        LEFT JOIN tmp_avg_care_team_interactions_per_6month_user ctm ON ct.user_id = ctm.user_id
        LEFT JOIN tmp_module_completion mc ON ct.user_id = mc.user_id  
        LEFT JOIN tmp_completed_non_orderonly_consultations cnoc ON ct.user_id = cnoc.user_id
    """


def get_super_optimized_query(cohort_table: str, cohort_name: str) -> str:
    """Ultra-fast query using pre-computed master health metrics - WITH TIME CALCULATIONS"""
    return f"""
        SELECT 
            '{cohort_name}' as cohort,
            COUNT(DISTINCT mhm.user_id) as total_users,
            
            -- Weight metrics with time calculations
            ROUND(AVG(mhm.baseline_weight_lbs), 2) as baseline_weight_avg,
            COUNT(CASE WHEN mhm.baseline_weight_lbs IS NOT NULL THEN 1 END) as baseline_weight_n,
            
            -- Average days between baseline and latest weight measurements
            ROUND(AVG(CASE WHEN mhm.baseline_weight_date IS NOT NULL 
                          THEN DATEDIFF(CURDATE(), mhm.baseline_weight_date) END), 0) as avg_days_since_baseline_weight,
            
            ROUND(AVG((mhm.baseline_weight_lbs - mhm.latest_weight_lbs) / mhm.baseline_weight_lbs * 100), 2) as weight_loss_pct,
            COUNT(CASE WHEN mhm.baseline_weight_lbs IS NOT NULL AND mhm.latest_weight_lbs IS NOT NULL THEN 1 END) as weight_loss_pct_n,
            ROUND(AVG(mhm.baseline_weight_lbs - mhm.latest_weight_lbs), 2) as weight_loss_lbs,
            ROUND(COUNT(CASE WHEN (mhm.baseline_weight_lbs - mhm.latest_weight_lbs) / mhm.baseline_weight_lbs * 100 >= 5 THEN 1 END) * 100.0 / 
                  COUNT(CASE WHEN mhm.baseline_weight_lbs IS NOT NULL AND mhm.latest_weight_lbs IS NOT NULL THEN 1 END), 2) as pct_lost_5pct,
            COUNT(CASE WHEN (mhm.baseline_weight_lbs - mhm.latest_weight_lbs) / mhm.baseline_weight_lbs * 100 >= 5 THEN 1 END) as lost_5pct_n,
            ROUND(COUNT(CASE WHEN (mhm.baseline_weight_lbs - mhm.latest_weight_lbs) / mhm.baseline_weight_lbs * 100 >= 10 THEN 1 END) * 100.0 / 
                  COUNT(CASE WHEN mhm.baseline_weight_lbs IS NOT NULL AND mhm.latest_weight_lbs IS NOT NULL THEN 1 END), 2) as pct_lost_10pct,
            COUNT(CASE WHEN (mhm.baseline_weight_lbs - mhm.latest_weight_lbs) / mhm.baseline_weight_lbs * 100 >= 10 THEN 1 END) as lost_10pct_n,
                
            -- BMI metrics with time calculations
            ROUND(AVG(mhm.baseline_bmi), 2) as baseline_bmi_avg,
            COUNT(CASE WHEN mhm.baseline_bmi IS NOT NULL THEN 1 END) as baseline_bmi_n,
            
            -- Average days since baseline BMI
            ROUND(AVG(CASE WHEN mhm.baseline_bmi_date IS NOT NULL 
                          THEN DATEDIFF(CURDATE(), mhm.baseline_bmi_date) END), 0) as avg_days_since_baseline_bmi,
            
            ROUND(AVG((mhm.baseline_bmi - mhm.latest_bmi) / mhm.baseline_bmi * 100), 2) as bmi_change_pct,
            COUNT(CASE WHEN mhm.baseline_bmi IS NOT NULL AND mhm.latest_bmi IS NOT NULL THEN 1 END) as bmi_change_pct_n,
            ROUND(AVG(mhm.baseline_bmi - mhm.latest_bmi), 2) as bmi_change_units, ---- !!!!!! FIX THIS !!!!!! 
                
            -- A1C metrics with time calculations
            ROUND(AVG(mhm.baseline_a1c), 2) as baseline_a1c_avg,
            COUNT(CASE WHEN mhm.baseline_a1c IS NOT NULL THEN 1 END) as baseline_a1c_n,
            
            -- Average days since baseline A1C
            ROUND(AVG(CASE WHEN mhm.baseline_a1c_date IS NOT NULL 
                          THEN DATEDIFF(CURDATE(), mhm.baseline_a1c_date) END), 0) as avg_days_since_baseline_a1c,
            
            ROUND(AVG(CASE WHEN mhm.baseline_a1c IS NOT NULL AND mhm.latest_a1c IS NOT NULL THEN mhm.baseline_a1c - mhm.latest_a1c END), 2) as a1c_change_avg,
            COUNT(CASE WHEN mhm.baseline_a1c IS NOT NULL AND mhm.latest_a1c IS NOT NULL THEN 1 END) as a1c_change_n,
            ROUND(AVG(CASE WHEN mhm.baseline_a1c >= 6.5 AND mhm.baseline_a1c < 8.0 AND mhm.latest_a1c IS NOT NULL THEN mhm.baseline_a1c - mhm.latest_a1c END), 2) as a1c_change_6_5_plus,
            COUNT(CASE WHEN mhm.baseline_a1c >= 6.5 AND mhm.baseline_a1c < 8.0 AND mhm.latest_a1c IS NOT NULL THEN 1 END) as a1c_change_6_5_plus_n,
            ROUND(AVG(CASE WHEN mhm.baseline_a1c >= 8.0 AND mhm.baseline_a1c < 9.0 AND mhm.latest_a1c IS NOT NULL THEN mhm.baseline_a1c - mhm.latest_a1c END), 2) as a1c_change_8_plus,
            COUNT(CASE WHEN mhm.baseline_a1c >= 8.0 AND mhm.baseline_a1c < 9.0 AND mhm.latest_a1c IS NOT NULL THEN 1 END) as a1c_change_8_plus_n,
            ROUND(AVG(CASE WHEN mhm.baseline_a1c >= 9.0 AND mhm.latest_a1c IS NOT NULL THEN mhm.baseline_a1c - mhm.latest_a1c END), 2) as a1c_change_9_plus,
            COUNT(CASE WHEN mhm.baseline_a1c >= 9.0 AND mhm.latest_a1c IS NOT NULL THEN 1 END) as a1c_change_9_plus_n,

            -- Blood Pressure metrics with time calculations
            ROUND(AVG(mhm.baseline_bp_systolic), 2) as baseline_bp_systolic_avg,
            ROUND(AVG(mhm.baseline_bp_diastolic), 2) as baseline_bp_diastolic_avg,
            COUNT(CASE WHEN mhm.baseline_bp_systolic IS NOT NULL THEN 1 END) as baseline_bp_n,
            
            -- Average days since baseline BP
            ROUND(AVG(CASE WHEN mhm.baseline_bp_date IS NOT NULL 
                          THEN DATEDIFF(CURDATE(), mhm.baseline_bp_date) END), 0) as avg_days_since_baseline_bp,
            
            ROUND(AVG(mhm.baseline_bp_systolic - mhm.latest_bp_systolic), 2) as bp_systolic_change,
            ROUND(AVG(mhm.baseline_bp_diastolic - mhm.latest_bp_diastolic), 2) as bp_diastolic_change,
            COUNT(CASE WHEN mhm.baseline_bp_systolic IS NOT NULL AND mhm.latest_bp_systolic IS NOT NULL THEN 1 END) as bp_change_n,
            ROUND(AVG(CASE WHEN mhm.baseline_bp_systolic >= 130 OR mhm.baseline_bp_diastolic >= 80
                         THEN mhm.baseline_bp_systolic - mhm.latest_bp_systolic END), 2) as systolic_bp_change_130_80_plus,
            ROUND(AVG(CASE WHEN mhm.baseline_bp_systolic >= 130 OR mhm.baseline_bp_diastolic >= 80
            THEN mhm.baseline_bp_diastolic - mhm.latest_bp_diastolic END), 2) as diastolic_bp_change_130_80_plus,
            COUNT(CASE WHEN (mhm.baseline_bp_systolic >= 130 OR mhm.baseline_bp_diastolic >= 80) AND
                          mhm.latest_bp_systolic IS NOT NULL THEN 1 END) as bp_change_130_80_plus_n,
            ROUND(AVG(CASE WHEN mhm.baseline_bp_systolic >= 140 OR mhm.baseline_bp_diastolic >= 90
                         THEN mhm.baseline_bp_systolic - mhm.latest_bp_systolic END), 2) as systolic_bp_change_140_90_plus,
            ROUND(AVG(CASE WHEN mhm.baseline_bp_systolic >= 140 OR mhm.baseline_bp_diastolic >= 90
            THEN mhm.baseline_bp_diastolic - mhm.latest_bp_diastolic END), 2) as diastolic_bp_change_140_90_plus,
            COUNT(CASE WHEN (mhm.baseline_bp_systolic >= 140 OR mhm.baseline_bp_diastolic >= 90) AND
                          mhm.latest_bp_systolic IS NOT NULL THEN 1 END) as bp_change_140_90_plus_n,
            
            -- NEW: Waist Circumference metrics
            ROUND(AVG(mhm.baseline_waist_circ_inches), 2) as baseline_waist_circ_avg,
            COUNT(CASE WHEN mhm.baseline_waist_circ_inches IS NOT NULL THEN 1 END) as baseline_waist_n,
            ROUND(AVG(mhm.baseline_waist_circ_inches - mhm.latest_waist_circ_inches), 2) as change_waist_circ_avg,
            COUNT(CASE WHEN mhm.baseline_waist_circ_inches IS NOT NULL AND mhm.latest_waist_circ_inches IS NOT NULL THEN 1 END) as change_waist_circ_n,
            
            -- NEW: Triglycerides metrics  
            ROUND(AVG(mhm.baseline_triglycerides_mg_dl), 2) as baseline_triglyceride_avg,
            COUNT(CASE WHEN mhm.baseline_triglycerides_mg_dl IS NOT NULL THEN 1 END) as baseline_triglyceride_n,
            ROUND(AVG(mhm.baseline_triglycerides_mg_dl - mhm.latest_triglycerides_mg_dl), 2) as change_triglyceride_avg,
            COUNT(CASE WHEN mhm.baseline_triglycerides_mg_dl IS NOT NULL AND mhm.latest_triglycerides_mg_dl IS NOT NULL THEN 1 END) as change_triglyceride_n,
            
            -- NEW: HDL metrics
            ROUND(AVG(mhm.baseline_hdl_mg_dl), 2) as baseline_HDL_avg,
            COUNT(CASE WHEN mhm.baseline_hdl_mg_dl IS NOT NULL THEN 1 END) as baseline_HDL_n,
            ROUND(AVG(mhm.latest_hdl_mg_dl - mhm.baseline_hdl_mg_dl), 2) as change_HDL_avg,  -- Note: HDL increase is good, so latest - baseline
            COUNT(CASE WHEN mhm.baseline_hdl_mg_dl IS NOT NULL AND mhm.latest_hdl_mg_dl IS NOT NULL THEN 1 END) as change_HDL_avg_n
            
        FROM tmp_master_health_metrics mhm
        JOIN {cohort_table} ct ON mhm.user_id = ct.user_id
    """

def get_weight_loss_users_query(cohort_table: str, cohort_name: str) -> str:
    """Get list of users who have both baseline and latest weight measurements"""
    return f"""
        SELECT 
            '{cohort_name}' as cohort,
            BIN_TO_UUID(mhm.user_id) as user_id,
            mhm.baseline_weight_lbs,
            mhm.latest_weight_lbs,
            ROUND((mhm.baseline_weight_lbs - mhm.latest_weight_lbs) / mhm.baseline_weight_lbs * 100, 2) as weight_loss_pct,
            ROUND(mhm.baseline_weight_lbs - mhm.latest_weight_lbs, 2) as weight_loss_lbs,
            mhm.baseline_weight_date,
            DATEDIFF(CURDATE(), mhm.baseline_weight_date) as days_since_baseline
        FROM tmp_master_health_metrics mhm
        JOIN {cohort_table} ct ON mhm.user_id = ct.user_id
        WHERE mhm.baseline_weight_lbs IS NOT NULL 
        AND mhm.latest_weight_lbs IS NOT NULL
        ORDER BY weight_loss_pct DESC
    """

def get_weight_loss_count_validation_query(cohort_table: str, cohort_name: str) -> str:
    """Get count validation for weight_loss_pct_n"""
    return f"""
        SELECT 
            '{cohort_name}' as cohort,
            COUNT(CASE WHEN mhm.baseline_weight_lbs IS NOT NULL AND mhm.latest_weight_lbs IS NOT NULL THEN 1 END) as weight_loss_pct_n_validation
        FROM tmp_master_health_metrics mhm
        JOIN {cohort_table} ct ON mhm.user_id = ct.user_id
    """

def get_prescription_statistics(cursor, cohort_table: str, cohort_name: str) -> dict:
    """Get prescription statistics using existing GLP1 table"""
    try:
        query = f"""
        SELECT 
            COUNT(DISTINCT wgu.user_id) as total_glp1_users,
            ROUND(AVG(wgu.total_covered_days), 2) as avg_total_prescription_days,
            ROUND(AVG(wgu.total_period_days), 2) as avg_total_continuous_days,
            ROUND(AVG(wgu.gap_percentage), 2) as avg_gap_percentage
        FROM tmp_weightloss_glp1_users wgu
        JOIN {cohort_table} ct ON wgu.user_id = ct.user_id
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        return {
            'total_glp1_users': result['total_glp1_users'] if result else 0,
            'avg_total_prescription_days': result['avg_total_prescription_days'] if result else 0,
            'avg_total_continuous_days': result['avg_total_period_days'] if result else 0,
            'avg_gap_percentage': result['avg_gap_percentage'] if result else 0
        }
    except Exception as e:
        print(f"      ⚠️  Error getting prescription stats: {e}")
        return {
            'total_glp1_users': 0,
            'avg_total_prescription_days': 0,
            'avg_total_continuous_days': 0,
            'avg_gap_percentage': 0
        }
    
def get_glp1_medication_metrics_query(cohort_table: str, cohort_name: str) -> str:
    """Get detailed GLP1 medication metrics for each cohort"""
    return f"""
        SELECT 
            '{cohort_name}' as cohort,
            COUNT(DISTINCT ct.user_id) as total_users,
            
            -- Question 1: Percentage prescribed GLP1 for weight loss
            COUNT(DISTINCT CASE WHEN m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' THEN ct.user_id END) as glp1_weight_loss_users,
            ROUND(COUNT(DISTINCT CASE WHEN m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' THEN ct.user_id END) * 100.0 / COUNT(DISTINCT ct.user_id), 2) as pct_prescribed_glp1_weight_loss,
            
            -- Question 2: Percentage prescribed no medications or any other medication
            COUNT(DISTINCT CASE WHEN p.id IS NULL THEN ct.user_id END) as no_medication_users,
            ROUND(COUNT(DISTINCT CASE WHEN p.id IS NULL THEN ct.user_id END) * 100.0 / COUNT(DISTINCT ct.user_id), 2) as pct_no_medication,
            COUNT(DISTINCT CASE WHEN p.id IS NOT NULL AND NOT (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%') THEN ct.user_id END) as other_medication_users,
            ROUND(COUNT(DISTINCT CASE WHEN p.id IS NOT NULL AND NOT (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%') THEN ct.user_id END) * 100.0 / COUNT(DISTINCT ct.user_id), 2) as pct_other_medication,
            ROUND((COUNT(DISTINCT CASE WHEN p.id IS NULL THEN ct.user_id END) + COUNT(DISTINCT CASE WHEN p.id IS NOT NULL AND NOT (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%') THEN ct.user_id END)) * 100.0 / COUNT(DISTINCT ct.user_id), 2) as pct_no_or_other_medication,
            
            -- Question 3: Average days on GLP1 for weight loss (simplified calculation)
            ROUND(AVG(CASE WHEN m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' 
                          THEN p.days_of_supply + (p.days_of_supply * p.total_refills) END), 2) as avg_days_on_glp1_weight_loss,
            
            -- Question 4: Simplified persistence metric (users with multiple prescriptions)
            COUNT(DISTINCT CASE WHEN m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' AND p.total_refills > 0 THEN ct.user_id END) as users_with_refills,
            ROUND(COUNT(DISTINCT CASE WHEN m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' AND p.total_refills > 0 THEN ct.user_id END) * 100.0 / 
                  NULLIF(COUNT(DISTINCT CASE WHEN m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' THEN ct.user_id END), 0), 2) as pct_with_refills_among_glp1_users,
            
            -- Additional metrics
            ROUND(AVG(CASE WHEN m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' THEN p.total_refills END), 2) as avg_refills_glp1_users,
            COUNT(CASE WHEN m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' THEN p.id END) as total_glp1_prescriptions
            
        FROM {cohort_table} ct
        LEFT JOIN prescriptions p ON ct.user_id = p.patient_user_id
        LEFT JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
        LEFT JOIN medications m ON m.id = ndcs.medication_id
        LEFT JOIN medication_categories mc ON mc.medication_id = m.id
    """
def main():
    """Streamlined main execution with comprehensive timing and validation"""
    
    cohorts = {
        'Apple+Amazon': 'tmp_apple_and_amazon_users',
        'Apple+Amazon Completed All Coaching': 'tmp_apple_and_amazon_completed_all_coaching_visits',
        'Apple+Amazon Skipped Optional Coaching': 'tmp_apple_and_amazon_skipped_week_5_and_8_coaching_visits',
        'Apple+Amazon Excluding Weightloss GLP1': 'tmp_apple_and_amazon_no_weightloss_glp1',
        'Apple+Amazon on Weight GLP1': 'tmp_apple_and_amazon_on_weight_glp1',
        'Apple Excluding Weightloss GLP1': 'tmp_apple_no_weightloss_glp1',
        'Apple on Weight GLP1': 'tmp_apple_on_weight_glp1',
        'Amazon Excluding Weightloss GLP1': 'tmp_amazon_no_weightloss_glp1',
        'Amazon on Weight GLP1': 'tmp_amazon_on_weight_glp1'
    }
    
    script_start_time = time.time()
    
    try:
        print("🔗 Connecting to database...")
        conn_start = time.time()
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        conn_duration = time.time() - conn_start
        print(f"  ⏱️  Database connection: {conn_duration:.2f}s")
        
        # Create temporary tables
        execute_temp_table_creation(cursor)
        
        # Create master health metrics
        create_all_health_metrics_at_once(cursor)
        
        # Create engagement metrics
        create_engagement_metrics(cursor)
        
        # Process all cohorts using pre-computed metrics
        print("\n📊 Processing cohorts:")
        cohort_start_time = time.time()
        all_results = []
        all_weight_loss_users = []
        all_prescription_stats = []
        all_engagement_results = []
        all_glp1_metrics = []  # NEW: Store detailed GLP1 metrics
        validation_results = {}
        
        for cohort_name, cohort_table in cohorts.items():
            print(f"\n  🎯 Processing: {cohort_name}")
            
            try:
                # Get health metrics
                query = get_super_optimized_query(cohort_table, cohort_name)
                cohort_query_start = time.time()
                cursor.execute(query)
                results = cursor.fetchall()
                cohort_duration = time.time() - cohort_query_start
                
                if results:
                    all_results.extend(results)
                    print(f"    ⏱️  Health metrics query: {cohort_duration:.2f}s")
                
                # Get engagement metrics
                engagement_query = get_engagement_metrics_query(cohort_table, cohort_name)
                engagement_start = time.time()
                cursor.execute(engagement_query)
                engagement_results = cursor.fetchall()
                engagement_duration = time.time() - engagement_start
                
                if engagement_results:
                    all_engagement_results.extend(engagement_results)
                    print(f"    ⏱️  Engagement metrics query: {engagement_duration:.2f}s")
                    print(f"    🤝 {cohort_name}: Engagement metrics retrieved")
                
                # NEW: Get detailed GLP1 medication metrics
                glp1_metrics_query = get_glp1_medication_metrics_query(cohort_table, cohort_name)
                glp1_metrics_start = time.time()
                cursor.execute(glp1_metrics_query)
                glp1_metrics_results = cursor.fetchall()
                glp1_metrics_duration = time.time() - glp1_metrics_start
                
                if glp1_metrics_results:
                    all_glp1_metrics.extend(glp1_metrics_results)
                    print(f"    ⏱️  GLP1 medication metrics query: {glp1_metrics_duration:.2f}s")
                    print(f"    💊 {cohort_name}: GLP1 medication metrics retrieved")
                
                # Get weight loss users for this cohort
                weight_loss_query = get_weight_loss_users_query(cohort_table, cohort_name)
                weight_loss_start = time.time()
                cursor.execute(weight_loss_query)
                weight_loss_users = cursor.fetchall()
                weight_loss_duration = time.time() - weight_loss_start
                
                if weight_loss_users:
                    all_weight_loss_users.extend(weight_loss_users)
                    print(f"    ⏱️  Weight loss users query: {weight_loss_duration:.2f}s")
                    print(f"    👥 {cohort_name}: {len(weight_loss_users)} users with weight data")
                
                # Get prescription statistics for specified cohorts
                prescription_stats = get_prescription_statistics(cursor, cohort_table, cohort_name)
                
                # Add cohort name to prescription stats and store
                prescription_stats_with_cohort = {
                    'cohort': cohort_name,
                    **prescription_stats
                }
                all_prescription_stats.append(prescription_stats_with_cohort)
                
                # Get validation count
                validation_query = get_weight_loss_count_validation_query(cohort_table, cohort_name)
                cursor.execute(validation_query)
                validation_result = cursor.fetchone()
                
                if validation_result:
                    validation_count = validation_result['weight_loss_pct_n_validation']
                    user_list_count = len(weight_loss_users)

                    # Store validation info with prescription stats
                    validation_results[cohort_name] = {
                        'metrics_weight_loss_pct_n': results[0]['weight_loss_pct_n'] if results else 0,
                        'validation_weight_loss_pct_n': validation_count,
                        'user_list_count': user_list_count,
                        'all_match': (results[0]['weight_loss_pct_n'] if results else 0) == validation_count == user_list_count,
                        'prescription_stats': prescription_stats
                    }
                    
                    # Print validation results
                    if validation_results[cohort_name]['all_match']:
                        print(f"    ✅ VALIDATION PASSED: All counts match ({user_list_count})")
                    else:
                        print(f"    ❌ VALIDATION FAILED:")
                        print(f"       Metrics weight_loss_pct_n: {results[0]['weight_loss_pct_n'] if results else 0}")
                        print(f"       Validation query count: {validation_count}")
                        print(f"       User list count: {user_list_count}")
                    
                    # Print prescription statistics for specified cohorts
                    ps = prescription_stats
                    if cohort_name in ['Apple+Amazon', 'Apple+Amazon Completed All Coaching', 'Apple+Amazon Skipped Optional Coaching',
                                     'Apple+Amazon Excluding Weightloss GLP1', 'Apple+Amazon on Weight GLP1', 
                                     'Apple Excluding Weightloss GLP1', 'Apple on Weight GLP1', 
                                     'Amazon Excluding Weightloss GLP1', 'Amazon on Weight GLP1']:
                        print(f"    💊 PRESCRIPTION STATS:")
                        if ps['total_glp1_users'] > 0:
                            print(f"       Total GLP1 users: {ps['total_glp1_users']}")
                            print(f"       Avg total prescription days: {ps['avg_total_prescription_days']}")
                            print(f"       Avg continuous period days: {ps['avg_total_continuous_days']}")
                            print(f"       Avg gap percentage: {ps['avg_gap_percentage']}%")
                        else:
                            print(f"       No GLP1 prescriptions found for this cohort")
                    
            except Exception as e:
                print(f"    ❌ Error processing {cohort_name}: {e}")
                continue
        
        total_cohort_duration = time.time() - cohort_start_time
        print(f"\n  🎉 All cohorts processed in {total_cohort_duration:.2f}s")
        
        # Print comprehensive validation summary with prescription stats
        print(f"\n🔍 VALIDATION SUMMARY:")
        print(f"{'Cohort':<45} {'Metrics':<8} {'Validation':<10} {'User List':<10} {'Match':<6} {'GLP1 Users':<10} {'Avg Rx Days':<12}")
        print("=" * 105)
        
        all_validations_passed = True
        for cohort_name, validation_data in validation_results.items():
            status = "✅ YES" if validation_data['all_match'] else "❌ NO"
            if not validation_data['all_match']:
                all_validations_passed = False
            
            ps = validation_data.get('prescription_stats', {})
            rx_users = ps.get('total_glp1_users', 0)
            avg_rx_days = ps.get('avg_total_prescription_days', 0)
            
            rx_display = f"{rx_users}" if rx_users > 0 else "-"
            days_display = f"{avg_rx_days}" if avg_rx_days > 0 else "-"
                
            print(f"{cohort_name:<45} {validation_data['metrics_weight_loss_pct_n']:<8} "
                  f"{validation_data['validation_weight_loss_pct_n']:<10} "
                  f"{validation_data['user_list_count']:<10} {status:<6} "
                  f"{rx_display:<10} {days_display}")
        
        print("=" * 105)
        if all_validations_passed:
            print("🎉 ALL VALIDATIONS PASSED - Data integrity confirmed!")
        else:
            print("⚠️  SOME VALIDATIONS FAILED - Please investigate discrepancies!")
        
        # Export results
        if all_results:
            export_start = time.time()
            
            # Export main health metrics
            output_file = 'cohort_health_metrics_v1_cleaned.csv'
            fieldnames = list(all_results[0].keys())
            
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_results)
            
            print(f"\n📄 Export Results:")
            print(f"  ✅ Health metrics exported to {output_file}")
            print(f"  📈 Total health metric rows: {len(all_results)}")
            
            # Export engagement metrics
            if all_engagement_results:
                engagement_file = 'cohort_engagement_metrics_v1_cleaned.csv'
                engagement_fieldnames = list(all_engagement_results[0].keys())
                
                with open(engagement_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=engagement_fieldnames)
                    writer.writeheader()
                    writer.writerows(all_engagement_results)
                
                print(f"  ✅ Engagement metrics exported to {engagement_file}")
                print(f"  🤝 Total engagement metric rows: {len(all_engagement_results)}")
                
                # Print engagement breakdown by cohort
                print(f"\n🤝 Engagement Metrics by Cohort:")
                for result in all_engagement_results:
                    cohort = result['cohort']
                    interactions = result['avg_care_team_interactions_per_month']
                    modules = result['avg_modules_completed']
                    pct_all_modules = result['pct_completed_all_modules']
                    consultations = result['avg_completed_consultations']
                    print(f"  {cohort}:")
                    print(f"    Avg care team interactions/month: {interactions}")
                    print(f"    Avg modules completed: {modules}")
                    print(f"    % completed all modules: {pct_all_modules}%")
                    print(f"    Avg consultations: {consultations}")
            
            # NEW: Export detailed GLP1 medication metrics
            if all_glp1_metrics:
                glp1_metrics_file = 'cohort_glp1_medication_metrics_detailed.csv'
                glp1_metrics_fieldnames = list(all_glp1_metrics[0].keys())
                
                with open(glp1_metrics_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=glp1_metrics_fieldnames)
                    writer.writeheader()
                    writer.writerows(all_glp1_metrics)
                
                print(f"  ✅ GLP1 medication metrics exported to {glp1_metrics_file}")
                print(f"  💊 Total GLP1 medication metric rows: {len(all_glp1_metrics)}")
                
                # Print GLP1 medication breakdown by cohort
                print(f"\n💊 GLP1 Medication Metrics by Cohort:")
                for result in all_glp1_metrics:
                    cohort = result['cohort']
                    total_users = result['total_users']
                    pct_glp1 = result['pct_prescribed_glp1_weight_loss']
                    pct_no_other = result['pct_no_or_other_medication']
                    avg_days = result['avg_days_on_glp1_weight_loss']
                    persistence = result['pct_with_refills_among_glp1_users']  # FIXED: Use correct column name
                    print(f"  {cohort}:")
                    print(f"    Total users: {total_users}")
                    print(f"    % prescribed GLP1 for weight loss: {pct_glp1}%")
                    print(f"    % no medication or other medication: {pct_no_other}%")
                    print(f"    Avg days on GLP1 weight loss: {avg_days}")
                    print(f"    % GLP1 users with refills (persistence): {persistence}%")  # Updated label

            # Export weight loss users by cohort
            if all_weight_loss_users:
                weight_loss_file = 'cohort_weight_loss_users_detailed.csv'
                weight_loss_fieldnames = list(all_weight_loss_users[0].keys())
                
                with open(weight_loss_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=weight_loss_fieldnames)
                    writer.writeheader()
                    writer.writerows(all_weight_loss_users)
                
                print(f"  ✅ Weight loss users exported to {weight_loss_file}")
                print(f"  👥 Total users with weight data: {len(all_weight_loss_users)}")
                
                # Print breakdown by cohort
                cohort_counts = {}
                for user in all_weight_loss_users:
                    cohort = user['cohort']
                    cohort_counts[cohort] = cohort_counts.get(cohort, 0) + 1
                
                print(f"\n📊 Weight Loss Users by Cohort:")
                for cohort_name in cohorts.keys():
                    count = cohort_counts.get(cohort_name, 0)
                    print(f"  {cohort_name}: {count} users")
            
            # Export validation summary with prescription stats
            validation_file = 'cohort_validation_summary.csv'
            validation_data_list = []
            for cohort_name, validation_data in validation_results.items():
                ps = validation_data.get('prescription_stats', {})
                validation_data_list.append({
                    'cohort': cohort_name,
                    'metrics_weight_loss_pct_n': validation_data['metrics_weight_loss_pct_n'],
                    'validation_weight_loss_pct_n': validation_data['validation_weight_loss_pct_n'],
                    'user_list_count': validation_data['user_list_count'],
                    'all_counts_match': validation_data['all_match'],
                    # Add prescription stats to validation summary
                    'total_glp1_users': ps.get('total_glp1_users', 0),
                    'avg_total_prescription_days': ps.get('avg_total_prescription_days', 0),
                    'avg_gap_percentage': ps.get('avg_gap_percentage', 0)
                })
            
            if validation_data_list:
                with open(validation_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=list(validation_data_list[0].keys()))
                    writer.writeheader()
                    writer.writerows(validation_data_list)
                
                print(f"  ✅ Validation summary exported to {validation_file}")
            
            export_duration = time.time() - export_start
            print(f"\n  ⏱️  Total export time: {export_duration:.2f}s")
            
            # --- ADD THIS BLOCK AT THE END OF main() ---
            print("\n📊 Generating engaged 6-month cohort GLP1 metrics...")
            metrics_df = summarize_engaged_6month_metrics(cursor)
            metrics_file = 'engaged_6month_cohort_glp1_metrics.csv'
            metrics_df.to_csv(metrics_file, index=False)
            print(f"  ✅ Engaged 6-month cohort GLP1 metrics exported to {metrics_file}")
            print(metrics_df)

            # --- NEW: Export A1C analysis to Excel ---
            print("\n📊 Exporting A1C analysis to Excel...")
            export_a1c_analysis(cursor)
            # --- END BLOCK ---
            
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        raise
    finally:
        # Cleanup temporary tables only
        cleanup_start = time.time()
        cleanup_tables = [
            'tmp_master_health_metrics',
            'tmp_subscription_starts_all',
            # Health metric tables
            'tmp_user_base_filtered',
            'tmp_cte_baseline_weight', 'tmp_cte_latest_weight',
            'tmp_cte_baseline_bmi', 'tmp_cte_latest_bmi', 
            'tmp_cte_baseline_a1c', 'tmp_cte_latest_a1c',
            'tmp_cte_baseline_bp', 'tmp_cte_latest_bp',
            'tmp_cte_baseline_waist_circ', 'tmp_cte_latest_waist_circ',
            'tmp_cte_baseline_triglycerides', 'tmp_cte_latest_triglycerides',
            'tmp_cte_baseline_hdl', 'tmp_cte_latest_hdl',
            # Engagement tables
            'tmp_avg_care_team_interactions_per_6month_user',
            'tmp_module_completion',
            'tmp_completed_non_orderonly_consultations'
        ]
        
        try:
            for table in cleanup_tables:
                cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {table}")
        except:
            pass
            
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        
        cleanup_duration = time.time() - cleanup_start
        total_script_duration = time.time() - script_start_time
        
        print(f"\n🧹 Cleanup completed in {cleanup_duration:.2f}s")
        print(f"🏁 TOTAL SCRIPT RUNTIME: {total_script_duration:.2f}s")

def get_6month_no_glp1_completed_all_modules(cursor, cohort_table: str) -> pd.DataFrame:
    """
    Returns users in the specified no-GLP1 cohort who are in 6-month retention and completed all 12 modules,
    including their module 12 completion date.
    """
    query = f"""
        SELECT
            ct.user_id,
            mc.completed_all_modules,
            mc.module12_completed_at
        FROM {cohort_table} ct
        INNER JOIN tmp_6_months_retention_users t6mru ON ct.user_id = t6mru.user_id
        LEFT JOIN (
            SELECT
                t.user_id,
                (MAX(CASE WHEN t.`group` = 'module01' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module02' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module03' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module04' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module05' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module06' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module07' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module08' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module09' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module10' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module11' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module12' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END)
                ) = 12 AS completed_all_modules,
                MAX(CASE WHEN t.`group` = 'module12' AND t.status = 'COMPLETED' THEN t.completed_at END) AS module12_completed_at
            FROM tasks t
            WHERE t.program = 'path-to-healthy-weight'
            GROUP BY t.user_id
        ) mc ON ct.user_id = mc.user_id
        WHERE mc.completed_all_modules = 1
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return pd.DataFrame(results)

def get_6month_no_glp1_completed_all_modules_and_post_module12_glp1(cursor, cohort_table: str) -> pd.DataFrame:
    """
    For users in the specified no-GLP1 cohort who are in 6-month retention and completed all 12 modules,
    returns their module 12 completion date, and whether they were prescribed Wegovy/Zepbound after module 12.
    """
    query = f"""
        SELECT
            ct.user_id,
            mc.completed_all_modules,
            mc.module12_completed_at,
            -- Check for GLP1 prescription after module 12 completion
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM prescriptions p
                    JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
                    JOIN medications m ON m.id = ndcs.medication_id
                    WHERE p.patient_user_id = ct.user_id
                      AND (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
                      AND p.prescribed_at >= mc.module12_completed_at
                ) THEN 1 ELSE 0
            END AS prescribed_glp1_after_module12,
            -- Get the first prescribed_at date after module 12 completion (if any)
            (
                SELECT MIN(p.prescribed_at)
                FROM prescriptions p
                JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
                JOIN medications m ON m.id = ndcs.medication_id
                WHERE p.patient_user_id = ct.user_id
                  AND (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
                  AND p.prescribed_at >= mc.module12_completed_at
            ) AS first_glp1_prescribed_at_after_module12
        FROM {cohort_table} ct
        INNER JOIN tmp_6_months_retention_users t6mru ON ct.user_id = t6mru.user_id
        LEFT JOIN (
            SELECT
                t.user_id,
                (MAX(CASE WHEN t.`group` = 'module01' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module02' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module03' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module04' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module05' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module06' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module07' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module08' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module09' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module10' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module11' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END) +
                 MAX(CASE WHEN t.`group` = 'module12' AND t.status = 'COMPLETED' THEN 1 ELSE 0 END)
                ) = 12 AS completed_all_modules,
                MAX(CASE WHEN t.`group` = 'module12' AND t.status = 'COMPLETED' THEN t.completed_at END) AS module12_completed_at
            FROM tasks t
            WHERE t.program = 'path-to-healthy-weight'
            GROUP BY t.user_id
        ) mc ON ct.user_id = mc.user_id
        WHERE mc.completed_all_modules = 1
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return pd.DataFrame(results)

def summarize_engaged_6month_metrics(cursor):
    cohorts = {
        "Apple engaged 6 month cohort": "tmp_apple_no_weightloss_glp1",
        "Amazon engaged 6 month cohort": "tmp_amazon_no_weightloss_glp1",
        "Apple+Amazon engaged 6 month cohort": "tmp_apple_and_amazon_no_weightloss_glp1"
    }
    summary = []
    for cohort_name, cohort_table in cohorts.items():
        df = get_6month_no_glp1_completed_all_modules_and_post_module12_glp1(cursor, cohort_table)
        total_n = len(df)
        on_weight_glp1 = df['prescribed_glp1_after_module12'].sum()
        excluding_weight_loss_glp1 = (df['prescribed_glp1_after_module12'] == 0).sum()
        summary.append({
            "cohort": cohort_name,
            "total_n": total_n,
            "on_weight_glp1": int(on_weight_glp1),
            "excluding_weight_loss_glp1": int(excluding_weight_loss_glp1)
        })
    return pd.DataFrame(summary)

def export_a1c_analysis(cursor, cohort_configs=None):
    """
    Runs A1C analysis queries for baseline A1C >= 6.5, 8.0, and 9.0,
    and exports results to separate sheets in an Excel file.
    Can optionally filter by specific cohorts.
    """
    
    if cohort_configs is None:
        # Default: run for all main cohorts
        cohort_configs = {
            "Apple+Amazon on GLP1": "tmp_apple_and_amazon_on_weight_glp1",
            "Apple on GLP1": "tmp_apple_on_weight_glp1",
            "Amazon on GLP1": "tmp_amazon_on_weight_glp1",
            'Optional Coaching Skipped on GLP1': 'tmp_amazon_on_weight_glp1',  # If you have a specific table for this, update accordingly
            'Completed All Coaching on GLP1': 'tmp_apple_and_amazon_on_weight_glp1'  # If you have a specific table for this, update accordingly
        }
    
    # Collect ALL queries BEFORE writing to Excel
    all_queries = {}
    
    for cohort_name, cohort_table in cohort_configs.items():
        all_queries[f"{cohort_name}_A1C_6_5_plus"] = f"""
            SELECT
                BIN_TO_UUID(mhm.user_id) as user_id,
                mhm.baseline_a1c,
                mhm.latest_a1c,
                mhm.baseline_a1c - mhm.latest_a1c AS a1c_change
            FROM tmp_master_health_metrics mhm
            JOIN {cohort_table} ct ON mhm.user_id = ct.user_id
            WHERE mhm.baseline_a1c >= 6.5
              AND mhm.baseline_a1c IS NOT NULL
              AND mhm.latest_a1c IS NOT NULL
              AND mhm.baseline_a1c - mhm.latest_a1c <= 4.0
        """
        
        all_queries[f"{cohort_name}_A1C_8_plus"] = f"""
            SELECT
                BIN_TO_UUID(mhm.user_id) as user_id,
                mhm.baseline_a1c,
                mhm.latest_a1c,
                mhm.baseline_a1c - mhm.latest_a1c AS a1c_change
            FROM tmp_master_health_metrics mhm
            JOIN {cohort_table} ct ON mhm.user_id = ct.user_id
            WHERE mhm.baseline_a1c >= 8.0
              AND mhm.baseline_a1c IS NOT NULL
              AND mhm.latest_a1c IS NOT NULL
              AND mhm.baseline_a1c - mhm.latest_a1c <= 4.0
        """
        
        all_queries[f"{cohort_name}_A1C_9_plus"] = f"""
            SELECT
                BIN_TO_UUID(mhm.user_id) as user_id,
                mhm.baseline_a1c,
                mhm.latest_a1c,
                mhm.baseline_a1c - mhm.latest_a1c AS a1c_change
            FROM tmp_master_health_metrics mhm
            JOIN {cohort_table} ct ON mhm.user_id = ct.user_id
            WHERE mhm.baseline_a1c >= 9.0
              AND mhm.baseline_a1c IS NOT NULL
              AND mhm.latest_a1c IS NOT NULL
              AND mhm.baseline_a1c - mhm.latest_a1c <= 4.0
        """
    
    # NOW write all queries to Excel
    excel_file = "a1c_analysis_output.xlsx"
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        for sheet_name, query in all_queries.items():
            print(f"  📊 Processing: {sheet_name}")
            cursor.execute(query)
            results = cursor.fetchall()
            df = pd.DataFrame(results)
            # Excel sheet names have 31 char limit
            safe_sheet_name = sheet_name[:31] if len(sheet_name) > 31 else sheet_name
            df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
    
    print(f"  ✅ A1C analysis exported to {excel_file} with {len(all_queries)} sheets")

if __name__ == "__main__":
    main()

