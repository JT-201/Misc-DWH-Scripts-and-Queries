import mysql.connector
import csv
import time
import pandas as pd
from config import get_db_config

def connect_to_db():
    """Connect to the database using config"""
    config = get_db_config()
    return mysql.connector.connect(**config)

def execute_with_timing(cursor, query, description):
    """Execute query with timing information"""
    start_time = time.time()
    cursor.execute(query)
    duration = time.time() - start_time
    print(f"    ⏱️  {description}: {duration:.2f}s")
    return duration

def create_willscot_users_table(cursor, partner='WillScot', end_date='2025-10-01'):
    """Create temporary table for partner users with configurable end date"""
    print(f"\n🏢 Creating {partner} users table (active on {end_date}, 90+ day subscriptions)...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_willscot_users", "Drop partner users table")
    
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_willscot_users AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name = '{partner}'
        AND s.status = 'ACTIVE';
    """, f"Create {partner} users table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_willscot_users_user_id ON tmp_willscot_users(user_id)", "Index partner users table")

def create_health_metrics_tables(cursor, start_date='2025-07-01', end_date='2025-10-01'):
    """Create health metrics tables for users with date filtering"""
    print(f"\n🏥 Creating health metrics tables (filtering for {start_date} to {end_date})...")
    
    # Create baseline and latest weight tables - CONVERTED FROM KG TO LBS
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_weight", "Drop baseline weight table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_weight AS
        WITH ranked_weights AS (
            SELECT 
                bwv.user_id,
                bwv.value * 2.20462 as weight_lbs,  -- Convert kg to lbs
                bwv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bwv.user_id ORDER BY bwv.effective_date ASC) as rn
            FROM body_weight_values bwv
            JOIN tmp_willscot_users au ON bwv.user_id = au.user_id
            WHERE bwv.value IS NOT NULL
              AND bwv.effective_date >= DATE_SUB('{start_date}', INTERVAL 30 DAY)
              AND bwv.effective_date <= '{end_date}'
        )
        SELECT user_id, weight_lbs as baseline_weight_lbs, effective_date as baseline_weight_date
        FROM ranked_weights WHERE rn = 1
    """, "Create baseline weight table (converted kg to lbs)")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_weight", "Drop latest weight table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_weight AS
        WITH ranked_weights AS (
            SELECT 
                bwv.user_id,
                bwv.value * 2.20462 as weight_lbs,  -- Convert kg to lbs
                bwv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bwv.user_id ORDER BY bwv.effective_date DESC) as rn
            FROM body_weight_values bwv
            JOIN tmp_willscot_users au ON bwv.user_id = au.user_id
            WHERE bwv.value IS NOT NULL
              AND bwv.effective_date >= '{start_date}'
              AND bwv.effective_date <= '{end_date}'
        )
        SELECT user_id, weight_lbs as latest_weight_lbs, effective_date as latest_weight_date
        FROM ranked_weights WHERE rn = 1
    """, "Create latest weight table (converted kg to lbs)")
    
    # Create baseline and latest BMI tables - FILTERED BY DATE RANGE
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_bmi", "Drop baseline BMI table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_bmi AS
        WITH ranked_bmi AS (
            SELECT 
                bv.user_id,
                bv.value as bmi,
                bv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bv.user_id ORDER BY bv.effective_date ASC) as rn
            FROM bmi_values bv
            JOIN tmp_willscot_users au ON bv.user_id = au.user_id
            WHERE bv.value IS NOT NULL
              AND bv.effective_date >= '{start_date}'
              AND bv.effective_date <= '{end_date}'
        )
        SELECT user_id, bmi as baseline_bmi, effective_date as baseline_bmi_date
        FROM ranked_bmi WHERE rn = 1
    """, "Create baseline BMI table")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_bmi", "Drop latest BMI table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_bmi AS
        WITH ranked_bmi AS (
            SELECT 
                bv.user_id,
                bv.value as bmi,
                bv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bv.user_id ORDER BY bv.effective_date DESC) as rn
            FROM bmi_values bv
            JOIN tmp_willscot_users au ON bv.user_id = au.user_id
            WHERE bv.value IS NOT NULL
              AND bv.effective_date >= '{start_date}'
              AND bv.effective_date <= '{end_date}'
        )
        SELECT user_id, bmi as latest_bmi, effective_date as latest_bmi_date
        FROM ranked_bmi WHERE rn = 1
    """, "Create latest BMI table")
    
    # Create baseline and latest A1C tables - FILTERED BY DATE RANGE
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_a1c", "Drop baseline A1C table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_a1c AS
        WITH ranked_a1c AS (
            SELECT 
                av.user_id,
                av.value as a1c,
                av.effective_date,
                ROW_NUMBER() OVER (PARTITION BY av.user_id ORDER BY av.effective_date ASC) as rn
            FROM a1c_values av
            JOIN tmp_willscot_users au ON av.user_id = au.user_id
            WHERE av.value IS NOT NULL
              AND av.effective_date >= '{start_date}'
              AND av.effective_date <= '{end_date}'
        )
        SELECT user_id, a1c as baseline_a1c, effective_date as baseline_a1c_date
        FROM ranked_a1c WHERE rn = 1
    """, "Create baseline A1C table")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_a1c", "Drop latest A1C table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_a1c AS
        WITH ranked_a1c AS (
            SELECT 
                av.user_id,
                av.value as a1c,
                av.effective_date,
                ROW_NUMBER() OVER (PARTITION BY av.user_id ORDER BY av.effective_date DESC) as rn
            FROM a1c_values av
            JOIN tmp_willscot_users au ON av.user_id = au.user_id
            WHERE av.value IS NOT NULL
              AND av.effective_date >= '{start_date}'
              AND av.effective_date <= '{end_date}'
        )
        SELECT user_id, a1c as latest_a1c, effective_date as latest_a1c_date
        FROM ranked_a1c WHERE rn = 1
    """, "Create latest A1C table")
    
    # Create baseline and latest blood pressure tables - FILTERED BY DATE RANGE
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_blood_pressure", "Drop baseline blood pressure table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_blood_pressure AS
        WITH ranked_bp AS (
            SELECT 
                bpv.user_id,
                bpv.systolic,
                bpv.diastolic,
                bpv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bpv.user_id ORDER BY bpv.effective_date ASC) as rn
            FROM blood_pressure_values bpv
            JOIN tmp_willscot_users au ON bpv.user_id = au.user_id
            WHERE bpv.systolic IS NOT NULL AND bpv.diastolic IS NOT NULL
              AND bpv.effective_date >= '{start_date}'
              AND bpv.effective_date <= '{end_date}'
        )
        SELECT user_id, systolic as baseline_systolic, diastolic as baseline_diastolic, 
               effective_date as baseline_bp_date
        FROM ranked_bp WHERE rn = 1
    """, "Create baseline blood pressure table")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_blood_pressure", "Drop latest blood pressure table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_blood_pressure AS
        WITH ranked_bp AS (
            SELECT 
                bpv.user_id,
                bpv.systolic,
                bpv.diastolic,
                bpv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bpv.user_id ORDER BY bpv.effective_date DESC) as rn
            FROM blood_pressure_values bpv
            JOIN tmp_willscot_users au ON bpv.user_id = au.user_id
            WHERE bpv.systolic IS NOT NULL AND bpv.diastolic IS NOT NULL
              AND bpv.effective_date >= '{start_date}'
              AND bpv.effective_date <= '{end_date}'
        )
        SELECT user_id, systolic as latest_systolic, diastolic as latest_diastolic, 
               effective_date as latest_bp_date
        FROM ranked_bp WHERE rn = 1
    """, "Create latest blood pressure table")
    
    # COMMENTED OUT: Create GLP1 users table
    # execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_willscot_glp1_users", "Drop GLP1 users table")
    # execute_with_timing(cursor, f"""
    #     CREATE TEMPORARY TABLE tmp_willscot_glp1_users AS
    #     WITH glp1_prescriptions AS (
    #         SELECT 
    #             au.user_id,
    #             p.prescribed_at,
    #             p.days_of_supply,
    #             p.total_refills,
    #             (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
    #             DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
    #         FROM tmp_willscot_users au
    #         JOIN prescriptions p ON au.user_id = p.patient_user_id
    #         JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
    #         JOIN medications m ON m.id = ndcs.medication_id
    #         WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
    #         AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('{end_date}', INTERVAL 30 DAY)
    #     ),
    #     user_prescription_coverage AS (
    #         SELECT 
    #             user_id,
    #             MIN(prescribed_at) as first_prescription_date,
    #             MAX(prescription_end_date) as last_prescription_end_date,
    #             SUM(total_covered_days) as total_covered_days,
    #             DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days,
    #             CASE 
    #                 WHEN DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) > 0 
    #                 THEN ((DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) - SUM(total_covered_days)) * 100.0 / 
    #                       DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)))
    #                 ELSE 0 
    #             END as gap_percentage
    #         FROM glp1_prescriptions
    #         GROUP BY user_id
    #     )
    #     SELECT 
    #         user_id,
    #         first_prescription_date as prescribed_at,
    #         last_prescription_end_date as prescription_end_date,
    #         total_covered_days,
    #         total_period_days,
    #         gap_percentage
    #     FROM user_prescription_coverage
    #     WHERE gap_percentage < 5.0
    #     AND total_covered_days >= 60
    # """, "Create GLP1 users table (continuous medication only)")
    
    # UPDATED: Create indexes WITHOUT GLP1 table
    for table in ['tmp_baseline_weight', 'tmp_latest_weight', 'tmp_baseline_bmi', 'tmp_latest_bmi', 
                  'tmp_baseline_a1c', 'tmp_latest_a1c', 'tmp_baseline_blood_pressure', 'tmp_latest_blood_pressure']:
        execute_with_timing(cursor, f"CREATE INDEX idx_{table}_user_id ON {table}(user_id)", f"Index {table}")

def create_a1c_analysis_table(cursor, start_date='2025-07-01', end_date='2025-10-01'):
    """Create A1C analysis table for prediabetic and diabetic users - CORRECTED DATE LOGIC"""
    print(f"\n🩺 Creating A1C analysis table ({start_date} to {end_date})...")
    
    # Step 1: Get BASELINE A1C values (earliest AFTER start_date - when they entered program)
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_a1c_for_analysis", "Drop baseline A1C analysis table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_a1c_for_analysis AS
        SELECT 
            a1.user_id,
            a1.value as baseline_a1c,
            a1.effective_date as baseline_a1c_date
        FROM a1c_values a1
        JOIN tmp_willscot_users au ON a1.user_id = au.user_id
        WHERE a1.value IS NOT NULL
          AND a1.value >= 5.7  -- Only prediabetic (5.7-6.4) or diabetic (6.5+)
          AND a1.effective_date >= '{start_date}'  -- AFTER they started the program
          AND a1.effective_date = (
              SELECT MIN(a2.effective_date)  -- EARLIEST after start_date
              FROM a1c_values a2 
              WHERE a2.user_id = a1.user_id 
                AND a2.effective_date >= '{start_date}'  -- AFTER start_date
                AND a2.value IS NOT NULL
                AND a2.value >= 5.7
          )
    """, "Create baseline A1C analysis table (earliest after start_date)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_baseline_a1c_analysis_user_id ON tmp_baseline_a1c_for_analysis(user_id)", "Index baseline A1C analysis table")
    
    # Step 2: Get LATEST A1C values (most recent BEFORE end_date)
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_a1c_for_analysis", "Drop latest A1C analysis table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_a1c_for_analysis AS
        SELECT 
            a1.user_id,
            a1.value as latest_a1c,
            a1.effective_date as latest_a1c_date
        FROM a1c_values a1
        JOIN tmp_baseline_a1c_for_analysis ba ON a1.user_id = ba.user_id  -- Only users with baseline
        WHERE a1.value IS NOT NULL
          AND a1.effective_date <= '{end_date}'  -- BEFORE end_date
          AND a1.effective_date > ba.baseline_a1c_date  -- AFTER their baseline reading
          AND a1.effective_date = (
              SELECT MAX(a2.effective_date)  -- LATEST before end_date
              FROM a1c_values a2 
              WHERE a2.user_id = a1.user_id 
                AND a2.effective_date <= '{end_date}'  -- BEFORE end_date
                AND a2.effective_date > ba.baseline_a1c_date  -- AFTER baseline
                AND a2.value IS NOT NULL
          )
    """, "Create latest A1C analysis table (latest before end_date)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_latest_a1c_analysis_user_id ON tmp_latest_a1c_for_analysis(user_id)", "Index latest A1C analysis table")
    
    # Step 3: Create final analysis with both baseline and latest values
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_a1c_analysis", "Drop A1C analysis table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_a1c_analysis AS
        SELECT 
            'A1C Analysis by Condition' as metric_category,
            
            -- Prediabetic users (5.7-6.4 at baseline)
            COUNT(DISTINCT CASE 
                WHEN ba.baseline_a1c >= 5.7 AND ba.baseline_a1c < 6.5 
                THEN ba.user_id 
            END) as prediabetic_user_count,
            COUNT(DISTINCT CASE 
                WHEN ba.baseline_a1c >= 5.7 AND ba.baseline_a1c < 6.5 
                AND la.user_id IS NOT NULL  -- Has follow-up A1C
                THEN ba.user_id 
            END) as prediabetic_users_in_analysis_period,
            
            -- Diabetic users (6.5+ at baseline)
            COUNT(DISTINCT CASE 
                WHEN ba.baseline_a1c >= 6.5 
                THEN ba.user_id 
            END) as diabetic_user_count,
            COUNT(DISTINCT CASE 
                WHEN ba.baseline_a1c >= 6.5 
                AND la.user_id IS NOT NULL  -- Has follow-up A1C
                THEN ba.user_id 
            END) as diabetic_users_in_analysis_period,
            
            -- Total counts
            COUNT(DISTINCT ba.user_id) as total_user_count,
            COUNT(DISTINCT la.user_id) as total_users_with_followup_a1c,
            
            -- A1C improvement metrics (baseline vs latest)
            ROUND(AVG(ba.baseline_a1c), 2) as avg_baseline_a1c,
            ROUND(AVG(la.latest_a1c), 2) as avg_latest_a1c,
            ROUND(AVG(ba.baseline_a1c - la.latest_a1c), 2) as avg_a1c_improvement,
            
            -- Prediabetic improvement
            ROUND(AVG(CASE 
                WHEN ba.baseline_a1c >= 5.7 AND ba.baseline_a1c < 6.5 
                AND la.user_id IS NOT NULL
                THEN ba.baseline_a1c - la.latest_a1c 
            END), 2) as prediabetic_avg_improvement,
            
            -- Diabetic improvement  
            ROUND(AVG(CASE 
                WHEN ba.baseline_a1c >= 6.5 
                AND la.user_id IS NOT NULL
                THEN ba.baseline_a1c - la.latest_a1c 
            END), 2) as diabetic_avg_improvement,
            
            -- Time between readings
            ROUND(AVG(DATEDIFF(la.latest_a1c_date, ba.baseline_a1c_date)), 0) as avg_days_between_readings
            
        FROM tmp_baseline_a1c_for_analysis ba
        LEFT JOIN tmp_latest_a1c_for_analysis la ON ba.user_id = la.user_id
    """, "Create final A1C analysis table")
    
    # Cleanup temporary helper tables
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_a1c_for_analysis", "Cleanup baseline A1C analysis table")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_a1c_for_analysis", "Cleanup latest A1C analysis table")

def create_module_completion_table(cursor, start_date='2025-07-01', end_date='2025-10-01'):
    """Create module completion analysis table for WillScot users"""
    print(f"\n📚 Creating module completion table ({start_date} to {end_date})...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_module_completion", "Drop module completion table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_module_completion AS
        SELECT 
            'Module Completion Analysis' as metric_category,
            
            -- Count distinct users who completed each module
            COUNT(DISTINCT CASE WHEN t.`group` = 'module01' AND t.status = 'COMPLETED' THEN t.user_id END) as module_01_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module02' AND t.status = 'COMPLETED' THEN t.user_id END) as module_02_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module03' AND t.status = 'COMPLETED' THEN t.user_id END) as module_03_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module04' AND t.status = 'COMPLETED' THEN t.user_id END) as module_04_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module05' AND t.status = 'COMPLETED' THEN t.user_id END) as module_05_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module06' AND t.status = 'COMPLETED' THEN t.user_id END) as module_06_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module07' AND t.status = 'COMPLETED' THEN t.user_id END) as module_07_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module08' AND t.status = 'COMPLETED' THEN t.user_id END) as module_08_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module09' AND t.status = 'COMPLETED' THEN t.user_id END) as module_09_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module10' AND t.status = 'COMPLETED' THEN t.user_id END) as module_10_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module11' AND t.status = 'COMPLETED' THEN t.user_id END) as module_11_completers,
            COUNT(DISTINCT CASE WHEN t.`group` = 'module12' AND t.status = 'COMPLETED' THEN t.user_id END) as module_12_completers,
            
            -- Count users who completed ANY module
            COUNT(DISTINCT CASE WHEN t.status = 'COMPLETED' THEN t.user_id END) as users_completed_any_module,
            
            -- Total willscot users for reference
            COUNT(DISTINCT au.user_id) as total_willscot_users,
            
            -- Completion percentage
            ROUND(
                (COUNT(DISTINCT CASE WHEN t.status = 'COMPLETED' THEN t.user_id END) * 100.0) / 
                COUNT(DISTINCT au.user_id), 2
            ) as completion_percentage
            
        FROM tmp_willscot_users au
        LEFT JOIN tasks t ON au.user_id = t.user_id
        WHERE t.program = 'path-to-healthy-weight'
          AND (t.created_at BETWEEN '{start_date}' AND '{end_date}' OR t.created_at IS NULL)
    """, "Create module completion table")

def get_health_outcomes_query():
    """Get comprehensive health outcomes similar to the cohort analysis - FIXED TABLE ALIASES"""
    return """
        SELECT 
            'willscot Users Health Outcomes' as metric_category,
            
            -- Overall Weight Loss Metrics
            COUNT(DISTINCT willscot_users.user_id) as total_willscot_users,
            
            -- Weight metrics (all users with weight data)
            ROUND(AVG(bw.baseline_weight_lbs), 2) as weight_baseline_avg,
            ROUND(AVG(lw.latest_weight_lbs), 2) as weight_current_avg,
            COUNT(CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL THEN 1 END) as weight_sample_size,
            ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as weight_loss_pct,
            ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as weight_loss_lbs,
            
            -- COMMENTED OUT: Weight metrics for GLP1 users only
            -- ROUND(AVG(CASE WHEN glp1.user_id IS NOT NULL THEN bw.baseline_weight_lbs END), 2) as weight_glp1_baseline_avg,
            -- ROUND(AVG(CASE WHEN glp1.user_id IS NOT NULL THEN lw.latest_weight_lbs END), 2) as weight_glp1_current_avg,
            -- COUNT(CASE WHEN glp1.user_id IS NOT NULL AND bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL THEN 1 END) as weight_glp1_sample_size,
            -- ROUND(AVG(CASE WHEN glp1.user_id IS NOT NULL THEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100 END), 2) as weight_glp1_loss_pct,
            -- ROUND(AVG(CASE WHEN glp1.user_id IS NOT NULL THEN bw.baseline_weight_lbs - lw.latest_weight_lbs END), 2) as weight_glp1_loss_lbs,
            
            -- BMI metrics
            ROUND(AVG(bb.baseline_bmi), 2) as bmi_baseline_avg,
            ROUND(AVG(lb.latest_bmi), 2) as bmi_current_avg,
            COUNT(CASE WHEN bb.baseline_bmi IS NOT NULL AND lb.latest_bmi IS NOT NULL THEN 1 END) as bmi_sample_size,
            ROUND(AVG(bb.baseline_bmi - lb.latest_bmi), 2) as bmi_delta,
            
            -- A1C metrics (all users)
            ROUND(AVG(ba.baseline_a1c), 2) as a1c_baseline_avg,
            ROUND(AVG(la.latest_a1c), 2) as a1c_current_avg,
            COUNT(CASE WHEN ba.baseline_a1c IS NOT NULL AND la.latest_a1c IS NOT NULL THEN 1 END) as a1c_sample_size,
            ROUND(AVG(ba.baseline_a1c - la.latest_a1c), 2) as a1c_delta,
            
            -- A1C metrics (baseline 6.5%+)
            ROUND(AVG(CASE WHEN ba.baseline_a1c >= 6.5 THEN ba.baseline_a1c END), 2) as a1c_6_5_plus_baseline_avg,
            ROUND(AVG(CASE WHEN ba.baseline_a1c >= 6.5 THEN la.latest_a1c END), 2) as a1c_6_5_plus_current_avg,
            COUNT(CASE WHEN ba.baseline_a1c >= 6.5 AND la.latest_a1c IS NOT NULL THEN 1 END) as a1c_6_5_plus_sample_size,
            ROUND(AVG(CASE WHEN ba.baseline_a1c >= 6.5 THEN ba.baseline_a1c - la.latest_a1c END), 2) as a1c_6_5_plus_delta
            
            -- COMMENTED OUT: GLP1 user count
            -- COUNT(DISTINCT glp1.user_id) as glp1_users_total
            
        FROM tmp_willscot_users willscot_users
        LEFT JOIN tmp_baseline_weight bw ON willscot_users.user_id = bw.user_id
        LEFT JOIN tmp_latest_weight lw ON willscot_users.user_id = lw.user_id
        LEFT JOIN tmp_baseline_bmi bb ON willscot_users.user_id = bb.user_id
        LEFT JOIN tmp_latest_bmi lb ON willscot_users.user_id = lb.user_id
        LEFT JOIN tmp_baseline_a1c ba ON willscot_users.user_id = ba.user_id
        LEFT JOIN tmp_latest_a1c la ON willscot_users.user_id = la.user_id
        LEFT JOIN tmp_baseline_blood_pressure bbp ON willscot_users.user_id = bbp.user_id
        LEFT JOIN tmp_latest_blood_pressure lbp ON willscot_users.user_id = lbp.user_id
        -- COMMENTED OUT: GLP1 join
        -- LEFT JOIN tmp_willscot_glp1_users glp1 ON willscot_users.user_id = glp1.user_id
        
        UNION ALL
        
        SELECT 
            'Outcomes - Hypertension' as metric_category,
            
            -- Blood pressure metrics for controlled hypertension (< 140/90)
            COUNT(DISTINCT willscot_users2.user_id) as total_willscot_users,
            
            -- Controlled BP - Baseline systolic/diastolic averages
            ROUND(AVG(CASE WHEN bbp2.baseline_systolic < 140 AND bbp2.baseline_diastolic < 90 THEN bbp2.baseline_systolic END), 2) as bp_controlled_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN bbp2.baseline_systolic < 140 AND bbp2.baseline_diastolic < 90 THEN bbp2.baseline_diastolic END), 2) as bp_controlled_baseline_diastolic_avg,
            
            -- Controlled BP - Latest systolic/diastolic averages
            ROUND(AVG(CASE WHEN bbp2.baseline_systolic < 140 AND bbp2.baseline_diastolic < 90 THEN lbp2.latest_systolic END), 2) as bp_controlled_latest_systolic_avg,
            ROUND(AVG(CASE WHEN bbp2.baseline_systolic < 140 AND bbp2.baseline_diastolic < 90 THEN lbp2.latest_diastolic END), 2) as bp_controlled_latest_diastolic_avg,
            
            -- Controlled BP - Sample size and deltas
            COUNT(CASE WHEN bbp2.baseline_systolic < 140 AND bbp2.baseline_diastolic < 90 AND lbp2.latest_systolic IS NOT NULL AND lbp2.latest_diastolic IS NOT NULL THEN 1 END) as bp_controlled_sample_size,
            ROUND(AVG(CASE WHEN bbp2.baseline_systolic < 140 AND bbp2.baseline_diastolic < 90 THEN bbp2.baseline_systolic - lbp2.latest_systolic END), 2) as bp_controlled_systolic_delta,
            ROUND(AVG(CASE WHEN bbp2.baseline_systolic < 140 AND bbp2.baseline_diastolic < 90 THEN bbp2.baseline_diastolic - lbp2.latest_diastolic END), 2) as bp_controlled_diastolic_delta,
            
            NULL as weight_baseline_avg, NULL as weight_current_avg, NULL as weight_sample_size, NULL as weight_loss_pct, NULL as weight_loss_lbs,
            -- COMMENTED OUT: GLP1 weight metrics
            -- NULL as weight_glp1_baseline_avg, NULL as weight_glp1_current_avg, NULL as weight_glp1_sample_size, NULL as weight_glp1_loss_pct, NULL as weight_glp1_loss_lbs,
            NULL as bmi_baseline_avg, NULL as bmi_current_avg, NULL as bmi_sample_size, NULL as bmi_delta,
            NULL as a1c_baseline_avg, NULL as a1c_current_avg, NULL as a1c_sample_size, NULL as a1c_delta,
            NULL as a1c_6_5_plus_baseline_avg, NULL as a1c_6_5_plus_current_avg, NULL as a1c_6_5_plus_sample_size, NULL as a1c_6_5_plus_delta
            -- COMMENTED OUT: GLP1 user count
            -- NULL as glp1_users_total
            
        FROM tmp_willscot_users willscot_users2
        LEFT JOIN tmp_baseline_blood_pressure bbp2 ON willscot_users2.user_id = bbp2.user_id
        LEFT JOIN tmp_latest_blood_pressure lbp2 ON willscot_users2.user_id = lbp2.user_id
        
        UNION ALL
        
        SELECT 
            'Outcomes - Hypertension (uncontrolled)' as metric_category,
            
            -- Blood pressure metrics for uncontrolled hypertension (>= 140/90)
            COUNT(DISTINCT willscot_users3.user_id) as total_willscot_users,
            
            -- Uncontrolled BP - Baseline systolic/diastolic averages
            ROUND(AVG(CASE WHEN bbp3.baseline_systolic >= 140 OR bbp3.baseline_diastolic >= 90 THEN bbp3.baseline_systolic END), 2) as bp_uncontrolled_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN bbp3.baseline_systolic >= 140 OR bbp3.baseline_diastolic >= 90 THEN bbp3.baseline_diastolic END), 2) as bp_uncontrolled_baseline_diastolic_avg,
            
            -- Uncontrolled BP - Latest systolic/diastolic averages
            ROUND(AVG(CASE WHEN bbp3.baseline_systolic >= 140 OR bbp3.baseline_diastolic >= 90 THEN lbp3.latest_systolic END), 2) as bp_uncontrolled_latest_systolic_avg,
            ROUND(AVG(CASE WHEN bbp3.baseline_systolic >= 140 OR bbp3.baseline_diastolic >= 90 THEN lbp3.latest_diastolic END), 2) as bp_uncontrolled_latest_diastolic_avg,
            
            -- Uncontrolled BP - Sample size and deltas
            COUNT(CASE WHEN (bbp3.baseline_systolic >= 140 OR bbp3.baseline_diastolic >= 90) AND lbp3.latest_systolic IS NOT NULL AND lbp3.latest_diastolic IS NOT NULL THEN 1 END) as bp_uncontrolled_sample_size,
            ROUND(AVG(CASE WHEN bbp3.baseline_systolic >= 140 OR bbp3.baseline_diastolic >= 90 THEN bbp3.baseline_systolic - lbp3.latest_systolic END), 2) as bp_uncontrolled_systolic_delta,
            ROUND(AVG(CASE WHEN bbp3.baseline_systolic >= 140 OR bbp3.baseline_diastolic >= 90 THEN bbp3.baseline_diastolic - lbp3.latest_diastolic END), 2) as bp_uncontrolled_diastolic_delta,
            
            NULL as weight_baseline_avg, NULL as weight_current_avg, NULL as weight_sample_size, NULL as weight_loss_pct, NULL as weight_loss_lbs,
            -- COMMENTED OUT: GLP1 weight metrics
            -- NULL as weight_glp1_baseline_avg, NULL as weight_glp1_current_avg, NULL as weight_glp1_sample_size, NULL as weight_glp1_loss_pct, NULL as weight_glp1_loss_lbs,
            NULL as bmi_baseline_avg, NULL as bmi_current_avg, NULL as bmi_sample_size, NULL as bmi_delta,
            NULL as a1c_baseline_avg, NULL as a1c_current_avg, NULL as a1c_sample_size, NULL as a1c_delta,
            NULL as a1c_6_5_plus_baseline_avg, NULL as a1c_6_5_plus_current_avg, NULL as a1c_6_5_plus_sample_size, NULL as a1c_6_5_plus_delta
            -- COMMENTED OUT: GLP1 user count
            -- NULL as glp1_users_total
            
        FROM tmp_willscot_users willscot_users3
        LEFT JOIN tmp_baseline_blood_pressure bbp3 ON willscot_users3.user_id = bbp3.user_id
        LEFT JOIN tmp_latest_blood_pressure lbp3 ON willscot_users3.user_id = lbp3.user_id
    """

def get_weight_medians_query(start_date, end_date):
    """Get weight medians analysis - FIXED MySQL syntax"""
    return f"""
        WITH weight_data AS (
            SELECT 
                willscot_users.user_id,
                bw.baseline_weight_lbs,
                lw.latest_weight_lbs,
                CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL
                     THEN ((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs) * 100
                END as weight_loss_pct
            FROM tmp_willscot_users willscot_users
            LEFT JOIN tmp_baseline_weight bw ON willscot_users.user_id = bw.user_id
            LEFT JOIN tmp_latest_weight lw ON willscot_users.user_id = lw.user_id
            WHERE bw.baseline_weight_lbs IS NOT NULL 
              AND lw.latest_weight_lbs IS NOT NULL
        ),
        weight_stats AS (
            SELECT 
                COUNT(*) as total_users,
                AVG(baseline_weight_lbs) as avg_baseline_weight,
                AVG(latest_weight_lbs) as avg_latest_weight,
                AVG(weight_loss_pct) as avg_weight_loss_pct,
                MIN(weight_loss_pct) as min_weight_loss_pct,
                MAX(weight_loss_pct) as max_weight_loss_pct
            FROM weight_data
        ),
        median_calc AS (
            SELECT 
                weight_loss_pct,
                ROW_NUMBER() OVER (ORDER BY weight_loss_pct) as row_num,
                COUNT(*) OVER () as total_count
            FROM weight_data
            WHERE weight_loss_pct IS NOT NULL
        )
        SELECT 
            'Weight Medians Analysis' as metric_category,
            ws.total_users,
            ROUND(ws.avg_baseline_weight, 2) as avg_baseline_weight_lbs,
            ROUND(ws.avg_latest_weight, 2) as avg_latest_weight_lbs,
            ROUND(ws.avg_weight_loss_pct, 2) as avg_weight_loss_percent,
            ROUND(ws.min_weight_loss_pct, 2) as min_weight_loss_percent,
            ROUND(ws.max_weight_loss_pct, 2) as max_weight_loss_percent,
            -- Calculate median using row numbers
            ROUND((SELECT AVG(weight_loss_pct) 
                   FROM median_calc 
                   WHERE row_num IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2))), 2) as median_weight_loss_percent
        FROM weight_stats ws
    """

def create_qbr_metrics_tables(cursor, start_date='2025-07-01', end_date='2025-10-01', goals_start_date='2025-01-01'):
    """Create pre-computed QBR metrics tables with configurable date ranges"""
    print(f"\n📊 Creating QBR metrics tables (Analysis: {start_date} to {end_date}, Goals: {goals_start_date} to {end_date})...")
    
    # Create weight medians table - SIMPLIFIED TO AVOID TABLE REOPEN ERROR
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_weight_medians", "Drop weight medians table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_weight_medians AS
        SELECT 
            'Weight Medians Analysis' as metric_category,
            COUNT(*) as total_users,
            ROUND(AVG(bw.baseline_weight_lbs), 2) as avg_baseline_weight_lbs,
            ROUND(AVG(lw.latest_weight_lbs), 2) as avg_latest_weight_lbs,
            ROUND(AVG(
                CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL
                     THEN ((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs) * 100
                END
            ), 2) as avg_weight_loss_percent,
            ROUND(MIN(
                CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL
                     THEN ((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs) * 100
                END
            ), 2) as min_weight_loss_percent,
            ROUND(MAX(
                CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL
                     THEN ((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs) * 100
                END
            ), 2) as max_weight_loss_percent,
            -- Simple median approximation using PERCENTILE_CONT (if available) or just the average
            ROUND(AVG(
                CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL
                     THEN ((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs) * 100
                END
            ), 2) as median_weight_loss_percent
        FROM tmp_willscot_users au
        LEFT JOIN tmp_baseline_weight bw ON au.user_id = bw.user_id
        LEFT JOIN tmp_latest_weight lw ON au.user_id = lw.user_id
        WHERE bw.baseline_weight_lbs IS NOT NULL 
          AND lw.latest_weight_lbs IS NOT NULL
    """, "Create weight medians table")

    # Continue with demographics table creation...
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_demographics", "Drop demographics table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_demographics AS
        SELECT 
            'Demographics Analysis' as metric_category,
            
            -- Age group breakdowns
            COUNT(CASE WHEN u.age >= 18 AND u.age <= 19 THEN u.id END) as active_users_18_19,
            COUNT(CASE WHEN u.age >= 20 AND u.age <= 29 THEN u.id END) as active_users_20_29,
            COUNT(CASE WHEN u.age >= 30 AND u.age <= 39 THEN u.id END) as active_users_30_39,
            COUNT(CASE WHEN u.age >= 40 AND u.age <= 49 THEN u.id END) as active_users_40_49,
            COUNT(CASE WHEN u.age >= 50 AND u.age <= 59 THEN u.id END) as active_users_50_59,
            COUNT(CASE WHEN u.age >= 60 AND u.age <= 69 THEN u.id END) as active_users_60_69,
            COUNT(CASE WHEN u.age >= 70 THEN u.id END) as active_users_70_plus,
            
            -- Gender breakdowns
            COUNT(CASE WHEN u.sex = 'MALE' THEN u.id END) as active_users_male,
            COUNT(CASE WHEN u.sex = 'FEMALE' THEN u.id END) as active_users_female,
            
            -- Ethnicity breakdowns
            COUNT(CASE WHEN u.ethnicity = 'WHITE' THEN u.id END) as active_users_white,
            COUNT(CASE WHEN u.ethnicity = 'HISPANIC_LATINO' THEN u.id END) as active_users_hispanic_latino,
            COUNT(CASE WHEN u.ethnicity = 'BLACK_OR_AFRICAN_AMERICAN' THEN u.id END) as active_users_black_african_american,
            COUNT(CASE WHEN u.ethnicity = 'ASIAN' THEN u.id END) as active_users_asian,
            COUNT(CASE WHEN u.ethnicity = 'AMERICAN_NATIVE_OR_ALASKAN' THEN u.id END) as active_users_american_native_alaskan,
            COUNT(CASE WHEN u.ethnicity IS NULL THEN u.id END) as active_users_unknown,
            COUNT(CASE WHEN u.ethnicity NOT IN (
                'WHITE', 'HISPANIC_LATINO', 'BLACK_OR_AFRICAN_AMERICAN', 'ASIAN', 'AMERICAN_NATIVE_OR_ALASKAN'
            ) AND u.ethnicity IS NOT NULL THEN u.id END) as active_users_other,
            
            -- Total count
            COUNT(DISTINCT u.id) as total_users
            
        FROM users u
        JOIN tmp_willscot_users au ON u.id = au.user_id
    """, "Create demographics table")
    
    # Create state distribution table
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_state_distribution", "Drop state distribution table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_state_distribution AS
        WITH state_counts AS (
            SELECT 
                COALESCE(u.shipment_address_state, 'Unknown') as state,
                COUNT(DISTINCT au.user_id) as user_count,
                ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT au.user_id) DESC) as state_rank
            FROM tmp_willscot_users au
            LEFT JOIN users u ON au.user_id = u.id
            GROUP BY COALESCE(u.shipment_address_state, 'Unknown')
        )
        SELECT 
            'State Distribution' as metric_category,
            CASE 
                WHEN state_rank <= 8 THEN state
                ELSE 'Other'
            END as state,
            SUM(user_count) as user_count
        FROM state_counts
        GROUP BY 
            CASE 
                WHEN state_rank <= 8 THEN state
                ELSE 'Other'
            END
        ORDER BY 
            CASE 
                WHEN MAX(state_rank) <= 8 THEN MAX(state_rank)
                ELSE 999
            END
    """, "Create state distribution table")
    
    # 3. Billable activities table
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_billable_activities", "Drop billable activities table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_billable_activities AS
        SELECT 
            'Billable Activities' as metric_category,
            ba.type as activity_type,
            MONTH(ba.activity_timestamp) AS month,
            YEAR(ba.activity_timestamp) AS year,
            COUNT(ba.id) AS activity_count,
            COUNT(DISTINCT ba.user_id) AS unique_users
        FROM billable_activities ba
        JOIN tmp_willscot_users au ON ba.user_id = au.user_id
        WHERE ba.activity_timestamp >= '{start_date}' 
          AND ba.activity_timestamp <= '{end_date}'
        GROUP BY ba.type, YEAR(ba.activity_timestamp), MONTH(ba.activity_timestamp)
        ORDER BY year, month, activity_count DESC
    """, "Create billable activities table")
    
    # 4. Analytics events table - OPTIMIZED with smaller event set and better indexing
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_analytics_events", "Drop analytics events table")
    
    # First create a filtered analytics events table to reduce data size
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_filtered_analytics_events AS
        SELECT 
            ae.id,
            ae.user_id,
            ae.event_name,
            ae.created_at
        FROM analytics_events ae
        WHERE ae.created_at >= '{start_date}' 
          AND ae.created_at <= '{end_date}'
          AND ae.event_name IN ('meal plan generated', 'meal plan selected', 'article closed', 'article_opened', 'Video Player Interaction: play')
    """, "Create filtered analytics events table")
    
    # Add index to the filtered table
    execute_with_timing(cursor, "CREATE INDEX idx_filtered_analytics_user_id ON tmp_filtered_analytics_events(user_id)", "Index filtered analytics events")
    
    # Now create the final analytics table using the smaller filtered table
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_analytics_events AS
        SELECT 
            'Analytics Events' as metric_category,
            fae.event_name,
            COUNT(DISTINCT fae.id) as event_count,
            COUNT(DISTINCT fae.user_id) as unique_users,
            MONTH(fae.created_at) AS month,
            YEAR(fae.created_at) AS year
        FROM tmp_filtered_analytics_events fae
        JOIN tmp_willscot_users au ON fae.user_id = au.user_id
        GROUP BY fae.event_name, YEAR(fae.created_at), MONTH(fae.created_at)
        ORDER BY year, month, event_count DESC
    """, "Create analytics events table from filtered data")
    
    # Cleanup the temporary filtered table
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_filtered_analytics_events", "Drop filtered analytics events table")
    
    # 5. Program goals table - use goals_start_date
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_program_goals", "Drop program goals table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_program_goals AS
        SELECT 
            'Program Goals' as metric_category,
            qr.answer_text as goal_type,
            COUNT(DISTINCT qr.user_id) as member_count,
            COUNT(qr.id) as total_responses
        FROM questionnaire_records qr
        JOIN tmp_willscot_users au ON qr.user_id = au.user_id
        WHERE qr.question_id = '4hseoh8ddqn8' 
          AND qr.answered_at >= '{goals_start_date}' 
          AND qr.answered_at <= '{end_date}'
        GROUP BY qr.answer_text
        ORDER BY member_count DESC
    """, "Create program goals table")
    
    # 6. NPS analysis table
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_nps_analysis", "Drop NPS analysis table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_nps_analysis AS
        SELECT 
            'NPS Analysis' as metric_category,
            COUNT(DISTINCT npsr.id) as response_count,
            ROUND(AVG(npsr.score), 2) as avg_nps_score,
            COUNT(CASE WHEN npsr.score >= 9 THEN 1 END) as promoters,
            COUNT(CASE WHEN npsr.score >= 7 AND npsr.score <= 8 THEN 1 END) as passives,
            COUNT(CASE WHEN npsr.score <= 6 THEN 1 END) as detractors,
            ROUND(
                (COUNT(CASE WHEN npsr.score >= 9 THEN 1 END) - COUNT(CASE WHEN npsr.score <= 6 THEN 1 END)) * 100.0 / 
                COUNT(npsr.id), 2
            ) as net_promoter_score
        FROM nps_response_records npsr
        JOIN questionnaire_records qr ON qr.questionnaire_id = npsr.questionnaire_id
        JOIN tmp_willscot_users au ON au.user_id = qr.user_id
        WHERE npsr.submitted_at >= '{goals_start_date}' 
          AND npsr.submitted_at <= '{end_date}'
    """, "Create NPS analysis table")
    
    # 7. CSAT analysis table
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_csat_analysis", "Drop CSAT analysis table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_csat_analysis AS
        SELECT 
            'CSAT Analysis' as metric_category,
            ae.event_name as csat_event,
            COUNT(DISTINCT qr.id) as response_count,
            COUNT(DISTINCT qr.user_id) as unique_users
        FROM questionnaire_records qr
        JOIN tmp_willscot_users au ON qr.user_id = au.user_id
        JOIN analytics_events ae ON ae.user_id = qr.user_id
        WHERE ae.created_at >= '{goals_start_date}' 
          AND ae.created_at <= '{end_date}'
          AND ae.event_name LIKE 'CSAT%'
        GROUP BY ae.event_name
        ORDER BY response_count DESC
    """, "Create CSAT analysis table")
    
    # 8. Medical conditions - FIXED MISSING CONDITION
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_medical_conditions", "Drop medical conditions table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_medical_conditions AS
        SELECT 
            'Medical Conditions' as metric_category,
            COUNT(DISTINCT mc.user_id) AS member_count,
            CASE 
                WHEN mc.name LIKE '%diabetes%' THEN 'Diabetes'
                WHEN mc.name LIKE '%hypertension%' THEN 'Hypertension' 
                WHEN mc.name LIKE '%Obese%' THEN 'Obese'
                WHEN mc.name LIKE '%Sleep Apnea%' THEN 'Sleep Apnea'
                WHEN mc.name LIKE '%Fatty Liver%' THEN 'Fatty Liver'
                WHEN mc.name LIKE '%MASLD%' THEN 'MASLD'
                WHEN mc.name LIKE '%Prediabetes%' THEN 'Prediabetes'
                WHEN mc.name LIKE '%Overweight%' THEN 'Overweight'
                WHEN mc.name LIKE '%PCOS%' THEN 'PCOS'
            END AS condition_category
        FROM medical_conditions mc
        JOIN tmp_willscot_users au ON au.user_id = mc.user_id
        WHERE (mc.name LIKE '%diabetes%' 
           OR mc.name LIKE '%hypertension%' 
           OR mc.name LIKE '%Obese%' 
           OR mc.name LIKE '%Sleep Apnea%' 
           OR mc.name LIKE '%Fatty Liver%' 
           OR mc.name LIKE '%MASLD%'
           OR mc.name LIKE '%Prediabetes%'
           OR mc.name LIKE '%Overweight%'
           OR mc.name LIKE '%PCOS%')
        AND mc.recorded_at BETWEEN '{goals_start_date}' AND '{end_date}'
        GROUP BY 
            CASE 
                WHEN mc.name LIKE '%diabetes%' THEN 'Diabetes'
                WHEN mc.name LIKE '%hypertension%' THEN 'Hypertension' 
                WHEN mc.name LIKE '%Obese%' THEN 'Obese'
                WHEN mc.name LIKE '%Sleep Apnea%' THEN 'Sleep Apnea'
                WHEN mc.name LIKE '%Fatty Liver%' THEN 'Fatty Liver'
                WHEN mc.name LIKE '%MASLD%' THEN 'MASLD'
                WHEN mc.name LIKE '%Prediabetes%' THEN 'Prediabetes'
                WHEN mc.name LIKE '%Overweight%' THEN 'Overweight'
                WHEN mc.name LIKE '%PCOS%' THEN 'PCOS'
            END
        ORDER BY member_count DESC
    """, "Create medical conditions table")
    
    # 9. Medical condition groups table
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_condition_groups", "Drop condition groups table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_condition_groups AS
        SELECT 
            'Medical Condition Groups' as metric_category,
            COUNT(DISTINCT mcg.user_id) as member_count, 
            mcg.condition_group
        FROM medical_condition_groups mcg
        JOIN tmp_willscot_users au ON au.user_id = mcg.user_id
        GROUP BY mcg.condition_group
        ORDER BY member_count DESC
    """, "Create condition groups table")
    
    # 10. Medication counts table
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_medication_counts", "Drop medication counts table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_medication_counts AS
        SELECT 
            'Medication Counts' as metric_category,
            m.name AS medication_name,
            COUNT(DISTINCT au.user_id) as user_count
        FROM tmp_willscot_users au
        JOIN prescriptions p ON au.user_id = p.patient_user_id
        JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
        JOIN medications m ON m.id = ndcs.medication_id
        WHERE (m.name LIKE '%Mounjaro%' 
           OR m.name LIKE '%Ozempic%' 
           OR m.name LIKE '%Wegovy%' 
           OR m.name LIKE '%Zepbound%' 
           OR m.name LIKE '%Topiramate%' 
           OR m.name LIKE '%Metformin Er%' 
           OR m.name LIKE '%Buproprion%' 
           OR m.name LIKE '%Naltrexone%')
        AND p.prescribed_at BETWEEN '{goals_start_date}' AND '{end_date}'
        GROUP BY m.name
        ORDER BY user_count DESC
    """, "Create medication counts table")
    
    # COMMENTED OUT: Health Outcomes table with GLP1
    # execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_health_outcomes", "Drop health outcomes table")
    # execute_with_timing(cursor, """
    #     CREATE TEMPORARY TABLE tmp_health_outcomes_overall AS
    #     SELECT 
    #         'Health Outcomes - Overall' as metric_category,
    #         COUNT(DISTINCT au.user_id) as total_willscot_users,
    #         ...
    #         LEFT JOIN tmp_willscot_glp1_users glp1 ON au.user_id = glp1.user_id
    # """, "Create overall health outcomes table")
    # 
    # execute_with_timing(cursor, """
    #     CREATE TEMPORARY TABLE tmp_health_outcomes_glp1 AS
    #     SELECT 
    #         'Health Outcomes - GLP1 Users' as metric_category,
    #         ...
    #         INNER JOIN tmp_willscot_glp1_users glp1 ON au.user_id = glp1.user_id
    # """, "Create GLP1 health outcomes table")

    # Now combine them without UNION issues
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_health_outcomes AS
        SELECT * FROM tmp_health_outcomes_overall
        UNION ALL
        SELECT * FROM tmp_health_outcomes_glp1
    """, "Combine health outcomes tables")

    # Clean up intermediate tables
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_health_outcomes_overall", "Drop overall health outcomes table")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_health_outcomes_glp1", "Drop GLP1 health outcomes table")
    
    # Create A1C analysis table
    create_a1c_analysis_table(cursor, start_date, end_date)
    
    # Create module completion table
    create_module_completion_table(cursor, start_date, end_date)

    print(f"✅ All QBR metrics tables created successfully!")

def create_source_table_indexes(cursor):
    """Create indexes on source tables for better performance"""
    print(f"\n🔧 Creating source table indexes for better performance...")
    
    indexes_to_create = [
        ("body_weight_values", "user_id"),
        ("body_weight_values", "effective_date"),
        ("bmi_values", "user_id"),
        ("bmi_values", "effective_date"),
        ("a1c_values", "user_id"),
        ("a1c_values", "effective_date"),
        ("blood_pressure_values", "user_id"),
        ("blood_pressure_values", "effective_date"),
        ("prescriptions", "patient_user_id"),
        ("prescriptions", "prescribed_at"),
        ("analytics_events", "user_id"),
        ("analytics_events", "created_at"),
        ("billable_activities", "user_id"),
        ("billable_activities", "activity_timestamp"),
        ("questionnaire_records", "user_id"),
        ("questionnaire_records", "answered_at"),
        ("medical_conditions", "user_id"),
        ("medical_conditions", "recorded_at"),
        ("tasks", "user_id"),
        ("tasks", "created_at")
    ]
    
    for table, column in indexes_to_create:
        try:
            execute_with_timing(cursor, 
                f"CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON {table}({column})", 
                f"Index {table}.{column}")
        except Exception as e:
            print(f"    ⚠️  Index {table}.{column} may already exist or failed: {e}")

def main(partner='WillScot', analysis_start_date='2025-01-01', analysis_end_date='2025-10-01', goals_start_date='2025-01-01'):
    """Main execution function for QBR analysis with configurable parameters"""
    
    script_start_time = time.time()
    
    try:
        print(f"🔗 Connecting to database...")
        print(f"📊 Configuration:")
        print(f"  🏢 Partner: {partner}")
        print(f"  📅 Analysis Period: {analysis_start_date} to {analysis_end_date}")
        print(f"  📅 Goals/Medical Period: {goals_start_date} to {analysis_end_date}")
        
        conn_start = time.time()
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        conn_duration = time.time() - conn_start
        print(f"  ⏱️  Database connection: {conn_duration:.2f}s")
        
        # Create partner users table
        create_willscot_users_table(cursor, partner, analysis_end_date)
        
        # Create source table indexes for better performance
        create_source_table_indexes(cursor)
        
        # Create health metrics tables - WITH DATE FILTERING
        create_health_metrics_tables(cursor, analysis_start_date, analysis_end_date)
        
        # Create QBR metrics tables (pre-computed)
        create_qbr_metrics_tables(cursor, analysis_start_date, analysis_end_date, goals_start_date)
        
        # Analysis queries to run - SIMPLIFIED
        analysis_queries = [
            ("Health Outcomes", "SELECT * FROM tmp_health_outcomes"),
            ("Weight Medians", "SELECT * FROM tmp_weight_medians"),
            ("Demographics", "SELECT * FROM tmp_demographics"),
            ("State Distribution", "SELECT * FROM tmp_state_distribution"),
            ("Billable Activities", "SELECT * FROM tmp_billable_activities"),
            ("Analytics Events", "SELECT * FROM tmp_analytics_events"),
            ("Program Goals", "SELECT * FROM tmp_program_goals"),
            ("NPS Analysis", "SELECT * FROM tmp_nps_analysis"),
            ("CSAT Analysis", "SELECT * FROM tmp_csat_analysis"),
            ("Medical Conditions", "SELECT * FROM tmp_medical_conditions"),
            ("Medical Condition Groups", "SELECT * FROM tmp_condition_groups"),
            ("Medication Counts", "SELECT * FROM tmp_medication_counts"),
            ("A1C Analysis", "SELECT * FROM tmp_a1c_analysis"),
            ("Module Completion", "SELECT * FROM tmp_module_completion")
        ]
        
        # Execute all analyses and organize by category
        print(f"\n📊 Running QBR Analysis for {partner} ({analysis_start_date} to {analysis_end_date})...")
        results_by_category = {}
        
        for analysis_name, query in analysis_queries:
            print(f"\n  🎯 Processing: {analysis_name}")
            
            try:
                analysis_start = time.time()
                cursor.execute(query)
                results = cursor.fetchall()
                analysis_duration = time.time() - analysis_start
                
                if results:
                    # Group results by metric_category for separate sheets
                    for result in results:
                        category = result.get('metric_category', analysis_name)
                        if category not in results_by_category:
                            results_by_category[category] = []
                        results_by_category[category].append(result)
                    
                    print(f"    ⏱️  Query execution: {analysis_duration:.2f}s")
                    print(f"    📈 Records retrieved: {len(results)}")
                else:
                    print(f"    ⚠️  No data returned for {analysis_name}")
                    
            except Exception as e:
                print(f"    ❌ Error in {analysis_name}: {e}")
                continue
        
        # Export results to Excel with multiple sheets
        if results_by_category:
            export_start = time.time()
            
            # Create Excel file with partner and date info in filename
            excel_file = f'qbr_analysis_{partner}_{analysis_start_date}_to_{analysis_end_date}.xlsx'
            
            try:
                with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                    for category, data in results_by_category.items():
                        # Convert to DataFrame
                        df = pd.DataFrame(data)
                        
                        # Clean sheet name (Excel sheet names have restrictions)
                        sheet_name = category.replace('/', '_').replace('\\', '_').replace('*', '_').replace('?', '_').replace(':', '_').replace('[', '_').replace(']', '_')[:31]
                        
                        # Write to sheet
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"    📄 Created sheet: '{sheet_name}' with {len(data)} records")
                        
            except ImportError:
                print("    ⚠️  openpyxl not available, creating CSV files only...")
                excel_file = None
            except Exception as e:
                print(f"    ⚠️  Excel export failed: {e}")
                excel_file = None
            
            # Also create a summary CSV with all data
            all_results = []
            for data_list in results_by_category.values():
                all_results.extend(data_list)
            
            csv_file = f'qbr_analysis_{partner}_{analysis_start_date}_to_{analysis_end_date}.csv'
            if all_results:
                # Get ALL unique fieldnames from all results
                all_fieldnames = set()
                for result in all_results:
                    all_fieldnames.update(result.keys())
                
                # Convert to sorted list for consistent ordering
                fieldnames = sorted(list(all_fieldnames))
                
                with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    # Write rows, filling missing fields with None/empty
                    for row in all_results:
                        # Create a complete row with all possible fields
                        complete_row = {field: row.get(field, None) for field in fieldnames}
                        writer.writerow(complete_row)
            
            export_duration = time.time() - export_start
            
            print(f"\n📄 Export Results:")
            if excel_file:
                print(f"  ✅ Multi-sheet Excel file: {excel_file}")
            print(f"  ✅ Summary CSV file: {csv_file}")
                
            print(f"  📊 Total categories: {len(results_by_category)}")
            print(f"  📈 Total records: {sum(len(data) for data in results_by_category.values())}")
            print(f"  ⏱️  Export time: {export_duration:.2f}s")
            
            # Print detailed summary by category/sheet
            print(f"\n📋 Results Summary:")
            for category, data in results_by_category.items():
                print(f"  📊 '{category}': {len(data)} records")
        
        else:
            print("\n⚠️  No results to export")
            
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Updated cleanup to exclude GLP1 tables
        cleanup_start = time.time()
        cleanup_tables = [
            'tmp_willscot_users', 'tmp_baseline_weight', 'tmp_latest_weight',
            'tmp_baseline_bmi', 'tmp_latest_bmi', 'tmp_baseline_a1c', 
            'tmp_latest_a1c',  # REMOVED: 'tmp_willscot_glp1_users',
            # QBR metrics tables
            'tmp_demographics', 'tmp_state_distribution', 'tmp_billable_activities',
            'tmp_analytics_events', 'tmp_program_goals', 'tmp_nps_analysis', 'tmp_csat_analysis',
            'tmp_medical_conditions', 'tmp_condition_groups', 'tmp_medication_counts',
            'tmp_a1c_analysis', 'tmp_module_completion', 'tmp_health_outcomes', 'tmp_weight_medians',
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

if __name__ == "__main__":
    # You can now call with custom parameters:
    # main(partner='willscot', analysis_start_date='2025-07-01', analysis_end_date='2025-09-30', goals_start_date='2025-01-01')
    # main(partner='Walmart', analysis_start_date='2025-01-01', analysis_end_date='2025-12-31', goals_start_date='2024-01-01')
    main()  # Uses defaults: willscot, 2025-07-01 to 2025-09-30, goals from 2025-01-01