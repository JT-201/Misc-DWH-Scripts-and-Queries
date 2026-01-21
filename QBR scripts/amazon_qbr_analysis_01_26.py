import mysql.connector
import csv
import time
import pandas as pd
import os
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

def create_amazon_user_tables(cursor, end_date='2025-12-31'):
    """Create Amazon user tables"""
    print(f"\n📦 Creating Amazon user tables (as of {end_date})...")
    
    # All users
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_all", "Drop Amazon all users table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_amazon_users_all AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers pe ON pe.user_id = s.user_id
        WHERE pe.name = 'Amazon'
          AND s.status = 'ACTIVE'
          AND s.start_date <= '{end_date}'
    """, "Create Amazon all users table")
    
    # Create index
    execute_with_timing(cursor, "CREATE INDEX idx_tmp_amazon_users_all_user_id ON tmp_amazon_users_all(user_id)", "Index Amazon users table")

def create_amazon_users_6month_retention_table(cursor, end_date='2025-12-31'):
    """Create temporary table for 6-month retention users (for health metrics only)"""
    print(f"\n🏥 Creating Amazon 6-month retention users table (for health metrics only)...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_6month", "Drop 6-month retention users table")
    
    # Step 1: Get base Amazon users
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_amazon_users_base_6month AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers pe ON pe.user_id = s.user_id
        WHERE pe.name = 'Amazon'
        AND s.status = 'ACTIVE'
    """, f"Create base Amazon users for 6-month retention")
    
    # Step 2: Apply 6-month retention logic (from Apple script)
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_amazon_users_6month AS
        WITH user_subscription_days AS (
            SELECT 
                au.user_id, 
                au.start_date,
                SUM(CASE WHEN bus.subscription_status = 'ACTIVE' THEN 1 ELSE 0 END) as days_with_active_subscription
            FROM tmp_amazon_users_base_6month au
            JOIN billable_user_statuses bus ON au.user_id = bus.user_id
            WHERE bus.partner = 'Universal'
            GROUP BY au.user_id, au.start_date
        ),
        six_months_retention_users AS (
            SELECT 
                us.user_id, 
                us.start_date,
                us.days_with_active_subscription
            FROM user_subscription_days us
            WHERE us.days_with_active_subscription >= 180
            AND EXISTS (SELECT 1 FROM billable_activities ba WHERE ba.user_id = us.user_id)
        ),
        user_activity_summary AS (
            SELECT 
                smru.user_id,
                smru.start_date,
                smru.days_with_active_subscription,
                COUNT(DISTINCT DATE_FORMAT(ba.activity_timestamp, '%Y-%m')) as months_with_activity,
                MIN(DATE(ba.activity_timestamp)) as first_activity_date,
                MAX(DATE(ba.activity_timestamp)) as last_activity_date,
                DATEDIFF(MAX(DATE(ba.activity_timestamp)), MIN(DATE(ba.activity_timestamp))) as activity_span_days
            FROM six_months_retention_users smru
            JOIN billable_activities ba ON smru.user_id = ba.user_id
            WHERE ba.activity_timestamp IS NOT NULL
            GROUP BY smru.user_id, smru.start_date, smru.days_with_active_subscription
        ),
        user_monthly_activity_check AS (
            SELECT 
                user_id,
                start_date,
                days_with_active_subscription,
                months_with_activity,
                first_activity_date,
                last_activity_date
            FROM user_activity_summary
            WHERE activity_span_days > 150  -- At least ~5 months span
            AND months_with_activity >= 6    -- Must have activity in at least 6 different months
        )
        SELECT 
            user_id,
            start_date,
            days_with_active_subscription,
            months_with_activity,
            first_activity_date,
            last_activity_date
        FROM user_monthly_activity_check
    """, f"Create Amazon 6-month retention users table")
    
    # Clean up intermediate table
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_base_6month", "Drop base 6-month users table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_amazon_users_6month_user_id ON tmp_amazon_users_6month(user_id)", "Index 6-month retention users table")
    
    # Print retention statistics comparison
    print(f"  📊 Calculating user statistics...")
    
    # Get all users count
    cursor.execute("SELECT COUNT(*) as all_users FROM tmp_amazon_users_all")
    all_count = cursor.fetchone()[0]  # Access first element of tuple
    
    # Get 6-month retention users count
    cursor.execute("SELECT COUNT(*) as retained_users FROM tmp_amazon_users_6month")
    retained_count = cursor.fetchone()[0]  # Access first element of tuple
    
    print(f"  📊 All Amazon users: {all_count}")
    print(f"  📊 6-month retention users: {retained_count}")
    
    # Avoid division by zero
    if all_count > 0:
        retention_rate = (retained_count / all_count * 100)
        print(f"  📊 Retention rate: {retention_rate:.1f}%")
    else:
        print(f"  📊 Retention rate: 0.0%")

def create_amazon_glp1_tables(cursor, end_date='2025-12-31', coverage_gap_days=1):
    """Create GLP1 user tables for Amazon users"""
    print(f"\n💊 Creating Amazon GLP1 user tables (coverage through {end_date} ± {coverage_gap_days} days)...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_glp1_users_all", "Drop Amazon GLP1 users table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_amazon_glp1_users_all AS
        WITH glp1_prescriptions AS (
            SELECT 
                au.user_id,
                p.prescribed_at,
                p.days_of_supply,
                p.total_refills,
                (p.days_of_supply + p.days_of_supply * COALESCE(p.total_refills, 0)) as total_prescription_days,
                DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * COALESCE(p.total_refills, 0)) DAY) as prescription_end_date
            FROM tmp_amazon_users_6month au  -- 6-MONTH RETENTION USERS
            JOIN prescriptions p ON au.user_id = p.patient_user_id
            JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
            JOIN medications m ON m.id = ndcs.medication_id
            WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
            AND p.prescribed_at <= '{end_date}'  -- Only include prescriptions that start before end_date
        ),
        user_prescription_coverage AS (
            SELECT 
                user_id,
                MIN(prescribed_at) as first_prescription_date,
                MAX(prescription_end_date) as last_prescription_end_date,
                SUM(total_prescription_days) as total_covered_days,
                DATEDIFF(MAX(prescription_end_date), MIN(prescribed_at)) as total_period_days,
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
        WHERE gap_percentage <= 20.0  -- More lenient than cohort script's 5%
        AND total_covered_days >= 90   -- 90 days vs 60 days in cohort script
        AND DATE_ADD(last_prescription_end_date, INTERVAL {coverage_gap_days} DAY) >= DATE_SUB('{end_date}', INTERVAL 90 DAY)  -- Coverage extends to end_date ± gap
    """, f"Create Amazon GLP1 users table (coverage through {end_date} ± {coverage_gap_days} days)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_amazon_glp1_all_user_id ON tmp_amazon_glp1_users_all(user_id)", "Index Amazon GLP1 table")

def create_weight_metrics_tables(cursor, end_date='2025-12-31'):
    """Create weight metrics tables for Amazon users using 6-month retention users for health metrics"""
    print(f"\n⚖️ Creating weight metrics tables (6-month retention users for health metrics)...")
    
    # Use 6-month retention users for health metrics
    user_table = 'tmp_amazon_users_6month'
    
    # Baseline weights from questionnaire records
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_weight_all", "Drop baseline weight table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_weight_all AS
        WITH ranked_weights AS (
            SELECT 
                bwv.user_id,
                bwv.value * 2.20462 as weight_lbs,
                bwv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bwv.user_id ORDER BY bwv.effective_date ASC) as rn
            FROM body_weight_values bwv
            JOIN {user_table} au ON bwv.user_id = au.user_id
            WHERE bwv.value IS NOT NULL
              AND bwv.effective_date >= DATE_SUB(au.start_date, INTERVAL 30 DAY)
              AND bwv.effective_date <= '{end_date}'
        )
        SELECT user_id, weight_lbs as baseline_weight_lbs, effective_date as baseline_weight_date
        FROM ranked_weights WHERE rn = 1
    """, "Create baseline weight table")

    # Latest weights from body_weight_values
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_weight_all", "Drop latest weight table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_weight_all AS
        WITH ranked_weights AS (
            SELECT 
                bwv.user_id,
                bwv.value * 2.20462 as weight_lbs,
                bwv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bwv.user_id ORDER BY bwv.effective_date DESC) as rn
            FROM body_weight_values_cleaned bwv
            JOIN {user_table} au ON bwv.user_id = au.user_id
            JOIN tmp_baseline_weight_all bbw ON bwv.user_id = bbw.user_id
            WHERE bwv.value IS NOT NULL
              AND bwv.effective_date >= au.start_date
              AND bwv.effective_date <= '{end_date}'
              AND bwv.effective_date >= DATE_ADD(bbw.baseline_weight_date, INTERVAL 30 DAY)
        )
        SELECT user_id, weight_lbs as latest_weight_lbs, effective_date as latest_weight_date
        FROM ranked_weights WHERE rn = 1
    """, "Create latest weight table")
    
    # Create indexes
    execute_with_timing(cursor, "CREATE INDEX idx_baseline_weight_all_user_id ON tmp_baseline_weight_all(user_id)", "Index baseline weight table")
    execute_with_timing(cursor, "CREATE INDEX idx_latest_weight_all_user_id ON tmp_latest_weight_all(user_id)", "Index latest weight table")

def create_blood_pressure_tables(cursor, end_date='2025-12-31'):
    """Create blood pressure tables for Amazon users"""
    print(f"\n🩺 Creating blood pressure tables...")
    
    # Baseline blood pressure
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_blood_pressure_all", "Drop baseline BP table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_blood_pressure_all AS
        WITH ranked_bp AS (
            SELECT 
                bpv.user_id,
                bpv.systolic,
                bpv.diastolic,
                bpv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bpv.user_id ORDER BY bpv.effective_date ASC) as rn
            FROM blood_pressure_values bpv
            JOIN tmp_amazon_users_all au ON bpv.user_id = au.user_id
            WHERE bpv.systolic IS NOT NULL AND bpv.diastolic IS NOT NULL
              AND bpv.effective_date >= au.start_date
              AND bpv.effective_date <= '{end_date}'
        )
        SELECT user_id, systolic as baseline_systolic, diastolic as baseline_diastolic, 
               effective_date as baseline_bp_date
        FROM ranked_bp WHERE rn = 1
    """, "Create baseline BP table")
    
    # Latest blood pressure
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_blood_pressure_all", "Drop latest BP table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_blood_pressure_all AS
        WITH ranked_bp AS (
            SELECT 
                bpv.user_id,
                bpv.systolic,
                bpv.diastolic,
                bpv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bpv.user_id ORDER BY bpv.effective_date DESC) as rn
            FROM blood_pressure_values bpv
            JOIN tmp_amazon_users_all au ON bpv.user_id = au.user_id
            JOIN tmp_baseline_blood_pressure_all bbbp ON bpv.user_id = bbbp.user_id
            WHERE bpv.systolic IS NOT NULL AND bpv.diastolic IS NOT NULL
              AND bpv.effective_date >= au.start_date
              AND bpv.effective_date <= '{end_date}'
              AND bpv.effective_date >= DATE_ADD(bbbp.baseline_bp_date, INTERVAL 30 DAY)
        )
        SELECT user_id, systolic as latest_systolic, diastolic as latest_diastolic, 
               effective_date as latest_bp_date
        FROM ranked_bp WHERE rn = 1
    """, "Create latest BP table")
    
    # Create indexes
    execute_with_timing(cursor, "CREATE INDEX idx_baseline_bp_all_user_id ON tmp_baseline_blood_pressure_all(user_id)", "Index baseline BP table")
    execute_with_timing(cursor, "CREATE INDEX idx_latest_bp_all_user_id ON tmp_latest_blood_pressure_all(user_id)", "Index latest BP table")

def create_a1c_metrics_tables(cursor, end_date='2025-12-31'):
    """Create A1C metrics tables for Amazon users"""
    print(f"\n🩺 Creating A1C metrics tables...")
    
    # Baseline A1C values
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_a1c_all", "Drop baseline A1C table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_a1c_all AS
        WITH ranked_a1c AS (
            SELECT 
                av.user_id,
                av.value as a1c,
                av.effective_date,
                ROW_NUMBER() OVER (PARTITION BY av.user_id ORDER BY av.effective_date ASC) as rn
            FROM a1c_values av
            JOIN tmp_amazon_users_all au ON av.user_id = au.user_id
            WHERE av.value IS NOT NULL
              AND av.value >= 5.7  -- Only prediabetic (5.7-6.4) or diabetic (6.5+)
              AND av.effective_date >= au.start_date
              AND av.effective_date <= '{end_date}'
        )
        SELECT user_id, a1c as baseline_a1c, effective_date as baseline_a1c_date
        FROM ranked_a1c WHERE rn = 1
    """, "Create baseline A1C table")
    
    # Latest A1C values
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_a1c_all", "Drop latest A1C table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_a1c_all AS
        WITH ranked_a1c AS (
            SELECT 
                av.user_id,
                av.value as a1c,
                av.effective_date,
                ROW_NUMBER() OVER (PARTITION BY av.user_id ORDER BY av.effective_date DESC) as rn
            FROM a1c_values av
            JOIN tmp_amazon_users_all au ON av.user_id = au.user_id
            JOIN tmp_baseline_a1c_all bba1c ON av.user_id = bba1c.user_id
            WHERE av.value IS NOT NULL
              AND av.effective_date >= au.start_date
              AND av.effective_date <= '{end_date}'
              AND av.effective_date >= DATE_ADD(bba1c.baseline_a1c_date, INTERVAL 30 DAY)
        )
        SELECT user_id, a1c as latest_a1c, effective_date as latest_a1c_date
        FROM ranked_a1c WHERE rn = 1
    """, "Create latest A1C table")
    
    # Create indexes
    execute_with_timing(cursor, "CREATE INDEX idx_baseline_a1c_all_user_id ON tmp_baseline_a1c_all(user_id)", "Index baseline A1C table")
    execute_with_timing(cursor, "CREATE INDEX idx_latest_a1c_all_user_id ON tmp_latest_a1c_all(user_id)", "Index latest A1C table")

def create_amazon_no_glp1_tables(cursor):
    """Create tables for Amazon users without GLP1 prescriptions"""
    print(f"\n🚫 Creating Amazon no GLP1 user tables...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_no_glp1_users_all", "Drop Amazon no GLP1 table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_amazon_no_glp1_users_all AS
        SELECT au.user_id
        FROM tmp_amazon_users_all au
        LEFT JOIN (
            SELECT DISTINCT p.patient_user_id AS user_id
            FROM prescriptions p
            JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
            JOIN medications m ON m.id = ndcs.medication_id
            WHERE m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%'
        ) glp1_any ON au.user_id = glp1_any.user_id
        WHERE glp1_any.user_id IS NULL
    """, "Create Amazon no GLP1 table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_amazon_no_glp1_all_user_id ON tmp_amazon_no_glp1_users_all(user_id)", "Index Amazon no GLP1 table")

def create_amazon_members_mapping(cursor):
    """Create mapping table between user_id and amazon_members data"""
    print(f"\n🏢 Creating Amazon members mapping table...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_members_mapping", "Drop Amazon members mapping table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_amazon_members_mapping AS
        SELECT DISTINCT 
            s.user_id,
            am.unique_id,
            am.job_category
        FROM subscriptions s 
        JOIN partner_eligibility_checks pec ON s.user_id = pec.user_id
        JOIN partner_eligibility_specific_user_data pesud1 ON pesud1.eligibility_check_id = pec.id 
            AND pesud1.key = 'uniqueId'
        JOIN amazon_members am ON am.unique_id = pesud1.value
        WHERE s.status = 'ACTIVE'
    """, "Create Amazon members mapping table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_amazon_members_mapping_user_id ON tmp_amazon_members_mapping(user_id)", "Index Amazon members mapping table")
    execute_with_timing(cursor, "CREATE INDEX idx_amazon_members_mapping_job_category ON tmp_amazon_members_mapping(job_category)", "Index Amazon members mapping job category")
    
    # Print job category statistics
    print(f"  📊 Amazon members job category breakdown:")
    cursor.execute("""
        SELECT 
            COALESCE(job_category, 'Unknown') as job_category,
            COUNT(*) as user_count
        FROM tmp_amazon_members_mapping
        GROUP BY job_category
        ORDER BY user_count DESC
    """)
    
    job_stats = cursor.fetchall()
    for job_cat, count in job_stats:
        print(f"    • {job_cat}: {count} users")

def create_hypertension_analysis(cursor):
    """Create hypertension-focused analysis with FIXED Corporate/Ops breakdowns"""
    print(f"\n🫀 Creating hypertension analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_hypertension_analysis", "Drop hypertension analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_hypertension_analysis (
            metric_category VARCHAR(255),
            time_period VARCHAR(50),
            user_group VARCHAR(100),
            total_users_with_data INT,
            uncontrolled_baseline_users INT,
            users_with_significant_bp_drop INT,
            percent_with_significant_bp_drop DECIMAL(10,2),
            avg_systolic_improvement DECIMAL(10,1),
            avg_diastolic_improvement DECIMAL(10,1),
            users_normalized_bp INT,
            percent_normalized_bp DECIMAL(10,2)
        )
    """, "Create hypertension analysis table structure")
    
    # Define hypertension user groups (FIXED JOINS)
    hypertension_groups = [
        ('All Hypertensive Users', ''),
        ('Corporate Hypertensive', "JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id WHERE amm.job_category = 'Corporate'"),
        ('Ops Hypertensive', "JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id WHERE amm.job_category = 'Ops'"),
        ('Hypertensive GLP1 Users', 'JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id'),
        ('Corporate Hypertensive GLP1', """JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                                          JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                                          WHERE amm.job_category = 'Corporate'"""),
        ('Ops Hypertensive GLP1', """JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                                     JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                                     WHERE amm.job_category = 'Ops'"""),
        ('Hypertensive No GLP1', 'LEFT JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id WHERE glp.user_id IS NULL'),
        ('Corporate Hypertensive No GLP1', """LEFT JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                                              JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                                              WHERE glp.user_id IS NULL AND amm.job_category = 'Corporate'"""),
        ('Ops Hypertensive No GLP1', """LEFT JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                                        JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                                        WHERE glp.user_id IS NULL AND amm.job_category = 'Ops'""")
    ]
    
    # Generate queries for all hypertension groups
    for group_name, join_where_clause in hypertension_groups:
        hypertension_query = f"""
            INSERT INTO tmp_hypertension_analysis
            SELECT 
                'Hypertension Management' as metric_category,
                'Uncontrolled BP Users' as time_period,
                '{group_name}' as user_group,
                COUNT(DISTINCT bbb.user_id) as total_users_with_data,
                COUNT(DISTINCT bbb.user_id) as uncontrolled_baseline_users,
                COUNT(DISTINCT CASE WHEN ((bbb.baseline_systolic - lbb.latest_systolic) >= 10 OR (bbb.baseline_diastolic - lbb.latest_diastolic) >= 5) THEN bbb.user_id END) as users_with_significant_bp_drop,
                ROUND((COUNT(DISTINCT CASE WHEN ((bbb.baseline_systolic - lbb.latest_systolic) >= 10 OR (bbb.baseline_diastolic - lbb.latest_diastolic) >= 5) THEN bbb.user_id END) * 100.0 / COUNT(DISTINCT bbb.user_id)), 2) as percent_with_significant_bp_drop,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic), 1) as avg_systolic_improvement,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic), 1) as avg_diastolic_improvement,
                COUNT(DISTINCT CASE WHEN (lbb.latest_systolic < 140 AND lbb.latest_diastolic < 90) THEN bbb.user_id END) as users_normalized_bp,
                ROUND((COUNT(DISTINCT CASE WHEN (lbb.latest_systolic < 140 AND lbb.latest_diastolic < 90) THEN bbb.user_id END) * 100.0 / COUNT(DISTINCT bbb.user_id)), 2) as percent_normalized_bp
            FROM tmp_baseline_blood_pressure_all bbb
            JOIN tmp_latest_blood_pressure_all lbb ON bbb.user_id = lbb.user_id
            {join_where_clause}
            AND (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)
        """
        
        execute_with_timing(cursor, hypertension_query, f"Insert {group_name} hypertension analysis")

def create_weight_loss_analysis(cursor):
    """Create comprehensive weight loss analysis with Corporate/Ops breakdowns"""
    print(f"\n📊 Creating weight loss analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_weight_loss_analysis", "Drop weight loss analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_weight_loss_analysis (
            metric_category VARCHAR(255),
            time_period VARCHAR(50),
            user_group VARCHAR(100),
            total_users_with_data INT,
            avg_weight_loss_lbs DECIMAL(10,2),
            avg_percent_weight_loss DECIMAL(10,2),
            users_5_percent_loss INT,
            users_10_percent_loss INT,
            percent_achieving_5_percent DECIMAL(10,2),
            percent_achieving_10_percent DECIMAL(10,2)
        )
    """, "Create weight loss analysis table structure")
    
    # Define all user groups to analyze (FIXED JOINS)
    user_groups = [
        ('All Users', ''),
        ('Corporate', "JOIN tmp_amazon_members_mapping amm ON bw.user_id = amm.user_id WHERE amm.job_category = 'Corporate'"),
        ('Ops', "JOIN tmp_amazon_members_mapping amm ON bw.user_id = amm.user_id WHERE amm.job_category = 'Ops'"),
        ('GLP1 Users', 'JOIN tmp_amazon_glp1_users_all glp ON bw.user_id = glp.user_id'),
        ('Corporate GLP1 Users', """JOIN tmp_amazon_glp1_users_all glp ON bw.user_id = glp.user_id 
                                   JOIN tmp_amazon_members_mapping amm ON bw.user_id = amm.user_id 
                                   WHERE amm.job_category = 'Corporate'"""),
        ('Ops GLP1 Users', """JOIN tmp_amazon_glp1_users_all glp ON bw.user_id = glp.user_id 
                              JOIN tmp_amazon_members_mapping amm ON bw.user_id = amm.user_id 
                              WHERE amm.job_category = 'Ops'"""),
        ('No GLP1 Users', 'JOIN tmp_amazon_no_glp1_users_all noglp ON bw.user_id = noglp.user_id WHERE (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs <= 0.21'),
        ('Corporate No GLP1 Users', """JOIN tmp_amazon_no_glp1_users_all noglp ON bw.user_id = noglp.user_id 
                                      JOIN tmp_amazon_members_mapping amm ON bw.user_id = amm.user_id 
                                      WHERE (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs <= 0.21 
                                      AND amm.job_category = 'Corporate'"""),
        ('Ops No GLP1 Users', """JOIN tmp_amazon_no_glp1_users_all noglp ON bw.user_id = noglp.user_id 
                                 JOIN tmp_amazon_members_mapping amm ON bw.user_id = amm.user_id 
                                 WHERE (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs <= 0.21 
                                 AND amm.job_category = 'Ops'""")
    ]
    
    # Generate queries for all user groups
    for group_name, join_where_clause in user_groups:
        base_query = f"""
            INSERT INTO tmp_weight_loss_analysis
            SELECT 
                'Weight Loss Outcomes' as metric_category,
                'All Users' as time_period,
                '{group_name}' as user_group,
                COUNT(DISTINCT bw.user_id) as total_users_with_data,
                ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
                ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
                COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
                COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss,
                ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_5_percent,
                ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_10_percent
            FROM tmp_baseline_weight_all bw
            JOIN tmp_latest_weight_all lw ON bw.user_id = lw.user_id
            {join_where_clause}
        """
        
        execute_with_timing(cursor, base_query, f"Insert {group_name} analysis")

def create_blood_pressure_analysis(cursor):
    """Create blood pressure analysis with Corporate/Ops breakdowns"""
    print(f"\n🩺 Creating blood pressure analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_bp_analysis", "Drop BP analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_bp_analysis (
            metric_category VARCHAR(255),
            time_period VARCHAR(50),
            user_group VARCHAR(100),
            total_users_with_data INT,
            avg_baseline_systolic DECIMAL(10,1),
            avg_baseline_diastolic DECIMAL(10,1),
            avg_latest_systolic DECIMAL(10,1),
            avg_latest_diastolic DECIMAL(10,1),
            avg_systolic_change DECIMAL(10,1),
            avg_diastolic_change DECIMAL(10,1),
            avg_days_between_readings DECIMAL(10,0)
        )
    """, "Create BP analysis table structure")
    
    # Define all user groups for BP analysis (FIXED JOINS)
    bp_groups = [
        ('All Users', ''),
        ('Corporate', "JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id WHERE amm.job_category = 'Corporate'"),
        ('Ops', "JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id WHERE amm.job_category = 'Ops'"),
        ('GLP1 Users', 'JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id'),
        ('Corporate GLP1 Users', """JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                                   JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                                   WHERE amm.job_category = 'Corporate'"""),
        ('Ops GLP1 Users', """JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                              JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                              WHERE amm.job_category = 'Ops'"""),
        ('No GLP1 Users', 'LEFT JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id WHERE glp.user_id IS NULL'),
        ('Corporate No GLP1 Users', """LEFT JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                                      JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                                      WHERE glp.user_id IS NULL AND amm.job_category = 'Corporate'"""),
        ('Ops No GLP1 Users', """LEFT JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                                 JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                                 WHERE glp.user_id IS NULL AND amm.job_category = 'Ops'""")
    ]
    
    # Generate queries for all BP groups
    for group_name, join_where_clause in bp_groups:
        bp_query = f"""
            INSERT INTO tmp_bp_analysis
            SELECT 
                'Blood Pressure Management' as metric_category,
                'All Users' as time_period,
                '{group_name}' as user_group,
                COUNT(DISTINCT bbb.user_id) as total_users_with_data,
                ROUND(AVG(bbb.baseline_systolic), 1) as avg_baseline_systolic,
                ROUND(AVG(bbb.baseline_diastolic), 1) as avg_baseline_diastolic,
                ROUND(AVG(lbb.latest_systolic), 1) as avg_latest_systolic,
                ROUND(AVG(lbb.latest_diastolic), 1) as avg_latest_diastolic,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic), 1) as avg_systolic_change,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic), 1) as avg_diastolic_change,
                ROUND(AVG(DATEDIFF(lbb.latest_bp_date, bbb.baseline_bp_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_blood_pressure_all bbb
            JOIN tmp_latest_blood_pressure_all lbb ON bbb.user_id = lbb.user_id
            {join_where_clause}
        """
        
        execute_with_timing(cursor, bp_query, f"Insert {group_name} BP analysis")

def create_a1c_analysis(cursor, end_date='2025-12-31'):
    """Create comprehensive A1C analysis with Corporate/Ops breakdowns"""
    print(f"\n🩺 Creating A1C analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_a1c_analysis", "Drop A1C analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_a1c_analysis (
            metric_category VARCHAR(255),
            time_period VARCHAR(50),
            user_group VARCHAR(100),
            total_users_with_data INT,
            prediabetic_users INT,
            diabetic_users INT,
            uncontrolled_diabetic_users INT,
            avg_baseline_a1c DECIMAL(10,2),
            avg_latest_a1c DECIMAL(10,2),
            avg_a1c_improvement DECIMAL(10,2),
            prediabetic_avg_improvement DECIMAL(10,2),
            diabetic_avg_improvement DECIMAL(10,2),
            uncontrolled_diabetic_avg_improvement DECIMAL(10,2),
            avg_days_between_readings DECIMAL(10,0)
        )
    """, "Create A1C analysis table structure")
    
    # Define all user groups for A1C analysis (FIXED JOINS)
    a1c_groups = [
        ('All Users', ''),
        ('Corporate', "JOIN tmp_amazon_members_mapping amm ON ba1c.user_id = amm.user_id WHERE amm.job_category = 'Corporate'"),
        ('Ops', "JOIN tmp_amazon_members_mapping amm ON ba1c.user_id = amm.user_id WHERE amm.job_category = 'Ops'"),
        ('GLP1 Users', 'JOIN tmp_amazon_glp1_users_all glp ON ba1c.user_id = glp.user_id'),
        ('Corporate GLP1 Users', """JOIN tmp_amazon_glp1_users_all glp ON ba1c.user_id = glp.user_id 
                                   JOIN tmp_amazon_members_mapping amm ON ba1c.user_id = amm.user_id 
                                   WHERE amm.job_category = 'Corporate'"""),
        ('Ops GLP1 Users', """JOIN tmp_amazon_glp1_users_all glp ON ba1c.user_id = glp.user_id 
                              JOIN tmp_amazon_members_mapping amm ON ba1c.user_id = amm.user_id 
                              WHERE amm.job_category = 'Ops'"""),
        ('No GLP1 Users', 'LEFT JOIN tmp_amazon_glp1_users_all glp ON ba1c.user_id = glp.user_id WHERE glp.user_id IS NULL'),
        ('Corporate No GLP1 Users', """LEFT JOIN tmp_amazon_glp1_users_all glp ON ba1c.user_id = glp.user_id 
                                      JOIN tmp_amazon_members_mapping amm ON ba1c.user_id = amm.user_id 
                                      WHERE glp.user_id IS NULL AND amm.job_category = 'Corporate'"""),
        ('Ops No GLP1 Users', """LEFT JOIN tmp_amazon_glp1_users_all glp ON ba1c.user_id = glp.user_id 
                                 JOIN tmp_amazon_members_mapping amm ON ba1c.user_id = amm.user_id 
                                 WHERE glp.user_id IS NULL AND amm.job_category = 'Ops'""")
    ]
    
    # Generate queries for all A1C groups
    for group_name, join_where_clause in a1c_groups:
        a1c_query = f"""
            INSERT INTO tmp_a1c_analysis
            SELECT 
                'A1C Management' as metric_category,
                'All Users' as time_period,
                '{group_name}' as user_group,
                COUNT(DISTINCT ba1c.user_id) as total_users_with_data,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 5.7 THEN ba1c.user_id END) as prediabetic_users,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 6.5 AND ba1c.baseline_a1c < 7.0 THEN ba1c.user_id END) as diabetic_users,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 7.0 THEN ba1c.user_id END) as uncontrolled_diabetic_users,
                ROUND(AVG(ba1c.baseline_a1c), 2) as avg_baseline_a1c,
                ROUND(AVG(la1c.latest_a1c), 2) as avg_latest_a1c,
                ROUND(AVG(ba1c.baseline_a1c - la1c.latest_a1c), 2) as avg_a1c_improvement,
                ROUND(AVG(CASE WHEN ba1c.baseline_a1c >= 5.7 THEN ba1c.baseline_a1c - la1c.latest_a1c END), 2) as prediabetic_avg_improvement,
                ROUND(AVG(CASE WHEN ba1c.baseline_a1c >= 6.5 AND ba1c.baseline_a1c < 7.0 THEN ba1c.baseline_a1c - la1c.latest_a1c END), 2) as diabetic_avg_improvement,
                ROUND(AVG(CASE WHEN ba1c.baseline_a1c >= 7.0 THEN ba1c.baseline_a1c - la1c.latest_a1c END), 2) as uncontrolled_diabetic_avg_improvement,
                ROUND(AVG(DATEDIFF(la1c.latest_a1c_date, ba1c.baseline_a1c_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_a1c_all ba1c
            JOIN tmp_latest_a1c_all la1c ON ba1c.user_id = la1c.user_id
            {join_where_clause}
        """
        
        execute_with_timing(cursor, a1c_query, f"Insert {group_name} A1C analysis")

def create_demographic_weight_loss_analysis(cursor):
    """Create demographic-specific weight loss analysis"""
    print(f"\n👥 Creating demographic weight loss analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_demographic_weight_analysis", "Drop demographic weight analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_demographic_weight_analysis (
            metric_category VARCHAR(255),
            time_period VARCHAR(50),
            user_group VARCHAR(100),
            demographic_segment VARCHAR(100),
            total_users_with_data INT,
            avg_weight_loss_lbs DECIMAL(10,2),
            avg_percent_weight_loss DECIMAL(10,2),
            users_5_percent_loss INT,
            users_10_percent_loss INT,
            percent_achieving_5_percent DECIMAL(10,2),
            percent_achieving_10_percent DECIMAL(10,2)
        )
    """, "Create demographic weight analysis table structure")
    
    # Define demographic groups (using the working format from your original script)
    demographics = [
        ('Female', 'FEMALE', 'sex'),
        ('Male', 'MALE', 'sex'),
        ('Black/African American', 'BLACK_OR_AFRICAN_AMERICAN', 'ethnicity'),
        ('Hispanic/Latino', 'HISPANIC_LATINO', 'ethnicity'),
        ('Asian', 'ASIAN', 'ethnicity')
    ]
    
    # Insert results for each demographic group
    for demo_name, demo_value, demo_field in demographics:
        # All users in demographic
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_demographic_weight_analysis
            SELECT 
                'Weight Loss by Demographics' as metric_category,
                'All Users' as time_period,
                '{demo_name}' as user_group,
                '{demo_name}' as demographic_segment,
                COUNT(DISTINCT bw.user_id) as total_users_with_data,
                ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
                ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
                COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
                COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss,
                ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_5_percent,
                ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_10_percent
            FROM tmp_baseline_weight_all bw
            JOIN tmp_latest_weight_all lw ON bw.user_id = lw.user_id
            JOIN users u ON bw.user_id = u.id
            WHERE u.{demo_field} = '{demo_value}'
        """, f"Insert {demo_name} analysis")
        
        # GLP1 users in demographic
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_demographic_weight_analysis
            SELECT 
                'Weight Loss by Demographics' as metric_category,
                'All Users' as time_period,
                '{demo_name} GLP1 Users' as user_group,
                '{demo_name} GLP1' as demographic_segment,
                COUNT(DISTINCT bw.user_id) as total_users_with_data,
                ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
                ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
                COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
                COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss,
                ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_5_percent,
                ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_10_percent
            FROM tmp_baseline_weight_all bw
            JOIN tmp_latest_weight_all lw ON bw.user_id = lw.user_id
            JOIN tmp_amazon_glp1_users_all glp ON bw.user_id = glp.user_id
            JOIN users u ON bw.user_id = u.id
            WHERE u.{demo_field} = '{demo_value}'
        """, f"Insert {demo_name} GLP1 analysis")

def create_demographic_a1c_analysis(cursor):
    """Create demographic-specific A1C analysis"""
    print(f"\n👥 Creating demographic A1C analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_demographic_a1c_analysis", "Drop demographic A1C analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_demographic_a1c_analysis (
            metric_category VARCHAR(255),
            time_period VARCHAR(50),
            user_group VARCHAR(100),
            demographic_segment VARCHAR(100),
            total_users_with_data INT,
            prediabetic_users INT,
            diabetic_users INT,
            uncontrolled_diabetic_users INT,
            avg_baseline_a1c DECIMAL(10,2),
            avg_latest_a1c DECIMAL(10,2),
            avg_a1c_improvement DECIMAL(10,2),
            users_with_significant_improvement INT,
            percent_with_significant_improvement DECIMAL(10,2)
        )
    """, "Create demographic A1C analysis table structure")
    
    # Define demographic groups
    demographics = [
        ('Female', 'FEMALE', 'sex'),
        ('Male', 'MALE', 'sex'),
        ('Black/African American', 'BLACK_OR_AFRICAN_AMERICAN', 'ethnicity'),
        ('Hispanic/Latino', 'HISPANIC_LATINO', 'ethnicity'),
        ('Asian', 'ASIAN', 'ethnicity')
    ]
    
    # Insert results for each demographic group
    for demo_name, demo_value, demo_field in demographics:
        # All users in demographic
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_demographic_a1c_analysis
            SELECT 
                'A1C by Demographics' as metric_category,
                'All Users' as time_period,
                '{demo_name}' as user_group,
                '{demo_name}' as demographic_segment,
                COUNT(DISTINCT ba1c.user_id) as total_users_with_data,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 THEN ba1c.user_id END) as prediabetic_users,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 6.5 AND ba1c.baseline_a1c < 7.0 THEN ba1c.user_id END) as diabetic_users,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 7.0 THEN ba1c.user_id END) as uncontrolled_diabetic_users,
                ROUND(AVG(ba1c.baseline_a1c), 2) as avg_baseline_a1c,
                ROUND(AVG(la1c.latest_a1c), 2) as avg_latest_a1c,
                ROUND(AVG(ba1c.baseline_a1c - la1c.latest_a1c), 2) as avg_a1c_improvement,
                COUNT(DISTINCT CASE WHEN (ba1c.baseline_a1c - la1c.latest_a1c) >= 0.5 THEN ba1c.user_id END) as users_with_significant_improvement,
                ROUND((COUNT(DISTINCT CASE WHEN (ba1c.baseline_a1c - la1c.latest_a1c) >= 0.5 THEN ba1c.user_id END) * 100.0 / COUNT(DISTINCT ba1c.user_id)), 2) as percent_with_significant_improvement
            FROM tmp_baseline_a1c_all ba1c
            JOIN tmp_latest_a1c_all la1c ON ba1c.user_id = la1c.user_id
            JOIN users u ON ba1c.user_id = u.id
            WHERE u.{demo_field} = '{demo_value}'
        """, f"Insert {demo_name} A1C analysis")
        
        # GLP1 users in demographic
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_demographic_a1c_analysis
            SELECT 
                'A1C by Demographics' as metric_category,
                'All Users' as time_period,
                '{demo_name} GLP1 Users' as user_group,
                '{demo_name} GLP1' as demographic_segment,
                COUNT(DISTINCT ba1c.user_id) as total_users_with_data,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 THEN ba1c.user_id END) as prediabetic_users,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 6.5 AND ba1c.baseline_a1c < 7.0 THEN ba1c.user_id END) as diabetic_users,
                COUNT(DISTINCT CASE WHEN ba1c.baseline_a1c >= 7.0 THEN ba1c.user_id END) as uncontrolled_diabetic_users,
                ROUND(AVG(ba1c.baseline_a1c), 2) as avg_baseline_a1c,
                ROUND(AVG(la1c.latest_a1c), 2) as avg_latest_a1c,
                ROUND(AVG(ba1c.baseline_a1c - la1c.latest_a1c), 2) as avg_a1c_improvement,
                COUNT(DISTINCT CASE WHEN (ba1c.baseline_a1c - la1c.latest_a1c) >= 0.5 THEN ba1c.user_id END) as users_with_significant_improvement,
                ROUND((COUNT(DISTINCT CASE WHEN (ba1c.baseline_a1c - la1c.latest_a1c) >= 0.5 THEN ba1c.user_id END) * 100.0 / COUNT(DISTINCT ba1c.user_id)), 2) as percent_with_significant_improvement
            FROM tmp_baseline_a1c_all ba1c
            JOIN tmp_latest_a1c_all la1c ON ba1c.user_id = la1c.user_id
            JOIN tmp_amazon_glp1_users_all glp ON ba1c.user_id = glp.user_id
            JOIN users u ON ba1c.user_id = u.id
            WHERE u.{demo_field} = '{demo_value}'
        """, f"Insert {demo_name} GLP1 A1C analysis")

def export_results_to_excel(cursor, filename='amazon_qbr_results.xlsx'):
    """Export all analysis results to Excel with separate sheets"""
    
    # Create csv folder if it doesn't exist
    csv_folder = 'csv'
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
        print(f"    📁 Created {csv_folder} folder")
    
    # Update filename to include folder path
    filename = os.path.join(csv_folder, filename)
    
    print(f"\n📊 Exporting results to {filename}...")
    
    # Define all analysis tables to export with their sheet names
    tables_to_export = [
        ('Weight Loss Outcomes', 'tmp_weight_loss_analysis'),
        ('Demographic Weight Analysis', 'tmp_demographic_weight_analysis'),
        ('Blood Pressure Management', 'tmp_bp_analysis'),
        ('Hypertension Management', 'tmp_hypertension_analysis'),
        ('A1C Management', 'tmp_a1c_analysis'),
        ('Demographic A1C Analysis', 'tmp_demographic_a1c_analysis')
    ]
    
    # Create Excel writer object
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        all_results = []
        
        for sheet_name, table_name in tables_to_export:
            try:
                # Get column names
                cursor.execute(f"DESCRIBE {table_name}")
                columns = [col[0] for col in cursor.fetchall()]
                
                # Get data
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()
                
                if rows:
                    # Create DataFrame
                    df = pd.DataFrame(rows, columns=columns)
                    
                    # Write to Excel sheet (Excel sheet names have a 31 character limit)
                    safe_sheet_name = sheet_name[:31] if len(sheet_name) > 31 else sheet_name
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    
                    print(f"    📋 Exported {len(rows)} rows to sheet '{safe_sheet_name}'")
                    
                    # Also add to combined results for CSV backup
                    for row in rows:
                        result_dict = {'analysis_type': sheet_name}
                        for i, col in enumerate(columns):
                            result_dict[col] = row[i]
                        all_results.append(result_dict)
                else:
                    print(f"    ⚠️  No data found for {sheet_name}")
                    
            except Exception as e:
                print(f"    ⚠️  Warning: Could not export {sheet_name}: {e}")
                continue
    
    print(f"    ✅ Successfully exported to Excel: {filename}")
    
    # Also create a CSV backup with all data combined
    csv_filename = filename.replace('.xlsx', '.csv')
    if all_results:
        # Get all unique column names
        all_columns = set()
        for result in all_results:
            all_columns.update(result.keys())
        
        # Sort columns for consistent output
        sorted_columns = sorted(all_columns)
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted_columns)
            writer.writeheader()
            writer.writerows(all_results)
        
        print(f"    📄 Also created CSV backup: {csv_filename}")

def export_weight_loss_user_lists(cursor, filename='weight_loss_user_lists.xlsx'):
    """Export user lists for Weight Loss Outcomes analysis - All Users"""
    
    # Create csv folder if it doesn't exist
    csv_folder = 'csv'
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
        print(f"    📁 Created {csv_folder} folder")
    
    # Update filename to include folder path
    filename = os.path.join(csv_folder, filename)
    
    print(f"\n👥 Exporting Weight Loss user lists to {filename}...")
    
    # Create Excel writer object
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        
        # 1. All Users - users with both baseline and latest weight       
        print("    📊 Getting All Users...")
        cursor.execute("""
            SELECT 
                BIN_TO_UUID(bw.user_id) as user_id,
                au.start_date,
                bw.baseline_weight_lbs,
                bw.baseline_weight_date,
                lw.latest_weight_lbs,
                lw.latest_weight_date,
                ROUND(bw.baseline_weight_lbs - lw.latest_weight_lbs, 2) as weight_loss_lbs,
                ROUND((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100, 2) as percent_weight_loss,
                CASE 
                    WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN 'Yes'
                    ELSE 'No'
                END as achieved_10_percent_loss,
                CASE 
                    WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN 'Yes'
                    ELSE 'No'
                END as achieved_5_percent_loss
            FROM tmp_baseline_weight_all bw
            JOIN tmp_latest_weight_all lw ON bw.user_id = lw.user_id
            JOIN tmp_amazon_users_all au ON bw.user_id = au.user_id
            ORDER BY percent_weight_loss DESC
        """)
        
        rows = cursor.fetchall()
        columns = ['user_id', 'start_date', 'baseline_weight_lbs', 'baseline_weight_date', 
                  'latest_weight_lbs', 'latest_weight_date', 'weight_loss_lbs', 
                  'percent_weight_loss', 'achieved_10_percent_loss', 'achieved_5_percent_loss']
        
        if rows:
            df_all = pd.DataFrame(rows, columns=columns)
            df_all.to_excel(writer, sheet_name='All Users', index=False)
            print(f"    ✅ All Users: {len(rows)} users")
        
        # 2. GLP1 Users
        print("    💊 Getting GLP1 Users...")
        cursor.execute("""
            SELECT 
                BIN_TO_UUID(bw.user_id) as user_id,
                au.start_date,
                glp.prescribed_at as glp1_start_date,
                glp.prescription_end_date as glp1_end_date,
                bw.baseline_weight_lbs,
                bw.baseline_weight_date,
                lw.latest_weight_lbs,
                lw.latest_weight_date,
                ROUND(bw.baseline_weight_lbs - lw.latest_weight_lbs, 2) as weight_loss_lbs,
                ROUND((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100, 2) as percent_weight_loss,
                CASE 
                    WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN 'Yes'
                    ELSE 'No'
                END as achieved_10_percent_loss,
                CASE 
                    WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN 'Yes'
                    ELSE 'No'
                END as achieved_5_percent_loss
            FROM tmp_baseline_weight_all bw
            JOIN tmp_latest_weight_all lw ON bw.user_id = lw.user_id
            JOIN tmp_amazon_users_all au ON bw.user_id = au.user_id
            JOIN tmp_amazon_glp1_users_all glp ON bw.user_id = glp.user_id
            ORDER BY percent_weight_loss DESC
        """)
        
        rows = cursor.fetchall()
        columns = ['user_id', 'start_date', 'glp1_start_date', 'glp1_end_date',
                  'baseline_weight_lbs', 'baseline_weight_date', 
                  'latest_weight_lbs', 'latest_weight_date', 'weight_loss_lbs', 
                  'percent_weight_loss', 'achieved_10_percent_loss', 'achieved_5_percent_loss']
        
        if rows:
            df_glp1 = pd.DataFrame(rows, columns=columns)
            df_glp1.to_excel(writer, sheet_name='GLP1 Users', index=False)
            print(f"    ✅ GLP1 Users: {len(rows)} users")
        
        # 3. No GLP1 Users
        print("    🚫 Getting No GLP1 Users...")
        cursor.execute("""
            SELECT 
                BIN_TO_UUID(bw.user_id) as user_id,
                au.start_date,
                bw.baseline_weight_lbs,
                bw.baseline_weight_date,
                lw.latest_weight_lbs,
                lw.latest_weight_date,
                ROUND(bw.baseline_weight_lbs - lw.latest_weight_lbs, 2) as weight_loss_lbs,
                ROUND((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100, 2) as percent_weight_loss,
                CASE 
                    WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN 'Yes'
                    ELSE 'No'
                END as achieved_10_percent_loss,
                CASE 
                    WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN 'Yes'
                    ELSE 'No'
                END as achieved_5_percent_loss
            FROM tmp_baseline_weight_all bw
            JOIN tmp_latest_weight_all lw ON bw.user_id = lw.user_id
            JOIN tmp_amazon_users_all au ON bw.user_id = au.user_id
            JOIN tmp_amazon_no_glp1_users_all noglp ON bw.user_id = noglp.user_id
            ORDER BY percent_weight_loss DESC
        """)
        
        rows = cursor.fetchall()
        columns = ['user_id', 'start_date', 'baseline_weight_lbs', 'baseline_weight_date', 
                  'latest_weight_lbs', 'latest_weight_date', 'weight_loss_lbs', 
                  'percent_weight_loss', 'achieved_10_percent_loss', 'achieved_5_percent_loss']
        
        if rows:
            df_no_glp1 = pd.DataFrame(rows, columns=columns)
            df_no_glp1.to_excel(writer, sheet_name='No GLP1 Users', index=False)
            print(f"    ✅ No GLP1 Users: {len(rows)} users")
    
    print(f"    📊 Successfully exported user lists to: {filename}")

def main_amazon_analysis(end_date='2025-12-31'):
    """Main function to run Amazon QBR analysis"""
    print(f"🚀 Starting Amazon QBR Analysis (as of {end_date})")
    
    with connect_to_db() as conn:
        with conn.cursor() as cursor:
            try:
                # Create base tables
                create_amazon_user_tables(cursor, end_date=end_date)
                create_amazon_users_6month_retention_table(cursor, end_date=end_date)
                create_amazon_members_mapping(cursor)  # NEW - Create the mapping table first
                create_amazon_glp1_tables(cursor, end_date=end_date)
                create_amazon_no_glp1_tables(cursor)
                create_weight_metrics_tables(cursor, end_date=end_date)
                create_blood_pressure_tables(cursor, end_date=end_date)
                create_a1c_metrics_tables(cursor, end_date=end_date)
                
                # Create analysis tables
                create_weight_loss_analysis(cursor)
                create_demographic_weight_loss_analysis(cursor)
                create_blood_pressure_analysis(cursor)
                create_hypertension_analysis(cursor)
                create_a1c_analysis(cursor)
                create_demographic_a1c_analysis(cursor)
                
                # Export results to Excel
                export_results_to_excel(cursor)
                
                # Export user lists for Weight Loss Outcomes
                export_weight_loss_user_lists(cursor)
                
                print(f"\n✅ Amazon QBR Analysis Complete!")
                
            finally:
                # Updated cleanup to include mapping table
                cleanup_tables = [
                    'tmp_amazon_users_all', 'tmp_amazon_users_6month', 
                    'tmp_amazon_members_mapping',  # NEW
                    'tmp_amazon_glp1_users_all', 'tmp_amazon_no_glp1_users_all',
                    'tmp_baseline_weight_all', 'tmp_latest_weight_all',
                    'tmp_baseline_blood_pressure_all', 'tmp_latest_blood_pressure_all',
                    'tmp_baseline_a1c_all', 'tmp_latest_a1c_all',
                    'tmp_weight_loss_analysis', 'tmp_demographic_weight_analysis', 'tmp_bp_analysis',
                    'tmp_hypertension_analysis', 'tmp_a1c_analysis', 'tmp_demographic_a1c_analysis'
                ]
                for table in cleanup_tables:
                    execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS {table}", f"Cleanup {table}")

if __name__ == "__main__":
    main_amazon_analysis()