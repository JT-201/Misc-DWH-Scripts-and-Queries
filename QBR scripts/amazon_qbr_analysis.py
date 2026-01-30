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
    """Create temporary table for 6-month retention users using consecutive engagement logic"""
    print(f"\n🏥 Creating Amazon 6-month retention users table...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_6month", "Drop 6-month retention users table")
    
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_amazon_users_6month AS
        WITH amazon_base_users AS (
            SELECT DISTINCT s.user_id, s.start_date
            FROM subscriptions s
            JOIN partner_employers pe ON pe.user_id = s.user_id
            WHERE pe.name = 'Amazon'
            AND s.status = 'ACTIVE'
            AND (s.cancellation_date IS NULL OR s.cancellation_date < s.start_date)
            AND s.start_date <= '{end_date}'
        ),
        user_monthly_engagement AS (
            SELECT 
                abu.user_id,
                abu.start_date,
                DATE_FORMAT(bus.date, '%Y-%m') as engagement_month
            FROM amazon_base_users abu
            JOIN billable_user_statuses bus ON abu.user_id = bus.user_id
            WHERE bus.is_billable = 1
            AND bus.date <= '{end_date}'
            GROUP BY abu.user_id, abu.start_date, DATE_FORMAT(bus.date, '%Y-%m')
        ),
        ordered_engagement AS (
            SELECT 
                user_id,
                start_date,
                engagement_month,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY engagement_month) as month_rank
            FROM user_monthly_engagement
        ),
        consecutive_periods AS (
            SELECT 
                user_id,
                start_date,
                engagement_month,
                month_rank,
                -- Create groups of consecutive months by subtracting the rank from the period
                DATE_SUB(STR_TO_DATE(CONCAT(engagement_month, '-01'), '%Y-%m-%d'), 
                         INTERVAL (month_rank - 1) MONTH) as period_group
            FROM ordered_engagement
        ),
        consecutive_counts AS (
            SELECT 
                user_id,
                start_date,
                period_group,
                COUNT(*) as consecutive_months
            FROM consecutive_periods
            GROUP BY user_id, start_date, period_group
        )
        SELECT DISTINCT
            user_id,
            start_date
        FROM consecutive_counts
        WHERE consecutive_months >= 6  -- Has at least one period of 6+ consecutive months
    """, f"Create Amazon 6-month retention users table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_amazon_users_6month_user_id ON tmp_amazon_users_6month(user_id)", "Index 6-month retention users table")
    
    # Print retention statistics
    cursor.execute("SELECT COUNT(*) FROM tmp_amazon_users_all")
    all_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM tmp_amazon_users_6month")
    retained_count = cursor.fetchone()[0]
    
    print(f"  📊 All Amazon users: {all_count}")
    print(f"  📊 6-month retention users: {retained_count}")
    
    if all_count > 0:
        retention_rate = (retained_count / all_count * 100)
        print(f"  📊 Retention rate: {retention_rate:.1f}%")

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
                SUM(total_prescription_days) as total_covered_days,  -- FIXED: Changed from total_covered_days to total_prescription_days
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
        WHERE gap_percentage <= 10.0  -- More lenient than cohort script's 5%
        AND total_covered_days >= 90   -- 90 days vs 60 days in cohort script
        # AND DATE_ADD(last_prescription_end_date, INTERVAL {coverage_gap_days} DAY) >= DATE_SUB('{end_date}', INTERVAL 90 DAY)  -- Coverage extends to end_date ± gap
    """, f"Create Amazon GLP1 users table (coverage through {end_date} ± {coverage_gap_days} days)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_amazon_glp1_all_user_id ON tmp_amazon_glp1_users_all(user_id)", "Index Amazon GLP1 table")

def create_weight_metrics_tables(cursor, end_date='2025-12-31'):
    """Create weight metrics tables for Amazon users using 6-month retention users"""
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
            FROM body_weight_values_cleaned bwv
            JOIN {user_table} au ON bwv.user_id = au.user_id
            WHERE bwv.value IS NOT NULL
              AND bwv.effective_date >= DATE_SUB(au.start_date, INTERVAL 30 DAY)
              AND bwv.effective_date <= '{end_date}'
        )
        SELECT user_id, weight_lbs as baseline_weight_lbs, effective_date as baseline_weight_date
        FROM ranked_weights WHERE rn = 1
    """, "Create baseline weight table")

    # Latest weights from body_weight_values_cleaned
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
            WHERE bwv.value IS NOT NULL
              AND bwv.effective_date >= au.start_date
              AND bwv.effective_date <= '{end_date}'
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
    """Create hypertension analysis for all, stage 1, stage 2, and BP>130/80"""
    print(f"\n🫀 Creating hypertension analysis (Stage 1 & 2)...")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_hypertension_analysis", "Drop hypertension analysis table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_hypertension_analysis (
            group_name VARCHAR(64),
            n_users INT,
            avg_baseline_systolic DECIMAL(10,1),
            avg_baseline_diastolic DECIMAL(10,1),
            avg_latest_systolic DECIMAL(10,1),
            avg_latest_diastolic DECIMAL(10,1),
            avg_systolic_change DECIMAL(10,1),
            avg_diastolic_change DECIMAL(10,1)
        )
    """, "Create hypertension analysis table structure")

    groups = [
        # (name, WHERE clause)
        ("All Users", ""),  # No filter
        ("BP > 130/80", "WHERE (bbb.baseline_systolic > 130 OR bbb.baseline_diastolic > 80)"),
        ("Stage 1 Hypertension", """
            WHERE (
                ((bbb.baseline_systolic BETWEEN 130 AND 139) OR (bbb.baseline_diastolic BETWEEN 80 AND 89))
                AND NOT (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)
            )
        """),
        ("Stage 2 Hypertension", "WHERE (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)")
    ]

    for group_name, where_clause in groups:
        query = f"""
            INSERT INTO tmp_hypertension_analysis
            SELECT
                '{group_name}' as group_name,
                COUNT(DISTINCT bbb.user_id) as n_users,
                ROUND(AVG(bbb.baseline_systolic),1) as avg_baseline_systolic,
                ROUND(AVG(bbb.baseline_diastolic),1) as avg_baseline_diastolic,
                ROUND(AVG(lbb.latest_systolic),1) as avg_latest_systolic,
                ROUND(AVG(lbb.latest_diastolic),1) as avg_latest_diastolic,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic),1) as avg_systolic_change,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic),1) as avg_diastolic_change
            FROM tmp_baseline_blood_pressure_all bbb
            JOIN tmp_latest_blood_pressure_all lbb ON bbb.user_id = lbb.user_id
            {where_clause}
        """
        execute_with_timing(cursor, query, f"Insert {group_name} hypertension analysis")

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
    
    # Define user groups - FIXED to use health outcomes summary table
    user_groups = [
        ('All Users', 'WHERE 1=1'),  # FIXED: Added WHERE 1=1 instead of empty string
        ('Corporate', "JOIN tmp_amazon_members_mapping amm ON hos.user_id = amm.user_id WHERE amm.job_category = 'Corporate'"),
        ('Ops', "JOIN tmp_amazon_members_mapping amm ON hos.user_id = amm.user_id WHERE amm.job_category = 'Ops'"),
        ('GLP1 Users', 'WHERE hos.is_glp1_user = 1'),
        ('Corporate GLP1 Users', """JOIN tmp_amazon_members_mapping amm ON hos.user_id = amm.user_id 
                                   WHERE hos.is_glp1_user = 1 AND amm.job_category = 'Corporate'"""),
        ('Ops GLP1 Users', """JOIN tmp_amazon_members_mapping amm ON hos.user_id = amm.user_id 
                              WHERE hos.is_glp1_user = 1 AND amm.job_category = 'Ops'"""),
        ('No GLP1 Users', 'WHERE hos.is_glp1_user = 0 AND hos.weight_loss_pct <= 21'),
        ('Corporate No GLP1 Users', """JOIN tmp_amazon_members_mapping amm ON hos.user_id = amm.user_id 
                                      WHERE hos.is_glp1_user = 0 AND hos.weight_loss_pct <= 21 
                                      AND amm.job_category = 'Corporate'"""),
        ('Ops No GLP1 Users', """JOIN tmp_amazon_members_mapping amm ON hos.user_id = amm.user_id 
                                 WHERE hos.is_glp1_user = 0 AND hos.weight_loss_pct <= 21 
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
                COUNT(DISTINCT hos.user_id) as total_users_with_data,
                ROUND(AVG(hos.weight_loss_lbs), 2) as avg_weight_loss_lbs,
                ROUND(AVG(hos.weight_loss_pct), 2) as avg_percent_weight_loss,
                COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 5 THEN hos.user_id END) as users_5_percent_loss,
                COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 10 THEN hos.user_id END) as users_10_percent_loss,
                ROUND((COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 5 THEN hos.user_id END) * 100.0 / COUNT(DISTINCT hos.user_id)), 2) as percent_achieving_5_percent,
                ROUND((COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 10 THEN hos.user_id END) * 100.0 / COUNT(DISTINCT hos.user_id)), 2) as percent_achieving_10_percent
            FROM tmp_health_outcomes_summary hos
            {join_where_clause}
            AND hos.baseline_weight_lbs IS NOT NULL 
            AND hos.latest_weight_lbs IS NOT NULL
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
    
    # Define BP groups - FIXED
    bp_groups = [
        ('All Users', ''),  # No filter - all users with BP data
        ('Corporate', "JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id WHERE amm.job_category = 'Corporate'"),
        ('Ops', "JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id WHERE amm.job_category = 'Ops'"),
        ('GLP1 Users', 'JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id'),  # All GLP1 users
        ('Corporate GLP1 Users', """JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                                   JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                                   WHERE amm.job_category = 'Corporate'"""),
        ('Ops GLP1 Users', """JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id 
                              JOIN tmp_amazon_members_mapping amm ON bbb.user_id = amm.user_id 
                              WHERE amm.job_category = 'Ops'"""),
        ('No GLP1 Users', 'LEFT JOIN tmp_amazon_glp1_users_all glp ON bbb.user_id = glp.user_id WHERE glp.user_id IS NULL'),  # All No-GLP1 users
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
    
    # Define A1C groups - FIXED  
    a1c_groups = [
        ('All Users', ''),  # No filter - all users with A1C data
        ('Corporate', "JOIN tmp_amazon_members_mapping amm ON ba1c.user_id = amm.user_id WHERE amm.job_category = 'Corporate'"),
        ('Ops', "JOIN tmp_amazon_members_mapping amm ON ba1c.user_id = amm.user_id WHERE amm.job_category = 'Ops'"),
        ('GLP1 Users', 'JOIN tmp_amazon_glp1_users_all glp ON ba1c.user_id = glp.user_id'),  # All GLP1 users
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
    """Create demographic weight loss analysis with job categories"""
    print(f"\n📊 Creating demographic weight loss analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_demographic_weight_analysis", "Drop demographic weight analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_demographic_weight_analysis (
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
    """, "Create demographic weight analysis table structure")
    
    # Define original demographic groups (restored from no_CorpsOps script)
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
        demo_query = f"""
            INSERT INTO tmp_demographic_weight_analysis
            SELECT 
                'Demographic Weight Analysis' as metric_category,
                'All Users' as time_period,
                '{demo_name}' as user_group,
                COUNT(DISTINCT hos.user_id) as total_users_with_data,
                ROUND(AVG(hos.weight_loss_lbs), 2) as avg_weight_loss_lbs,
                ROUND(AVG(hos.weight_loss_pct), 2) as avg_percent_weight_loss,
                COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 5 THEN hos.user_id END) as users_5_percent_loss,
                COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 10 THEN hos.user_id END) as users_10_percent_loss,
                ROUND((COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 5 THEN hos.user_id END) * 100.0 / COUNT(DISTINCT hos.user_id)), 2) as percent_achieving_5_percent,
                ROUND((COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 10 THEN hos.user_id END) * 100.0 / COUNT(DISTINCT hos.user_id)), 2) as percent_achieving_10_percent
            FROM tmp_health_outcomes_summary hos
            JOIN users u ON hos.user_id = u.id
            WHERE hos.baseline_weight_lbs IS NOT NULL 
            AND hos.latest_weight_lbs IS NOT NULL
            AND u.{demo_field} = '{demo_value}'
        """
        
        execute_with_timing(cursor, demo_query, f"Insert {demo_name} demographic analysis")
        
        # GLP1 users in demographic
        demo_glp1_query = f"""
            INSERT INTO tmp_demographic_weight_analysis
            SELECT 
                'Demographic Weight Analysis' as metric_category,
                'All Users' as time_period,
                '{demo_name} GLP1 Users' as user_group,
                COUNT(DISTINCT hos.user_id) as total_users_with_data,
                ROUND(AVG(hos.weight_loss_lbs), 2) as avg_weight_loss_lbs,
                ROUND(AVG(hos.weight_loss_pct), 2) as avg_percent_weight_loss,
                COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 5 THEN hos.user_id END) as users_5_percent_loss,
                COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 10 THEN hos.user_id END) as users_10_percent_loss,
                ROUND((COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 5 THEN hos.user_id END) * 100.0 / COUNT(DISTINCT hos.user_id)), 2) as percent_achieving_5_percent,
                ROUND((COUNT(DISTINCT CASE WHEN hos.weight_loss_pct >= 10 THEN hos.user_id END) * 100.0 / COUNT(DISTINCT hos.user_id)), 2) as percent_achieving_10_percent
            FROM tmp_health_outcomes_summary hos
            JOIN users u ON hos.user_id = u.id
            WHERE hos.baseline_weight_lbs IS NOT NULL 
            AND hos.latest_weight_lbs IS NOT NULL
            AND hos.is_glp1_user = 1
            AND u.{demo_field} = '{demo_value}'
        """
        
        execute_with_timing(cursor, demo_glp1_query, f"Insert {demo_name} GLP1 demographic analysis")

def create_demographic_a1c_analysis(cursor):
    """Create demographic A1C analysis with original demographics"""
    print(f"\n🩺 Creating demographic A1C analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_demographic_a1c_analysis", "Drop demographic A1C analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_demographic_a1c_analysis (
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
            uncontrolled_diabetic_avg_improvement DECIMAL(10,2)
        )
    """, "Create demographic A1C analysis table structure")
    
    # Define original demographic groups (restored from no_CorpsOps script)
    demographics = [
        ('Female', 'FEMALE', 'sex'),
        ('Male', 'MALE', 'sex'),
        ('Black/African American', 'BLACK_OR_AFRICAN_AMERICAN', 'ethnicity'),
        ('Hispanic/Latino', 'HISPANIC_LATINO', 'ethnicity'),
        ('Asian', 'ASIAN', 'ethnicity')
    ]
    
    # Generate queries for all demographic groups
    for demo_name, demo_value, demo_field in demographics:
        demo_a1c_query = f"""
            INSERT INTO tmp_demographic_a1c_analysis
            SELECT 
                'Demographic A1C Analysis' as metric_category,
                'All Users' as time_period,
                '{demo_name}' as user_group,
                COUNT(DISTINCT hos.user_id) as total_users_with_data,
                COUNT(DISTINCT CASE WHEN hos.baseline_a1c >= 5.7 THEN hos.user_id END) as prediabetic_users,
                COUNT(DISTINCT CASE WHEN hos.baseline_a1c >= 6.5 AND hos.baseline_a1c < 7.0 THEN hos.user_id END) as diabetic_users,
                COUNT(DISTINCT CASE WHEN hos.baseline_a1c >= 7.0 THEN hos.user_id END) as uncontrolled_diabetic_users,
                ROUND(AVG(hos.baseline_a1c), 2) as avg_baseline_a1c,
                ROUND(AVG(hos.latest_a1c), 2) as avg_latest_a1c,
                ROUND(AVG(hos.a1c_delta), 2) as avg_a1c_improvement,
                ROUND(AVG(CASE WHEN hos.baseline_a1c >= 5.7 THEN hos.a1c_delta END), 2) as prediabetic_avg_improvement,
                ROUND(AVG(CASE WHEN hos.baseline_a1c >= 6.5 AND hos.baseline_a1c < 7.0 THEN hos.a1c_delta END), 2) as diabetic_avg_improvement,
                ROUND(AVG(CASE WHEN hos.baseline_a1c >= 7.0 THEN hos.a1c_delta END), 2) as uncontrolled_diabetic_avg_improvement
            FROM tmp_health_outcomes_summary hos
            JOIN users u ON hos.user_id = u.id
            WHERE hos.baseline_a1c IS NOT NULL 
            AND hos.latest_a1c IS NOT NULL
            AND u.{demo_field} = '{demo_value}'
        """
        
        execute_with_timing(cursor, demo_a1c_query, f"Insert {demo_name} demographic A1C analysis")
        
        # GLP1 users in demographic
        demo_glp1_a1c_query = f"""
            INSERT INTO tmp_demographic_a1c_analysis
            SELECT 
                'Demographic A1C Analysis' as metric_category,
                'All Users' as time_period,
                '{demo_name} GLP1 Users' as user_group,
                COUNT(DISTINCT hos.user_id) as total_users_with_data,
                COUNT(DISTINCT CASE WHEN hos.baseline_a1c >= 5.7 THEN hos.user_id END) as prediabetic_users,
                COUNT(DISTINCT CASE WHEN hos.baseline_a1c >= 6.5 AND hos.baseline_a1c < 7.0 THEN hos.user_id END) as diabetic_users,
                COUNT(DISTINCT CASE WHEN hos.baseline_a1c >= 7.0 THEN hos.user_id END) as uncontrolled_diabetic_users,
                ROUND(AVG(hos.baseline_a1c), 2) as avg_baseline_a1c,
                ROUND(AVG(hos.latest_a1c), 2) as avg_latest_a1c,
                ROUND(AVG(hos.a1c_delta), 2) as avg_a1c_improvement,
                ROUND(AVG(CASE WHEN hos.baseline_a1c >= 5.7 THEN hos.a1c_delta END), 2) as prediabetic_avg_improvement,
                ROUND(AVG(CASE WHEN hos.baseline_a1c >= 6.5 AND hos.baseline_a1c < 7.0 THEN hos.a1c_delta END), 2) as diabetic_avg_improvement,
                ROUND(AVG(CASE WHEN hos.baseline_a1c >= 7.0 THEN hos.a1c_delta END), 2) as uncontrolled_diabetic_avg_improvement
            FROM tmp_health_outcomes_summary hos
            JOIN users u ON hos.user_id = u.id
            WHERE hos.baseline_a1c IS NOT NULL 
            AND hos.latest_a1c IS NOT NULL
            AND hos.is_glp1_user = 1
            AND u.{demo_field} = '{demo_value}'
        """
        
        execute_with_timing(cursor, demo_glp1_a1c_query, f"Insert {demo_name} GLP1 demographic A1C analysis")

def create_health_outcomes_summary_table(cursor, end_date='2025-12-31'):
    """Create health outcomes summary using 6-month retention users with 30+ day requirements"""
    print(f"\n📊 Creating health outcomes summary table (30+ day requirements)...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_health_outcomes_summary", "Drop health outcomes summary table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_health_outcomes_summary AS
        SELECT 
            -- User categorization
            au.user_id,
            CASE WHEN glp1.user_id IS NOT NULL THEN 1 ELSE 0 END as is_glp1_user,
            
            -- Weight data (30+ days required between measurements)
            CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL 
                 AND DATEDIFF(lw.latest_weight_date, bw.baseline_weight_date) >= 30
                 THEN bw.baseline_weight_lbs END as baseline_weight_lbs,
            CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL 
                 AND DATEDIFF(lw.latest_weight_date, bw.baseline_weight_date) >= 30
                 THEN lw.latest_weight_lbs END as latest_weight_lbs,
            CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL 
                 AND DATEDIFF(lw.latest_weight_date, bw.baseline_weight_date) >= 30
                 THEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100 END as weight_loss_pct,
            CASE WHEN bw.baseline_weight_lbs IS NOT NULL AND lw.latest_weight_lbs IS NOT NULL 
                 AND DATEDIFF(lw.latest_weight_date, bw.baseline_weight_date) >= 30
                 THEN bw.baseline_weight_lbs - lw.latest_weight_lbs END as weight_loss_lbs,
            
            -- A1C data (30+ days required between measurements)
            CASE WHEN ba1c.baseline_a1c IS NOT NULL AND la1c.latest_a1c IS NOT NULL 
                 AND DATEDIFF(la1c.latest_a1c_date, ba1c.baseline_a1c_date) >= 30
                 THEN ba1c.baseline_a1c END as baseline_a1c,
            CASE WHEN ba1c.baseline_a1c IS NOT NULL AND la1c.latest_a1c IS NOT NULL 
                 AND DATEDIFF(la1c.latest_a1c_date, ba1c.baseline_a1c_date) >= 30
                 THEN la1c.latest_a1c END as latest_a1c,
            CASE WHEN ba1c.baseline_a1c IS NOT NULL AND la1c.latest_a1c IS NOT NULL 
                 AND DATEDIFF(la1c.latest_a1c_date, ba1c.baseline_a1c_date) >= 30
                 THEN ba1c.baseline_a1c - la1c.latest_a1c END as a1c_delta,
            
            -- Blood pressure data (30+ days required between measurements)
            CASE WHEN bbp.baseline_systolic IS NOT NULL AND lbp.latest_systolic IS NOT NULL 
                 AND DATEDIFF(lbp.latest_bp_date, bbp.baseline_bp_date) >= 30
                 THEN bbp.baseline_systolic END as baseline_systolic,
            CASE WHEN bbp.baseline_systolic IS NOT NULL AND lbp.latest_systolic IS NOT NULL 
                 AND DATEDIFF(lbp.latest_bp_date, bbp.baseline_bp_date) >= 30
                 THEN bbp.baseline_diastolic END as baseline_diastolic,
            CASE WHEN bbp.baseline_systolic IS NOT NULL AND lbp.latest_systolic IS NOT NULL 
                 AND DATEDIFF(lbp.latest_bp_date, bbp.baseline_bp_date) >= 30
                 THEN lbp.latest_systolic END as latest_systolic,
            CASE WHEN bbp.baseline_systolic IS NOT NULL AND lbp.latest_systolic IS NOT NULL 
                 AND DATEDIFF(lbp.latest_bp_date, bbp.baseline_bp_date) >= 30
                 THEN lbp.latest_diastolic END as latest_diastolic
            
        FROM tmp_amazon_users_6month au  -- 6-MONTH RETENTION USERS
        LEFT JOIN tmp_baseline_weight_all bw ON au.user_id = bw.user_id
        LEFT JOIN tmp_latest_weight_all lw ON au.user_id = lw.user_id
        LEFT JOIN tmp_baseline_a1c_all ba1c ON au.user_id = ba1c.user_id
        LEFT JOIN tmp_latest_a1c_all la1c ON au.user_id = la1c.user_id
        LEFT JOIN tmp_baseline_blood_pressure_all bbp ON au.user_id = bbp.user_id
        LEFT JOIN tmp_latest_blood_pressure_all lbp ON au.user_id = lbp.user_id
        LEFT JOIN tmp_amazon_glp1_users_all glp1 ON au.user_id = glp1.user_id
    """, "Create health outcomes summary table (30+ day requirements)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_health_outcomes_summary_user_id ON tmp_health_outcomes_summary(user_id)", "Index health outcomes summary table")

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
                create_health_outcomes_summary_table(cursor, end_date=end_date)  # ADD THIS LINE
                
                # Create analysis tables
                create_weight_loss_analysis(cursor)
                create_demographic_weight_loss_analysis(cursor)
                create_blood_pressure_analysis(cursor)
                create_hypertension_analysis(cursor)
                create_a1c_analysis(cursor)
                create_demographic_a1c_analysis(cursor)
                create_health_outcomes_summary_table(cursor, end_date=end_date)  # NEW - Create health outcomes summary table
                
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
                    'tmp_health_outcomes_summary',  # ADD THIS LINE
                    'tmp_amazon_glp1_users_all', 'tmp_amazon_no_glp1_users_all',
                    'tmp_baseline_weight_all', 'tmp_latest_weight_all',
                    'tmp_baseline_blood_pressure_all', 'tmp_latest_blood_pressure_all',
                    'tmp_baseline_a1c_all', 'tmp_latest_a1c_all',
                    'tmp_weight_loss_analysis', 'tmp_demographic_weight_analysis', 'tmp_bp_analysis',
                    'tmp_hypertension_analysis', 'tmp_a1c_analysis', 'tmp_demographic_a1c_analysis',
                    'tmp_health_outcomes_summary'  # NEW - Cleanup health outcomes summary table
                ]
                for table in cleanup_tables:
                    execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS {table}", f"Cleanup {table}")

if __name__ == "__main__":
    main_amazon_analysis()