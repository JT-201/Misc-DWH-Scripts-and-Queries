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

def create_apple_users_table(cursor, partner='Amazon', end_date='2025-12-31'):
    """Create temporary table for ALL active partner users (for general analytics)"""
    print(f"\n🍎 Creating {partner} users table (all active users on {end_date})...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_apple_users", "Drop partner users table")
    
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_apple_users AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name = '{partner}'
        AND s.status = 'ACTIVE'
    """, f"Create {partner} users table (all active users)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_apple_users_user_id ON tmp_apple_users(user_id)", "Index partner users table")

def create_apple_users_6month_retention_table(cursor, partner='Apple', end_date='2025-12-31'):
    """Create temporary table for 6-month retention users using consecutive engagement logic"""
    print(f"\n🏥 Creating {partner} 6-month retention users table...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_apple_users_6month", "Drop 6-month retention users table")
    
    # Step 1: Get base partner users
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_apple_users_6month AS
        WITH apple_base_users AS (
            SELECT DISTINCT s.user_id, s.start_date
            FROM subscriptions s
            JOIN partner_employers pe ON pe.user_id = s.user_id
            WHERE pe.name = '{partner}'
            AND s.status = 'ACTIVE'
            AND (s.cancellation_date IS NULL OR s.cancellation_date < s.start_date)
            AND s.start_date <= '{end_date}'
        ),
        user_monthly_engagement AS (
            SELECT 
                abu.user_id,
                abu.start_date,
                DATE_FORMAT(bus.date, '%Y-%m') as engagement_month
            FROM apple_base_users abu
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
    """, f"Create {partner} 6-month retention users table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_apple_users_6month_user_id ON tmp_apple_users_6month(user_id)", "Index 6-month retention users table")
    
    # Print retention statistics
    cursor.execute("SELECT COUNT(*) FROM tmp_apple_users")
    all_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM tmp_apple_users_6month")
    retained_count = cursor.fetchone()[0]
    
    print(f"  📊 All {partner} users: {all_count}")
    print(f"  📊 6-month retention users: {retained_count}")
    
    if all_count > 0:
        retention_rate = (retained_count / all_count * 100)
        print(f"  📊 Retention rate: {retention_rate:.1f}%")

def execute_with_timing(cursor, query: str, description: str = "Query"):
    """Execute a query with timing logging - handle results properly"""
    start_time = time.time()
    cursor.execute(query)
    
    # Always consume any results to prevent unread result errors
    try:
        # For queries that return results, fetch them (but don't necessarily use them)
        results = cursor.fetchall()
        if results and description.startswith("Count"):
            # If it's a count query, we might want to return the result
            return results
    except:
        # For DDL queries (CREATE, DROP, etc.) that don't return results
        pass
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"    ⏱️  {description}: {duration:.2f}s")
    return duration

def create_health_metrics_tables(cursor, start_date='2025-01-01', end_date='2025-12-31'):
    """Create health metrics tables using 6-month retention users ONLY"""
    print(f"\n🏥 Creating health metrics tables (6-month retention users only, {start_date} to {end_date})...")
    
    # Create baseline and latest weight tables - USING 6-MONTH RETENTION USERS
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_weight", "Drop baseline weight table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_weight AS
        WITH ranked_weights AS (
            SELECT 
                bwv.user_id,
                bwv.value * 2.20462 as weight_lbs,
                bwv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bwv.user_id ORDER BY bwv.effective_date ASC) as rn
            FROM body_weight_values_cleaned bwv
            JOIN tmp_apple_users_6month au ON bwv.user_id = au.user_id  -- 6-MONTH RETENTION USERS
            WHERE bwv.value IS NOT NULL
              AND bwv.effective_date >= DATE_SUB(au.start_date, INTERVAL 30 DAY)
              AND bwv.effective_date <= '{end_date}'
        )
        SELECT user_id, weight_lbs as baseline_weight_lbs, effective_date as baseline_weight_date
        FROM ranked_weights WHERE rn = 1
    """, "Create baseline weight table (6-month retention users)")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_weight", "Drop latest weight table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_weight AS
        WITH ranked_weights AS (
            SELECT 
                bwv.user_id,
                bwv.value * 2.20462 as weight_lbs,
                bwv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bwv.user_id ORDER BY bwv.effective_date DESC) as rn
            FROM body_weight_values_cleaned bwv
            JOIN tmp_apple_users_6month au ON bwv.user_id = au.user_id  -- 6-MONTH RETENTION USERS
            WHERE bwv.value IS NOT NULL
              AND bwv.effective_date >= '{start_date}'
              AND bwv.effective_date <= '{end_date}'
        )
        SELECT user_id, weight_lbs as latest_weight_lbs, effective_date as latest_weight_date
        FROM ranked_weights WHERE rn = 1
    """, "Create latest weight table (6-month retention users)")
    
    # Create baseline and latest BMI tables - USING 6-MONTH RETENTION USERS
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_baseline_bmi", "Drop baseline BMI table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_baseline_bmi AS
        WITH ranked_bmi AS (
            SELECT 
                bv.user_id,
                bv.value as bmi,
                bv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bv.user_id ORDER BY bv.effective_date ASC) as rn
            FROM bmi_values_cleaned bv
            JOIN tmp_apple_users_6month au ON bv.user_id = au.user_id  -- 6-MONTH RETENTION USERS
            WHERE bv.value IS NOT NULL
              AND bv.value <= 100
              AND bv.effective_date >= DATE_SUB(au.start_date, INTERVAL 30 DAY)
              AND bv.effective_date <= '{end_date}'
        )
        SELECT user_id, bmi as baseline_bmi, effective_date as baseline_bmi_date
        FROM ranked_bmi WHERE rn = 1
    """, "Create baseline BMI table (6-month retention users)")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_latest_bmi", "Drop latest BMI table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_latest_bmi AS
        WITH ranked_bmi AS (
            SELECT 
                bv.user_id,
                bv.value as bmi,
                bv.effective_date,
                ROW_NUMBER() OVER (PARTITION BY bv.user_id ORDER BY bv.effective_date DESC) as rn
            FROM bmi_values_cleaned bv
            JOIN tmp_apple_users_6month au ON bv.user_id = au.user_id  -- 6-MONTH RETENTION USERS
            WHERE bv.value IS NOT NULL
              AND bv.value <= 100
              AND bv.effective_date >= '{start_date}'
              AND bv.effective_date <= '{end_date}'
        )
        SELECT user_id, bmi as latest_bmi, effective_date as latest_bmi_date
        FROM ranked_bmi WHERE rn = 1
    """, "Create latest BMI table (6-month retention users)")
    
    # Create baseline and latest A1C tables - USING 6-MONTH RETENTION USERS
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
            JOIN tmp_apple_users_6month au ON av.user_id = au.user_id  -- 6-MONTH RETENTION USERS
            WHERE av.value IS NOT NULL
              AND av.effective_date >= DATE_SUB(au.start_date, INTERVAL 30 DAY)
              AND av.effective_date <= '{end_date}'
        )
        SELECT user_id, a1c as baseline_a1c, effective_date as baseline_a1c_date
        FROM ranked_a1c WHERE rn = 1
    """, "Create baseline A1C table (6-month retention users)")
    
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
            JOIN tmp_apple_users_6month au ON av.user_id = au.user_id  -- 6-MONTH RETENTION USERS
            WHERE av.value IS NOT NULL
              AND av.effective_date >= '{start_date}'
              AND av.effective_date <= '{end_date}'
        )
        SELECT user_id, a1c as latest_a1c, effective_date as latest_a1c_date
        FROM ranked_a1c WHERE rn = 1
    """, "Create latest A1C table (6-month retention users)")
    
    # Create baseline and latest blood pressure tables - USING 6-MONTH RETENTION USERS
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
            JOIN tmp_apple_users_6month au ON bpv.user_id = au.user_id  -- 6-MONTH RETENTION USERS
            WHERE bpv.systolic IS NOT NULL AND bpv.diastolic IS NOT NULL
              AND bpv.effective_date >= DATE_SUB(au.start_date, INTERVAL 30 DAY)
              AND bpv.effective_date <= '{end_date}'
        )
        SELECT user_id, systolic as baseline_systolic, diastolic as baseline_diastolic, 
               effective_date as baseline_bp_date
        FROM ranked_bp WHERE rn = 1
    """, "Create baseline blood pressure table (6-month retention users)")
    
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
            JOIN tmp_apple_users_6month au ON bpv.user_id = au.user_id  -- 6-MONTH RETENTION USERS
            WHERE bpv.systolic IS NOT NULL AND bpv.diastolic IS NOT NULL
              AND bpv.effective_date >= '{start_date}'
              AND bpv.effective_date <= '{end_date}'
        )
        SELECT user_id, systolic as latest_systolic, diastolic as latest_diastolic, 
               effective_date as latest_bp_date
        FROM ranked_bp WHERE rn = 1
    """, "Create latest blood pressure table (6-month retention users)")
    
    # Create GLP1 users table - UPDATED TO MATCH AMAZON STRUCTURE
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_apple_glp1_users", "Drop GLP1 users table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_apple_glp1_users AS
        WITH glp1_prescriptions AS (
            SELECT 
                au.user_id,
                p.prescribed_at,
                p.days_of_supply,
                p.total_refills,
                (p.days_of_supply + p.days_of_supply * COALESCE(p.total_refills, 0)) as total_prescription_days,
                DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * COALESCE(p.total_refills, 0)) DAY) as prescription_end_date
            FROM tmp_apple_users_6month au  -- 6-MONTH RETENTION USERS
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
        WHERE gap_percentage <= 10.0  -- More lenient than cohort script's 5%
        AND total_covered_days >= 90   -- 90 days vs 60 days in cohort script
        -- AND DATE_ADD(last_prescription_end_date, INTERVAL {coverage_gap_days} DAY) >= DATE_SUB('{end_date}', INTERVAL 90 DAY)  -- Coverage extends to end_date ± gap
    """, "Create GLP1 users table (6-month retention users)")
    
    # Create indexes
    for table in ['tmp_baseline_weight', 'tmp_latest_weight', 'tmp_baseline_bmi', 'tmp_latest_bmi', 
                  'tmp_baseline_a1c', 'tmp_latest_a1c', 'tmp_baseline_blood_pressure', 'tmp_latest_blood_pressure',
                  'tmp_apple_glp1_users']:
        execute_with_timing(cursor, f"CREATE INDEX idx_{table}_user_id ON {table}(user_id)", f"Index {table}")

def create_health_outcomes_summary_table(cursor, start_date='2025-01-01', end_date='2025-12-31'):
    """Create health outcomes summary using 6-month retention users with 30+ day requirements"""
    print(f"\n📊 Creating health outcomes summary table (6-month retention users with 30+ day requirements)...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_health_outcomes_summary", "Drop health outcomes summary table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_health_outcomes_summary AS
        SELECT 
            -- User categorization
            au.user_id,
            CASE WHEN glp1.user_id IS NOT NULL THEN 1 ELSE 0 END as is_glp1_user,
            
            -- Weight data (30+ days required)
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
            
            -- BMI data (30+ days required)
            CASE WHEN bb.baseline_bmi IS NOT NULL AND lb.latest_bmi IS NOT NULL 
                 AND DATEDIFF(lb.latest_bmi_date, bb.baseline_bmi_date) >= 30
                 THEN bb.baseline_bmi END as baseline_bmi,
            CASE WHEN bb.baseline_bmi IS NOT NULL AND lb.latest_bmi IS NOT NULL 
                 AND DATEDIFF(lb.latest_bmi_date, bb.baseline_bmi_date) >= 30
                 THEN lb.latest_bmi END as latest_bmi,
            CASE WHEN bb.baseline_bmi IS NOT NULL AND lb.latest_bmi IS NOT NULL 
                 AND DATEDIFF(lb.latest_bmi_date, bb.baseline_bmi_date) >= 30
                 THEN bb.baseline_bmi - lb.latest_bmi END as bmi_delta,
            
            -- A1C data (30+ days required)
            CASE WHEN ba.baseline_a1c IS NOT NULL AND la.latest_a1c IS NOT NULL 
                 AND DATEDIFF(la.latest_a1c_date, ba.baseline_a1c_date) >= 30
                 THEN ba.baseline_a1c END as baseline_a1c,
            CASE WHEN ba.baseline_a1c IS NOT NULL AND la.latest_a1c IS NOT NULL 
                 AND DATEDIFF(la.latest_a1c_date, ba.baseline_a1c_date) >= 30
                 THEN la.latest_a1c END as latest_a1c,
            CASE WHEN ba.baseline_a1c IS NOT NULL AND la.latest_a1c IS NOT NULL 
                 AND DATEDIFF(la.latest_a1c_date, ba.baseline_a1c_date) >= 30
                 THEN ba.baseline_a1c - la.latest_a1c END as a1c_delta,
            CASE WHEN ba.baseline_a1c >= 6.5 AND la.latest_a1c IS NOT NULL 
                 AND DATEDIFF(la.latest_a1c_date, ba.baseline_a1c_date) >= 30
                 THEN 1 ELSE 0 END as is_diabetic,
            
            -- Blood pressure data (30+ days required)
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
                 THEN lbp.latest_diastolic END as latest_diastolic,
            CASE WHEN bbp.baseline_systolic < 140 AND bbp.baseline_diastolic < 90 
                 AND bbp.baseline_systolic IS NOT NULL AND lbp.latest_systolic IS NOT NULL
                 AND DATEDIFF(lbp.latest_bp_date, bbp.baseline_bp_date) >= 30
                 THEN 1 ELSE 0 END as is_controlled_bp,
            CASE WHEN (bbp.baseline_systolic >= 140 OR bbp.baseline_diastolic >= 90) 
                 AND bbp.baseline_systolic IS NOT NULL AND lbp.latest_systolic IS NOT NULL
                 AND DATEDIFF(lbp.latest_bp_date, bbp.baseline_bp_date) >= 30
                 THEN 1 ELSE 0 END as is_uncontrolled_bp
            
        FROM tmp_apple_users_6month au  -- 6-MONTH RETENTION USERS
        LEFT JOIN tmp_baseline_weight bw ON au.user_id = bw.user_id
        LEFT JOIN tmp_latest_weight lw ON au.user_id = lw.user_id
        LEFT JOIN tmp_baseline_bmi bb ON au.user_id = bb.user_id
        LEFT JOIN tmp_latest_bmi lb ON au.user_id = lb.user_id
        LEFT JOIN tmp_baseline_a1c ba ON au.user_id = ba.user_id
        LEFT JOIN tmp_latest_a1c la ON au.user_id = la.user_id
        LEFT JOIN tmp_baseline_blood_pressure bbp ON au.user_id = bbp.user_id
        LEFT JOIN tmp_latest_blood_pressure lbp ON au.user_id = lbp.user_id
        LEFT JOIN tmp_apple_glp1_users glp1 ON au.user_id = glp1.user_id
    """, "Create health outcomes summary table (6-month retention users with 30+ day requirements)")
    
    execute_with_timing(cursor, "CREATE INDEX idx_health_outcomes_summary_user_id ON tmp_health_outcomes_summary(user_id)", "Index health outcomes summary table")

def main(partner='Amazon', analysis_start_date='2025-01-01', analysis_end_date='2025-12-31', goals_start_date='2025-01-01'):
    """Main execution function with separate user cohorts for health vs analysis metrics"""
    
    script_start_time = time.time()
    
    try:
        print(f"🔗 Connecting to database...")
        print(f"📊 Configuration:")
        print(f"  🏢 Partner: {partner}")
        print(f"  📅 Analysis Period: {analysis_start_date} to {analysis_end_date}")
        print(f"  📅 Goals/Medical Period: {goals_start_date} to {analysis_end_date}")
        print(f"  👥 User Cohorts:")
        print(f"    🏥 Health Metrics: 6-month retention users only")
        print(f"    📊 Engagement/Demographics: All active users")
        
        conn_start = time.time()
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        conn_duration = time.time() - conn_start
        print(f"  ⏱️  Database connection: {conn_duration:.2f}s")
        
        # Create BOTH user tables
        create_apple_users_table(cursor, partner, analysis_end_date)  # All active users
        create_apple_users_6month_retention_table(cursor, partner, analysis_end_date)  # 6-month retention
        
        # Create source table indexes for better performance
        create_source_table_indexes(cursor)
        
        # Create health metrics tables using 6-month retention users
        create_health_metrics_tables(cursor, analysis_start_date, analysis_end_date)
        
        # Create QBR metrics tables (pre-computed) - will use appropriate user table for each metric
        create_qbr_metrics_tables(cursor, analysis_start_date, analysis_end_date, goals_start_date)
        
        # Analysis queries to run - REMOVED NPS AND CSAT
        analysis_queries = [
            ("Health Outcomes", "SELECT * FROM tmp_health_outcomes"),
            ("Weight Medians", "SELECT * FROM tmp_weight_medians"),
            ("Demographics", "SELECT * FROM tmp_demographics"),
            ("State Distribution", "SELECT * FROM tmp_state_distribution"),
            ("Billable Activities", "SELECT * FROM tmp_billable_activities"),
            ("Analytics Events", "SELECT * FROM tmp_analytics_events"),
            ("Program Goals", "SELECT * FROM tmp_program_goals"),
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
        # Updated cleanup to include both user tables
        cleanup_start = time.time()
        cleanup_tables = [
            'tmp_apple_users', 'tmp_apple_users_6month',  # Both user tables
            'tmp_baseline_weight', 'tmp_latest_weight',
            'tmp_baseline_bmi', 'tmp_latest_bmi', 'tmp_baseline_a1c', 
            'tmp_latest_a1c', 'tmp_baseline_blood_pressure', 'tmp_latest_blood_pressure',
            'tmp_apple_glp1_users', 'tmp_health_outcomes_summary',
            # QBR metrics tables
            'tmp_demographics', 'tmp_state_distribution', 'tmp_billable_activities',
            'tmp_analytics_events', 'tmp_program_goals', 'tmp_medical_conditions', 
            'tmp_condition_groups', 'tmp_medication_counts', 'tmp_a1c_analysis', 
            'tmp_module_completion', 'tmp_health_outcomes', 'tmp_weight_medians'
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

def create_source_table_indexes(cursor):
    """Create indexes on source tables for better performance"""
    print(f"\n🔧 Creating source table indexes...")
    
    index_queries = [
        # Body weight indexes
        ("CREATE INDEX IF NOT EXISTS idx_body_weight_user_effective ON body_weight_values_cleaned(user_id, effective_date)", "Body weight user+date index"),
        
        # BMI indexes
        ("CREATE INDEX IF NOT EXISTS idx_bmi_user_effective ON bmi_values_cleaned(user_id, effective_date)", "BMI user+date index"),
        
        # A1C indexes
        ("CREATE INDEX IF NOT EXISTS idx_a1c_user_effective ON a1c_values(user_id, effective_date)", "A1C user+date index"),
        
        # Blood pressure indexes
        ("CREATE INDEX IF NOT EXISTS idx_bp_user_effective ON blood_pressure_values(user_id, effective_date)", "Blood pressure user+date index"),
        
        # Prescriptions indexes
        ("CREATE INDEX IF NOT EXISTS idx_prescriptions_user_prescribed ON prescriptions(patient_user_id, prescribed_at)", "Prescriptions user+date index"),
        
        # Billable activities indexes
        ("CREATE INDEX IF NOT EXISTS idx_billable_activities_user_timestamp ON billable_activities(user_id, activity_timestamp)", "Billable activities user+timestamp index"),
        
        # Analytics events indexes
        ("CREATE INDEX IF NOT EXISTS idx_analytics_events_user_created ON analytics_events(user_id, created_at)", "Analytics events user+created index"),
        
        # Questionnaire records indexes
        ("CREATE INDEX IF NOT EXISTS idx_questionnaire_user_answered ON questionnaire_records(user_id, answered_at)", "Questionnaire user+answered index"),
        
        # Medical conditions indexes
        ("CREATE INDEX IF NOT EXISTS idx_medical_conditions_user_recorded ON medical_conditions(user_id, recorded_at)", "Medical conditions user+recorded index"),
        
        # Tasks indexes
        ("CREATE INDEX IF NOT EXISTS idx_tasks_user_program ON tasks(user_id, program)", "Tasks user+program index")
    ]
    
    for query, description in index_queries:
        try:
            execute_with_timing(cursor, query, description)
        except Exception as e:
            print(f"    ⚠️  {description} failed: {e}")

def create_qbr_metrics_tables(cursor, start_date='2025-01-01', end_date='2025-12-31', goals_start_date='2025-01-01'):
    """Create pre-computed QBR metrics tables with GLP1 vs Non-GLP1 breakouts"""
    print(f"\n📊 Creating QBR metrics tables (Analysis: {start_date} to {end_date}, Goals: {goals_start_date} to {end_date})...")
    
    # Create the health outcomes summary table first
    create_health_outcomes_summary_table(cursor, start_date, end_date)
    
    # Create separate health outcomes tables - NO UNION ALL to avoid reopen error
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_health_outcomes", "Drop health outcomes table")
    
    # Create the main health outcomes table with OVERALL metrics only
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_health_outcomes AS
        SELECT 
            'Apple Health Outcomes - Overall' as metric_category,
            COUNT(DISTINCT user_id) as total_apple_users,
            
            -- Weight metrics
            ROUND(AVG(baseline_weight_lbs), 2) as weight_baseline_avg,
            ROUND(AVG(latest_weight_lbs), 2) as weight_current_avg,
            COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END) as weight_sample_size,
            ROUND(AVG(weight_loss_pct), 2) as weight_loss_pct,
            ROUND(AVG(weight_loss_lbs), 2) as weight_loss_lbs,
            
            -- Weight loss percentages (5% and 10%)
            ROUND(COUNT(CASE WHEN weight_loss_pct >= 5 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_5pct,
            COUNT(CASE WHEN weight_loss_pct >= 5 THEN 1 END) as lost_5pct_n,
            ROUND(COUNT(CASE WHEN weight_loss_pct >= 10 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_10pct,
            COUNT(CASE WHEN weight_loss_pct >= 10 THEN 1 END) as lost_10pct_n,
            
            -- BMI metrics
            ROUND(AVG(baseline_bmi), 2) as bmi_baseline_avg,
            ROUND(AVG(latest_bmi), 2) as bmi_current_avg,
            COUNT(CASE WHEN baseline_bmi IS NOT NULL AND latest_bmi IS NOT NULL THEN 1 END) as bmi_sample_size,
            ROUND(AVG(bmi_delta), 2) as bmi_delta,
            
            -- A1C metrics (all users)
            ROUND(AVG(baseline_a1c), 2) as a1c_baseline_avg,
            ROUND(AVG(latest_a1c), 2) as a1c_current_avg,
            COUNT(CASE WHEN baseline_a1c IS NOT NULL AND latest_a1c IS NOT NULL THEN 1 END) as a1c_sample_size,
            ROUND(AVG(a1c_delta), 2) as a1c_delta,
            
            -- A1C metrics for diabetic users (baseline 6.5%+)
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN baseline_a1c END), 2) as a1c_6_5_plus_baseline_avg,
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN latest_a1c END), 2) as a1c_6_5_plus_current_avg,
            COUNT(CASE WHEN is_diabetic = 1 AND baseline_a1c IS NOT NULL AND latest_a1c IS NOT NULL THEN 1 END) as a1c_6_5_plus_sample_size,
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN a1c_delta END), 2) as a1c_6_5_plus_delta,
            
            -- Normal BP metrics (ALL users with BP data)
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic END), 2) as bp_normal_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic END), 2) as bp_normal_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_systolic END), 2) as bp_normal_latest_systolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_diastolic END), 2) as bp_normal_latest_diastolic_avg,
            COUNT(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL 
                      THEN 1 END) as bp_normal_sample_size,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic - latest_systolic END), 2) as bp_normal_systolic_delta,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic - latest_diastolic END), 2) as bp_normal_diastolic_delta,
            
            -- Hypertensive BP metrics (ONLY users with baseline hypertension ≥140/90)
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic END), 2) as bp_htn_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic END), 2) as bp_htn_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_systolic END), 2) as bp_htn_latest_systolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_diastolic END), 2) as bp_htn_latest_diastolic_avg,
            COUNT(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL 
                          THEN 1 END) as bp_htn_sample_size,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic - latest_systolic END), 2) as bp_htn_systolic_delta,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic - latest_diastolic END), 2) as bp_htn_diastolic_delta,
            
            -- GLP1 user count
            COUNT(CASE WHEN is_glp1_user = 1 THEN 1 END) as glp1_users_total
            
        FROM tmp_health_outcomes_summary
    """, "Create overall health outcomes table")
    
    # Add GLP1 Users Health Outcomes as separate INSERT
    execute_with_timing(cursor, """
        INSERT INTO tmp_health_outcomes
        SELECT 
            'Apple Health Outcomes - GLP1' as metric_category,
            COUNT(DISTINCT user_id) as total_apple_users,
            
            -- Weight metrics (GLP1 users only)
            ROUND(AVG(baseline_weight_lbs), 2) as weight_baseline_avg,
            ROUND(AVG(latest_weight_lbs), 2) as weight_current_avg,
            COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END) as weight_sample_size,
            ROUND(AVG(weight_loss_pct), 2) as weight_loss_pct,
            ROUND(AVG(weight_loss_lbs), 2) as weight_loss_lbs,
            
            -- Weight loss percentages (GLP1 users only)
            ROUND(COUNT(CASE WHEN weight_loss_pct >= 5 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_5pct,
            COUNT(CASE WHEN weight_loss_pct >= 5 THEN 1 END) as lost_5pct_n,
            ROUND(COUNT(CASE WHEN weight_loss_pct >= 10 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_10pct,
            COUNT(CASE WHEN weight_loss_pct >= 10 THEN 1 END) as lost_10pct_n,
            
            -- BMI metrics (GLP1 users only)
            ROUND(AVG(baseline_bmi), 2) as bmi_baseline_avg,
            ROUND(AVG(latest_bmi), 2) as bmi_current_avg,
            COUNT(CASE WHEN baseline_bmi IS NOT NULL AND latest_bmi IS NOT NULL THEN 1 END) as bmi_sample_size,
            ROUND(AVG(bmi_delta), 2) as bmi_delta,
            
            -- A1C metrics (GLP1 users only)
            ROUND(AVG(baseline_a1c), 2) as a1c_baseline_avg,
            ROUND(AVG(latest_a1c), 2) as a1c_current_avg,
            COUNT(CASE WHEN baseline_a1c IS NOT NULL AND latest_a1c IS NOT NULL THEN 1 END) as a1c_sample_size,
            ROUND(AVG(a1c_delta), 2) as a1c_delta,
            
            -- A1C metrics for diabetic GLP1 users (baseline 6.5%+)
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN baseline_a1c END), 2) as a1c_6_5_plus_baseline_avg,
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN latest_a1c END), 2) as a1c_6_5_plus_current_avg,
            COUNT(CASE WHEN is_diabetic = 1 AND baseline_a1c IS NOT NULL AND latest_a1c IS NOT NULL THEN 1 END) as a1c_6_5_plus_sample_size,
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN a1c_delta END), 2) as a1c_6_5_plus_delta,
            
            -- Normal BP metrics (ALL GLP1 users with BP data)
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic END), 2) as bp_normal_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic END), 2) as bp_normal_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_systolic END), 2) as bp_normal_latest_systolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_diastolic END), 2) as bp_normal_latest_diastolic_avg,
            COUNT(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL 
                      THEN 1 END) as bp_normal_sample_size,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic - latest_systolic END), 2) as bp_normal_systolic_delta,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic - latest_diastolic END), 2) as bp_normal_diastolic_delta,
            
            -- Hypertensive BP metrics (ONLY GLP1 users with baseline hypertension ≥140/90)
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic END), 2) as bp_htn_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic END), 2) as bp_htn_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_systolic END), 2) as bp_htn_latest_systolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_diastolic END), 2) as bp_htn_latest_diastolic_avg,
            COUNT(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL 
                          THEN 1 END) as bp_htn_sample_size,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic - latest_systolic END), 2) as bp_htn_systolic_delta,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic - latest_diastolic END), 2) as bp_htn_diastolic_delta,
            
            -- GLP1 user count (should equal total_apple_users for this category)
            COUNT(CASE WHEN is_glp1_user = 1 THEN 1 END) as glp1_users_total
            
        FROM tmp_health_outcomes_summary
    """, "Create overall health outcomes table")
    
    # Add GLP1 Users Health Outcomes as separate INSERT
    execute_with_timing(cursor, """
        INSERT INTO tmp_health_outcomes
        SELECT 
            'Apple Health Outcomes - GLP1' as metric_category,
            COUNT(DISTINCT user_id) as total_apple_users,
            
            -- Weight metrics (GLP1 users only)
            ROUND(AVG(baseline_weight_lbs), 2) as weight_baseline_avg,
            ROUND(AVG(latest_weight_lbs), 2) as weight_current_avg,
            COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END) as weight_sample_size,
            ROUND(AVG(weight_loss_pct), 2) as weight_loss_pct,
            ROUND(AVG(weight_loss_lbs), 2) as weight_loss_lbs,
            
            -- Weight loss percentages (GLP1 users only)
            ROUND(COUNT(CASE WHEN weight_loss_pct >= 5 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_5pct,
            COUNT(CASE WHEN weight_loss_pct >= 5 THEN 1 END) as lost_5pct_n,
            ROUND(COUNT(CASE WHEN weight_loss_pct >= 10 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_10pct,
            COUNT(CASE WHEN weight_loss_pct >= 10 THEN 1 END) as lost_10pct_n,
            
            -- BMI metrics (GLP1 users only)
            ROUND(AVG(baseline_bmi), 2) as bmi_baseline_avg,
            ROUND(AVG(latest_bmi), 2) as bmi_current_avg,
            COUNT(CASE WHEN baseline_bmi IS NOT NULL AND latest_bmi IS NOT NULL THEN 1 END) as bmi_sample_size,
            ROUND(AVG(bmi_delta), 2) as bmi_delta,
            
            -- A1C metrics (GLP1 users only)
            ROUND(AVG(baseline_a1c), 2) as a1c_baseline_avg,
            ROUND(AVG(latest_a1c), 2) as a1c_current_avg,
            COUNT(CASE WHEN baseline_a1c IS NOT NULL AND latest_a1c IS NOT NULL THEN 1 END) as a1c_sample_size,
            ROUND(AVG(a1c_delta), 2) as a1c_delta,
            
            -- A1C metrics for diabetic GLP1 users (baseline 6.5%+)
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN baseline_a1c END), 2) as a1c_6_5_plus_baseline_avg,
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN latest_a1c END), 2) as a1c_6_5_plus_current_avg,
            COUNT(CASE WHEN is_diabetic = 1 AND baseline_a1c IS NOT NULL AND latest_a1c IS NOT NULL THEN 1 END) as a1c_6_5_plus_sample_size,
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN a1c_delta END), 2) as a1c_6_5_plus_delta,
            
            -- Normal BP metrics (ALL GLP1 users with BP data)
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic END), 2) as bp_normal_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic END), 2) as bp_normal_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_systolic END), 2) as bp_normal_latest_systolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_diastolic END), 2) as bp_normal_latest_diastolic_avg,
            COUNT(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL 
                      THEN 1 END) as bp_normal_sample_size,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic - latest_systolic END), 2) as bp_normal_systolic_delta,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic - latest_diastolic END), 2) as bp_normal_diastolic_delta,
            
            -- Hypertensive BP metrics (ONLY GLP1 users with baseline hypertension ≥140/90)
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic END), 2) as bp_htn_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic END), 2) as bp_htn_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_systolic END), 2) as bp_htn_latest_systolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_diastolic END), 2) as bp_htn_latest_diastolic_avg,
            COUNT(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL 
                          THEN 1 END) as bp_htn_sample_size,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic - latest_systolic END), 2) as bp_htn_systolic_delta,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic - latest_diastolic END), 2) as bp_htn_diastolic_delta,
            
            -- GLP1 user count (should equal total_apple_users for this category)
            COUNT(CASE WHEN is_glp1_user = 1 THEN 1 END) as glp1_users_total
            
        FROM tmp_health_outcomes_summary
        WHERE is_glp1_user = 1
    """, "Add GLP1 users health outcomes")
    
    # Add Non-GLP1 Users Health Outcomes as separate INSERT
    execute_with_timing(cursor, """
        INSERT INTO tmp_health_outcomes
        SELECT 
            'Apple Health Outcomes - Non-GLP1' as metric_category,
            COUNT(DISTINCT user_id) as total_apple_users,
            
            -- Weight metrics (Non-GLP1 users only)
            ROUND(AVG(baseline_weight_lbs), 2) as weight_baseline_avg,
            ROUND(AVG(latest_weight_lbs), 2) as weight_current_avg,
            COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END) as weight_sample_size,
            ROUND(AVG(weight_loss_pct), 2) as weight_loss_pct,
            ROUND(AVG(weight_loss_lbs), 2) as weight_loss_lbs,
            
            -- Weight loss percentages (Non-GLP1 users only)
            ROUND(COUNT(CASE WHEN weight_loss_pct >= 5 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_5pct,
            COUNT(CASE WHEN weight_loss_pct >= 5 THEN 1 END) as lost_5pct_n,
            ROUND(COUNT(CASE WHEN weight_loss_pct >= 10 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN baseline_weight_lbs IS NOT NULL AND latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_10pct,
            COUNT(CASE WHEN weight_loss_pct >= 10 THEN 1 END) as lost_10pct_n,
            
            -- BMI metrics (Non-GLP1 users only)
            ROUND(AVG(baseline_bmi), 2) as bmi_baseline_avg,
            ROUND(AVG(latest_bmi), 2) as bmi_current_avg,
            COUNT(CASE WHEN baseline_bmi IS NOT NULL AND latest_bmi IS NOT NULL THEN 1 END) as bmi_sample_size,
            ROUND(AVG(bmi_delta), 2) as bmi_delta,
            
            -- A1C metrics (Non-GLP1 users only)
            ROUND(AVG(baseline_a1c), 2) as a1c_baseline_avg,
            ROUND(AVG(latest_a1c), 2) as a1c_current_avg,
            COUNT(CASE WHEN baseline_a1c IS NOT NULL AND latest_a1c IS NOT NULL THEN 1 END) as a1c_sample_size,
            ROUND(AVG(a1c_delta), 2) as a1c_delta,
            
            -- A1C metrics for diabetic Non-GLP1 users (baseline 6.5%+)
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN baseline_a1c END), 2) as a1c_6_5_plus_baseline_avg,
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN latest_a1c END), 2) as a1c_6_5_plus_current_avg,
            COUNT(CASE WHEN is_diabetic = 1 AND baseline_a1c IS NOT NULL AND latest_a1c IS NOT NULL THEN 1 END) as a1c_6_5_plus_sample_size,
            ROUND(AVG(CASE WHEN is_diabetic = 1 THEN a1c_delta END), 2) as a1c_6_5_plus_delta,
            
            -- Normal BP metrics (ALL Non-GLP1 users with BP data)
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic END), 2) as bp_normal_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic END), 2) as bp_normal_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_systolic END), 2) as bp_normal_latest_systolic_avg,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_diastolic END), 2) as bp_normal_latest_diastolic_avg,
            COUNT(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL 
                      THEN 1 END) as bp_normal_sample_size,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic - latest_systolic END), 2) as bp_normal_systolic_delta,
            ROUND(AVG(CASE WHEN baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic - latest_diastolic END), 2) as bp_normal_diastolic_delta,
            
            -- Hypertensive BP metrics (ONLY Non-GLP1 users with baseline hypertension ≥140/90)
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic END), 2) as bp_htn_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic END), 2) as bp_htn_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_systolic END), 2) as bp_htn_latest_systolic_avg,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN latest_diastolic END), 2) as bp_htn_latest_diastolic_avg,
            COUNT(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL 
                          THEN 1 END) as bp_htn_sample_size,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_systolic - latest_systolic END), 2) as bp_htn_systolic_delta,
            ROUND(AVG(CASE WHEN (baseline_systolic >= 140 OR baseline_diastolic >= 90) 
                          AND baseline_systolic IS NOT NULL AND latest_systolic IS NOT NULL
                          THEN baseline_diastolic - latest_diastolic END), 2) as bp_htn_diastolic_delta,
            
            -- GLP1 user count (should be 0 for this category)
            COUNT(CASE WHEN is_glp1_user = 1 THEN 1 END) as glp1_users_total
            
        FROM tmp_health_outcomes_summary
        WHERE is_glp1_user = 0
    """, "Add Non-GLP1 users health outcomes")
    
    # Add GLP1 (no continuation) Users Health Outcomes as separate INSERT
    execute_with_timing(cursor, """
        INSERT INTO tmp_health_outcomes
        SELECT 
            'Apple Health Outcomes - GLP1 Disc' as metric_category,
            COUNT(DISTINCT hos.user_id) as total_apple_users,
            
            -- Weight metrics (GLP1 discontinued users only)
            ROUND(AVG(hos.baseline_weight_lbs), 2) as weight_baseline_avg,
            ROUND(AVG(hos.latest_weight_lbs), 2) as weight_current_avg,
            COUNT(CASE WHEN hos.baseline_weight_lbs IS NOT NULL AND hos.latest_weight_lbs IS NOT NULL THEN 1 END) as weight_sample_size,
            ROUND(AVG(hos.weight_loss_pct), 2) as weight_loss_pct,
            ROUND(AVG(hos.weight_loss_lbs), 2) as weight_loss_lbs,
            
            -- Weight loss percentages (GLP1 discontinued users only)
            ROUND(COUNT(CASE WHEN hos.weight_loss_pct >= 5 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN hos.baseline_weight_lbs IS NOT NULL AND hos.latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_5pct,
            COUNT(CASE WHEN hos.weight_loss_pct >= 5 THEN 1 END) as lost_5pct_n,
            ROUND(COUNT(CASE WHEN hos.weight_loss_pct >= 10 THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN hos.baseline_weight_lbs IS NOT NULL AND hos.latest_weight_lbs IS NOT NULL THEN 1 END), 0), 2) as pct_lost_10pct,
            COUNT(CASE WHEN hos.weight_loss_pct >= 10 THEN 1 END) as lost_10pct_n,
            
            -- BMI metrics (GLP1 discontinued users only)
            ROUND(AVG(hos.baseline_bmi), 2) as bmi_baseline_avg,
            ROUND(AVG(hos.latest_bmi), 2) as bmi_current_avg,
            COUNT(CASE WHEN hos.baseline_bmi IS NOT NULL AND hos.latest_bmi IS NOT NULL THEN 1 END) as bmi_sample_size,
            ROUND(AVG(hos.bmi_delta), 2) as bmi_delta,
            
            -- A1C metrics (GLP1 discontinued users only)
            ROUND(AVG(hos.baseline_a1c), 2) as a1c_baseline_avg,
            ROUND(AVG(hos.latest_a1c), 2) as a1c_current_avg,
            COUNT(CASE WHEN hos.baseline_a1c IS NOT NULL AND hos.latest_a1c IS NOT NULL THEN 1 END) as a1c_sample_size,
            ROUND(AVG(hos.a1c_delta), 2) as a1c_delta,
            
            -- A1C metrics for diabetic GLP1 discontinued users (baseline 6.5%+)
            ROUND(AVG(CASE WHEN hos.is_diabetic = 1 THEN hos.baseline_a1c END), 2) as a1c_6_5_plus_baseline_avg,
            ROUND(AVG(CASE WHEN hos.is_diabetic = 1 THEN hos.latest_a1c END), 2) as a1c_6_5_plus_current_avg,
            COUNT(CASE WHEN hos.is_diabetic = 1 AND hos.baseline_a1c IS NOT NULL AND hos.latest_a1c IS NOT NULL THEN 1 END) as a1c_6_5_plus_sample_size,
            ROUND(AVG(CASE WHEN hos.is_diabetic = 1 THEN hos.a1c_delta END), 2) as a1c_6_5_plus_delta,
            
            -- Normal BP metrics (ALL GLP1 discontinued users with BP data)
            ROUND(AVG(CASE WHEN hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.baseline_systolic END), 2) as bp_normal_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.baseline_diastolic END), 2) as bp_normal_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.latest_systolic END), 2) as bp_normal_latest_systolic_avg,
            ROUND(AVG(CASE WHEN hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.latest_diastolic END), 2) as bp_normal_latest_diastolic_avg,
            COUNT(CASE WHEN hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL 
                      THEN 1 END) as bp_normal_sample_size,
            ROUND(AVG(CASE WHEN hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.baseline_systolic - hos.latest_systolic END), 2) as bp_normal_systolic_delta,
            ROUND(AVG(CASE WHEN hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.baseline_diastolic - hos.latest_diastolic END), 2) as bp_normal_diastolic_delta,
            
            -- Hypertensive BP metrics (ONLY GLP1 discontinued users with baseline hypertension ≥140/90)
            ROUND(AVG(CASE WHEN (hos.baseline_systolic >= 140 OR hos.baseline_diastolic >= 90) 
                          AND hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.baseline_systolic END), 2) as bp_htn_baseline_systolic_avg,
            ROUND(AVG(CASE WHEN (hos.baseline_systolic >= 140 OR hos.baseline_diastolic >= 90) 
                          AND hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.baseline_diastolic END), 2) as bp_htn_baseline_diastolic_avg,
            ROUND(AVG(CASE WHEN (hos.baseline_systolic >= 140 OR hos.baseline_diastolic >= 90) 
                          AND hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.latest_systolic END), 2) as bp_htn_latest_systolic_avg,
            ROUND(AVG(CASE WHEN (hos.baseline_systolic >= 140 OR hos.baseline_diastolic >= 90) 
                          AND hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.latest_diastolic END), 2) as bp_htn_latest_diastolic_avg,
            COUNT(CASE WHEN (hos.baseline_systolic >= 140 OR hos.baseline_diastolic >= 90) 
                          AND hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL 
                          THEN 1 END) as bp_htn_sample_size,
            ROUND(AVG(CASE WHEN (hos.baseline_systolic >= 140 OR hos.baseline_diastolic >= 90) 
                          AND hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.baseline_systolic - hos.latest_systolic END), 2) as bp_htn_systolic_delta,
            ROUND(AVG(CASE WHEN (hos.baseline_systolic >= 140 OR hos.baseline_diastolic >= 90) 
                          AND hos.baseline_systolic IS NOT NULL AND hos.latest_systolic IS NOT NULL
                          THEN hos.baseline_diastolic - hos.latest_diastolic END), 2) as bp_htn_diastolic_delta,
            
            -- GLP1 user count (should equal total_apple_users for this category)
            COUNT(CASE WHEN hos.is_glp1_user = 1 THEN 1 END) as glp1_users_total
            
        FROM tmp_health_outcomes_summary hos
        WHERE hos.is_glp1_user = 1
        AND EXISTS (
            SELECT 1 
            FROM questionnaire_records qr
            WHERE qr.user_id = hos.user_id
            AND qr.question_id = 'A8z9j98E0sxR'
            AND qr.answer_value = 1
        )
    """, "Add GLP1 (Discontinued) users health outcomes")

    # Update weight medians with shorter names too
    # Overall weight medians (using 6-month retention users) - CREATE TABLE FIRST
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_weight_medians AS
        SELECT 
            'Weight Medians - Overall' as metric_category,
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
            ), 2) as max_weight_loss_percent
        FROM tmp_apple_users_6month au
        LEFT JOIN tmp_baseline_weight bw ON au.user_id = bw.user_id
        LEFT JOIN tmp_latest_weight lw ON au.user_id = lw.user_id
        WHERE bw.baseline_weight_lbs IS NOT NULL 
          AND lw.latest_weight_lbs IS NOT NULL
    """, "Create overall weight medians table")
    
    # GLP1 weight medians
    execute_with_timing(cursor, f"""
        INSERT INTO tmp_weight_medians
        SELECT 
            'Weight Medians - GLP1' as metric_category,
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
            ), 2) as max_weight_loss_percent
        FROM tmp_apple_users_6month au
        INNER JOIN tmp_apple_glp1_users glp1 ON au.user_id = glp1.user_id
        LEFT JOIN tmp_baseline_weight bw ON au.user_id = bw.user_id
        LEFT JOIN tmp_latest_weight lw ON au.user_id = lw.user_id
        WHERE bw.baseline_weight_lbs IS NOT NULL 
          AND lw.latest_weight_lbs IS NOT NULL
    """, "Add GLP1 weight medians")
    
    # Non-GLP1 weight medians
    execute_with_timing(cursor, f"""
        INSERT INTO tmp_weight_medians
        SELECT 
            'Weight Medians - Non-GLP1' as metric_category,
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
            ), 2) as max_weight_loss_percent
        FROM tmp_apple_users_6month au
        LEFT JOIN tmp_apple_glp1_users glp1 ON au.user_id = glp1.user_id
        LEFT JOIN tmp_baseline_weight bw ON au.user_id = bw.user_id
        LEFT JOIN tmp_latest_weight lw ON au.user_id = lw.user_id
        WHERE bw.baseline_weight_lbs IS NOT NULL 
          AND lw.latest_weight_lbs IS NOT NULL
          AND glp1.user_id IS NULL  -- Non-GLP1 users only
    """, "Add Non-GLP1 weight medians")

    # NOW add GLP1 (Discontinued) weight medians (table exists now)
    execute_with_timing(cursor, f"""
        INSERT INTO tmp_weight_medians
        SELECT 
            'Weight Medians - GLP1 Disc' as metric_category,
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
            ), 2) as max_weight_loss_percent
        FROM tmp_apple_users_6month au
        INNER JOIN tmp_apple_glp1_users glp1 ON au.user_id = glp1.user_id
        INNER JOIN questionnaire_records qr ON au.user_id = qr.user_id
        LEFT JOIN tmp_baseline_weight bw ON au.user_id = bw.user_id
        LEFT JOIN tmp_latest_weight lw ON au.user_id = lw.user_id
        WHERE bw.baseline_weight_lbs IS NOT NULL 
          AND lw.latest_weight_lbs IS NOT NULL
          AND qr.question_id = 'A8z9j98E0sxR'
          AND qr.answer_value = 1
    """, "Add GLP1 (Discontinued) weight medians")

    # Create placeholder tables for the remaining metrics
    placeholder_tables = [
        ('tmp_demographics', 'Demographics Analysis'),
        ('tmp_state_distribution', 'State Distribution'),
        ('tmp_billable_activities', 'Billable Activities'),
        ('tmp_analytics_events', 'Analytics Events'),
        ('tmp_program_goals', 'Program Goals'),
        ('tmp_medical_conditions', 'Medical Conditions'),
        ('tmp_condition_groups', 'Condition Groups'),
        ('tmp_medication_counts', 'Medication Counts')
    ]
    
    for table_name, category in placeholder_tables:
        execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS {table_name}", f"Drop {table_name}")
        execute_with_timing(cursor, f"""
            CREATE TEMPORARY TABLE {table_name} AS
            SELECT '{category}' as metric_category, 'Placeholder - 6-month retention applied for health metrics only' as data_type, 0 as count
        """, f"Create {table_name} placeholder")
    
    # Create the A1C and module completion tables
    create_a1c_analysis_table(cursor, start_date, end_date)
    create_module_completion_table(cursor, start_date, end_date)
    
    print(f"✅ QBR metrics tables created successfully with GLP1/Non-GLP1 breakouts!")

def create_a1c_analysis_table(cursor, start_date='2025-01-01', end_date='2025-12-31'):
    """Create A1C analysis table placeholder"""
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_a1c_analysis", "Drop A1C analysis table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_a1c_analysis AS
        SELECT 'A1C Analysis' as metric_category, 'Placeholder' as analysis_type, 0 as count
    """, "Create A1C analysis table")

def create_module_completion_table(cursor, start_date='2025-01-01', end_date='2025-12-31'):
    """Create module completion table placeholder"""
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_module_completion", "Drop module completion table")
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_module_completion AS
        SELECT 'Module Completion' as metric_category, 'Placeholder' as module_name, 0 as completion_count
    """, "Create module completion table")

if __name__ == "__main__":
    main()
