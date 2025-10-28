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

def create_amazon_user_tables(cursor, end_date='2025-09-30'):
    """Create Amazon user tables for different time periods"""
    print(f"\n📦 Creating Amazon user tables (as of {end_date})...")
    
    
    # All users (previously 120 days)
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
    
    # 180 days
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_180", "Drop Amazon 180-day users table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_amazon_users_180 AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers pe ON pe.user_id = s.user_id
        WHERE pe.name = 'Amazon'
          AND s.status = 'ACTIVE'
          AND s.start_date <= DATE_SUB('{end_date}', INTERVAL 180 DAY)
    """, "Create Amazon 180-day users table")
    
    # Create indexes
    for table in ['tmp_amazon_users_all', 'tmp_amazon_users_180']:
        execute_with_timing(cursor, f"CREATE INDEX idx_{table}_user_id ON {table}(user_id)", f"Index {table}")

def create_amazon_glp1_tables(cursor, end_date='2025-10-01'):
    """Create GLP1 user tables for Amazon users"""
    print(f"\n💊 Creating Amazon GLP1 user tables...")
    
    for period in ['all', 180]:
        execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS tmp_amazon_glp1_users_{period}", f"Drop Amazon GLP1 {period} users table")
        execute_with_timing(cursor, f"""
            CREATE TEMPORARY TABLE tmp_amazon_glp1_users_{period} AS
            WITH glp1_prescriptions AS (
                SELECT 
                    au.user_id,
                    p.prescribed_at,
                    p.days_of_supply,
                    p.total_refills,
                    (p.days_of_supply + p.days_of_supply * p.total_refills) as total_prescription_days,
                    DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) as prescription_end_date
                FROM tmp_amazon_users_{period} au
                JOIN prescriptions p ON au.user_id = p.patient_user_id
                JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
                JOIN medications m ON m.id = ndcs.medication_id
                WHERE (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%')
                AND DATE_ADD(p.prescribed_at, INTERVAL (p.days_of_supply + p.days_of_supply * p.total_refills) DAY) >= DATE_SUB('{end_date}', INTERVAL 30 DAY)
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
            FROM user_prescription_coverage
        """, f"Create Amazon GLP1 {period} users table")
        
        execute_with_timing(cursor, f"CREATE INDEX idx_amazon_glp1_{period}_user_id ON tmp_amazon_glp1_users_{period}(user_id)", f"Index Amazon GLP1 {period} table")

def create_weight_metrics_tables(cursor, end_date='2025-09-30'):
    """Create weight metrics tables for Amazon users using questionnaire records"""
    print(f"\n⚖️ Creating weight metrics tables...")
    
    for period in ['all', 180]:
        # Baseline weights from questionnaire records
        execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS tmp_baseline_weight_{period}", f"Drop baseline weight {period} table")
        execute_with_timing(cursor, f"""
            CREATE TEMPORARY TABLE tmp_baseline_weight_{period} AS
            SELECT  
                user_id, 
                answer_value * 2.20462 as baseline_weight_lbs,  -- Convert kg to lbs
                answered_at as baseline_weight_date
            FROM (
                SELECT 
                    qr.user_id,
                    qr.answer_value,
                    qr.answered_at,
                    ROW_NUMBER() OVER (PARTITION BY qr.user_id ORDER BY qr.answered_at DESC) AS rn
                FROM questionnaire_records qr
                JOIN tmp_amazon_users_{period} au ON au.user_id = qr.user_id
                WHERE qr.question_id IN ('L5ft6jIJz26B')
                  AND qr.answer_value IS NOT NULL
            ) latest
            WHERE latest.rn = 1
        """, f"Create baseline weight {period} table from questionnaire")
        
        # Latest weights from body_weight_values
        execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS tmp_latest_weight_{period}", f"Drop latest weight {period} table")
        execute_with_timing(cursor, f"""
            CREATE TEMPORARY TABLE tmp_latest_weight_{period} AS
            WITH ranked_weights AS (
                SELECT 
                    bwv.user_id,
                    bwv.value * 2.20462 as weight_lbs,
                    bwv.effective_date,
                    ROW_NUMBER() OVER (PARTITION BY bwv.user_id ORDER BY bwv.effective_date DESC) as rn
                FROM body_weight_values bwv
                JOIN tmp_amazon_users_{period} au ON bwv.user_id = au.user_id
                JOIN tmp_baseline_weight_{period} bbw ON bwv.user_id = bbw.user_id
                WHERE bwv.value IS NOT NULL
                  AND bwv.effective_date >= au.start_date
                  AND bwv.effective_date <= '{end_date}'
                  AND bwv.effective_date >= DATE_ADD(bbw.baseline_weight_date, INTERVAL 30 DAY)
            )
            SELECT user_id, weight_lbs as latest_weight_lbs, effective_date as latest_weight_date
            FROM ranked_weights WHERE rn = 1
        """, f"Create latest weight {period} table")
        
        # Create indexes
        execute_with_timing(cursor, f"CREATE INDEX idx_baseline_weight_{period}_user_id ON tmp_baseline_weight_{period}(user_id)", f"Index baseline weight {period} table")
        execute_with_timing(cursor, f"CREATE INDEX idx_latest_weight_{period}_user_id ON tmp_latest_weight_{period}(user_id)", f"Index latest weight {period} table")

def create_blood_pressure_tables(cursor, end_date='2025-09-30'):
    """Create blood pressure tables for Amazon users"""
    print(f"\n🩺 Creating blood pressure tables...")
    
    for period in ['all', 180]:
        # Baseline blood pressure
        execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS tmp_baseline_blood_pressure_{period}", f"Drop baseline BP {period} table")
        execute_with_timing(cursor, f"""
            CREATE TEMPORARY TABLE tmp_baseline_blood_pressure_{period} AS
            WITH ranked_bp AS (
                SELECT 
                    bpv.user_id,
                    bpv.systolic,
                    bpv.diastolic,
                    bpv.effective_date,
                    ROW_NUMBER() OVER (PARTITION BY bpv.user_id ORDER BY bpv.effective_date ASC) as rn
                FROM blood_pressure_values bpv
                JOIN tmp_amazon_users_{period} au ON bpv.user_id = au.user_id
                WHERE bpv.systolic IS NOT NULL AND bpv.diastolic IS NOT NULL
                  AND bpv.effective_date >= au.start_date
                  AND bpv.effective_date <= '{end_date}'
            )
            SELECT user_id, systolic as baseline_systolic, diastolic as baseline_diastolic, 
                   effective_date as baseline_bp_date
            FROM ranked_bp WHERE rn = 1
        """, f"Create baseline BP {period} table")
        
        # Latest blood pressure
        execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS tmp_latest_blood_pressure_{period}", f"Drop latest BP {period} table")
        execute_with_timing(cursor, f"""
            CREATE TEMPORARY TABLE tmp_latest_blood_pressure_{period} AS
            WITH ranked_bp AS (
                SELECT 
                    bpv.user_id,
                    bpv.systolic,
                    bpv.diastolic,
                    bpv.effective_date,
                    ROW_NUMBER() OVER (PARTITION BY bpv.user_id ORDER BY bpv.effective_date DESC) as rn
                FROM blood_pressure_values bpv
                JOIN tmp_amazon_users_{period} au ON bpv.user_id = au.user_id
                JOIN tmp_baseline_blood_pressure_{period} bbbp ON bpv.user_id = bbbp.user_id
                WHERE bpv.systolic IS NOT NULL AND bpv.diastolic IS NOT NULL
                  AND bpv.effective_date >= au.start_date
                  AND bpv.effective_date <= '{end_date}'
                  AND bpv.effective_date >= DATE_ADD(bbbp.baseline_bp_date, INTERVAL 30 DAY)
            )
            SELECT user_id, systolic as latest_systolic, diastolic as latest_diastolic, 
                   effective_date as latest_bp_date
            FROM ranked_bp WHERE rn = 1
        """, f"Create latest BP {period} table")
        
        # Create indexes
        execute_with_timing(cursor, f"CREATE INDEX idx_baseline_bp_{period}_user_id ON tmp_baseline_blood_pressure_{period}(user_id)", f"Index baseline BP {period} table")
        execute_with_timing(cursor, f"CREATE INDEX idx_latest_bp_{period}_user_id ON tmp_latest_blood_pressure_{period}(user_id)", f"Index latest BP {period} table")

def create_a1c_metrics_tables(cursor, end_date='2025-09-30'):
    """Create A1C metrics tables for Amazon users"""
    print(f"\n🩺 Creating A1C metrics tables...")
    
    for period in ['all', 180]:
        # Baseline A1C values
        execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS tmp_baseline_a1c_{period}", f"Drop baseline A1C {period} table")
        execute_with_timing(cursor, f"""
            CREATE TEMPORARY TABLE tmp_baseline_a1c_{period} AS
            WITH ranked_a1c AS (
                SELECT 
                    av.user_id,
                    av.value as a1c,
                    av.effective_date,
                    ROW_NUMBER() OVER (PARTITION BY av.user_id ORDER BY av.effective_date ASC) as rn
                FROM a1c_values av
                JOIN tmp_amazon_users_{period} au ON av.user_id = au.user_id
                WHERE av.value IS NOT NULL
                  AND av.value >= 5.7  -- Only prediabetic (5.7-6.4) or diabetic (6.5+)
                  AND av.effective_date >= au.start_date
                  AND av.effective_date <= '{end_date}'
            )
            SELECT user_id, a1c as baseline_a1c, effective_date as baseline_a1c_date
            FROM ranked_a1c WHERE rn = 1
        """, f"Create baseline A1C {period} table")
        
        # Latest A1C values
        execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS tmp_latest_a1c_{period}", f"Drop latest A1C {period} table")
        execute_with_timing(cursor, f"""
            CREATE TEMPORARY TABLE tmp_latest_a1c_{period} AS
            WITH ranked_a1c AS (
                SELECT 
                    av.user_id,
                    av.value as a1c,
                    av.effective_date,
                    ROW_NUMBER() OVER (PARTITION BY av.user_id ORDER BY av.effective_date DESC) as rn
                FROM a1c_values av
                JOIN tmp_amazon_users_{period} au ON av.user_id = au.user_id
                JOIN tmp_baseline_a1c_{period} bba1c ON av.user_id = bba1c.user_id
                WHERE av.value IS NOT NULL
                  AND av.effective_date >= au.start_date
                  AND av.effective_date <= '{end_date}'
                  AND av.effective_date >= DATE_ADD(bba1c.baseline_a1c_date, INTERVAL 30 DAY)
            )
            SELECT user_id, a1c as latest_a1c, effective_date as latest_a1c_date
            FROM ranked_a1c WHERE rn = 1
        """, f"Create latest A1C {period} table")
        
        # Create indexes
        execute_with_timing(cursor, f"CREATE INDEX idx_baseline_a1c_{period}_user_id ON tmp_baseline_a1c_{period}(user_id)", f"Index baseline A1C {period} table")
        execute_with_timing(cursor, f"CREATE INDEX idx_latest_a1c_{period}_user_id ON tmp_latest_a1c_{period}(user_id)", f"Index latest A1C {period} table")

def create_weight_loss_analysis(cursor):
    """Create comprehensive weight loss analysis"""
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
    
    # Insert all users results (renamed from 120-day)
    execute_with_timing(cursor, """
        INSERT INTO tmp_weight_loss_analysis
        SELECT 
            'Weight Loss Outcomes' as metric_category,
            'All Users' as time_period,
            'All Users' as user_group,
            COUNT(DISTINCT bw.user_id) as total_users_with_data,
            ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
            ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_5_percent,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_10_percent
        FROM tmp_baseline_weight_all bw
        JOIN tmp_latest_weight_all lw ON bw.user_id = lw.user_id
    """, "Insert all users analysis")
    
    # Insert all users GLP1 results (renamed from 120-day)
    execute_with_timing(cursor, """
        INSERT INTO tmp_weight_loss_analysis
        SELECT 
            'Weight Loss Outcomes' as metric_category,
            'All Users' as time_period,
            'GLP1 Users' as user_group,
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
    """, "Insert all users GLP1 analysis")
    
    # Insert all users no GLP1 results (renamed from 120-day)
    execute_with_timing(cursor, """
        INSERT INTO tmp_weight_loss_analysis
        SELECT 
            'Weight Loss Outcomes' as metric_category,
            'All Users' as time_period,
            'No GLP1 Users' as user_group,
            COUNT(DISTINCT bw.user_id) as total_users_with_data,
            ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
            ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_5_percent,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_10_percent
        FROM tmp_baseline_weight_all bw
        JOIN tmp_latest_weight_all lw ON bw.user_id = lw.user_id
        LEFT JOIN tmp_amazon_glp1_users_all glp ON bw.user_id = glp.user_id
        WHERE glp.user_id IS NULL
    """, "Insert all users no GLP1 analysis")
    
    # Insert 180-day all users results
    execute_with_timing(cursor, """
        INSERT INTO tmp_weight_loss_analysis
        SELECT 
            'Weight Loss Outcomes' as metric_category,
            '180 Days' as time_period,
            'All Users' as user_group,
            COUNT(DISTINCT bw.user_id) as total_users_with_data,
            ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
            ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_5_percent,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_10_percent
        FROM tmp_baseline_weight_180 bw
        JOIN tmp_latest_weight_180 lw ON bw.user_id = lw.user_id
    """, "Insert 180-day all users analysis")
    
    # Insert 180-day GLP1 users results
    execute_with_timing(cursor, """
        INSERT INTO tmp_weight_loss_analysis
        SELECT 
            'Weight Loss Outcomes' as metric_category,
            '180 Days' as time_period,
            'GLP1 Users' as user_group,
            COUNT(DISTINCT bw.user_id) as total_users_with_data,
            ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
            ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_5_percent,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_10_percent
        FROM tmp_baseline_weight_180 bw
        JOIN tmp_latest_weight_180 lw ON bw.user_id = lw.user_id
        JOIN tmp_amazon_glp1_users_180 glp ON bw.user_id = glp.user_id
    """, "Insert 180-day GLP1 users analysis")
    
    # Insert 180-day no GLP1 users results
    execute_with_timing(cursor, """
        INSERT INTO tmp_weight_loss_analysis
        SELECT 
            'Weight Loss Outcomes' as metric_category,
            '180 Days' as time_period,
            'No GLP1 Users' as user_group,
            COUNT(DISTINCT bw.user_id) as total_users_with_data,
            ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
            ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
            COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_5_percent,
            ROUND((COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) * 100.0 / COUNT(DISTINCT bw.user_id)), 2) as percent_achieving_10_percent
        FROM tmp_baseline_weight_180 bw
        JOIN tmp_latest_weight_180 lw ON bw.user_id = lw.user_id
        LEFT JOIN tmp_amazon_glp1_users_180 glp ON bw.user_id = glp.user_id
        WHERE glp.user_id IS NULL
    """, "Insert 180-day no GLP1 users analysis")
 
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
            total_users_with_data INT,
            avg_weight_loss_lbs DECIMAL(10,2),
            avg_percent_weight_loss DECIMAL(10,2),
            users_5_percent_loss INT,
            users_10_percent_loss INT
        )
    """, "Create demographic weight analysis table structure")
    
    # Define demographic groups
    demographics = [
        ('Female', 'FEMALE', 'sex'),
        ('Male', 'MALE', 'sex'),
        ('Black/African American', 'BLACK_OR_AFRICAN_AMERICAN', 'ethnicity'),
        ('Hispanic/Latino', 'HISPANIC_LATINO', 'ethnicity'),
        ('Asian', 'ASIAN', 'ethnicity')
    ]
    
    # Insert results for each demographic group (all users and GLP1 users) for both All Users and 180 days
    for time_period, period in [('All Users', 'all'), ('180 Days', '180')]:
        for demo_name, demo_value, demo_field in demographics:
            # All users in demographic
            execute_with_timing(cursor, f"""
                INSERT INTO tmp_demographic_weight_analysis
                SELECT 
                    'Weight Loss Outcomes' as metric_category,
                    '{time_period}' as time_period,
                    '{demo_name}' as user_group,
                    COUNT(DISTINCT bw.user_id) as total_users_with_data,
                    ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
                    ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
                    COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
                    COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss
                FROM tmp_baseline_weight_{period} bw
                JOIN tmp_latest_weight_{period} lw ON bw.user_id = lw.user_id
                JOIN users u ON bw.user_id = u.id
                WHERE u.{demo_field} = '{demo_value}'
            """, f"Insert {demo_name} {time_period} analysis")
            
            # GLP1 users in demographic
            execute_with_timing(cursor, f"""
                INSERT INTO tmp_demographic_weight_analysis
                SELECT 
                    'Weight Loss Outcomes' as metric_category,
                    '{time_period}' as time_period,
                    '{demo_name} GLP1 Users' as user_group,
                    COUNT(DISTINCT bw.user_id) as total_users_with_data,
                    ROUND(AVG(bw.baseline_weight_lbs - lw.latest_weight_lbs), 2) as avg_weight_loss_lbs,
                    ROUND(AVG((bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs * 100), 2) as avg_percent_weight_loss,
                    COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.05 THEN bw.user_id END) as users_5_percent_loss,
                    COUNT(DISTINCT CASE WHEN (bw.baseline_weight_lbs - lw.latest_weight_lbs) / bw.baseline_weight_lbs >= 0.10 THEN bw.user_id END) as users_10_percent_loss
                FROM tmp_baseline_weight_{period} bw
                JOIN tmp_latest_weight_{period} lw ON bw.user_id = lw.user_id
                JOIN tmp_amazon_glp1_users_{period} glp ON bw.user_id = glp.user_id
                JOIN users u ON bw.user_id = u.id
                WHERE u.{demo_field} = '{demo_value}'
            """, f"Insert {demo_name} GLP1 {time_period} users analysis")

def create_blood_pressure_analysis(cursor):
    """Create blood pressure analysis for all time periods"""
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
    
    # Insert results for each time period
    for time_period, period in [('All Users', 'all'), ('180 Days', '180')]:
        # All users BP analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_bp_analysis
            SELECT 
                'Blood Pressure Management' as metric_category,
                '{time_period}' as time_period,
                'All Users' as user_group,
                COUNT(DISTINCT bbb.user_id) as total_users_with_data,
                ROUND(AVG(bbb.baseline_systolic), 1) as avg_baseline_systolic,
                ROUND(AVG(bbb.baseline_diastolic), 1) as avg_baseline_diastolic,
                ROUND(AVG(lbb.latest_systolic), 1) as avg_latest_systolic,
                ROUND(AVG(lbb.latest_diastolic), 1) as avg_latest_diastolic,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic), 1) as avg_systolic_change,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic), 1) as avg_diastolic_change,
                ROUND(AVG(DATEDIFF(lbb.latest_bp_date, bbb.baseline_bp_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_blood_pressure_{period} bbb
            JOIN tmp_latest_blood_pressure_{period} lbb ON bbb.user_id = lbb.user_id
        """, f"Insert {time_period} all users BP analysis")
        
        # GLP1 users BP analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_bp_analysis
            SELECT 
                'Blood Pressure Management' as metric_category,
                '{time_period}' as time_period,
                'GLP1 Users' as user_group,
                COUNT(DISTINCT bbb.user_id) as total_users_with_data,
                ROUND(AVG(bbb.baseline_systolic), 1) as avg_baseline_systolic,
                ROUND(AVG(bbb.baseline_diastolic), 1) as avg_baseline_diastolic,
                ROUND(AVG(lbb.latest_systolic), 1) as avg_latest_systolic,
                ROUND(AVG(lbb.latest_diastolic), 1) as avg_latest_diastolic,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic), 1) as avg_systolic_change,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic), 1) as avg_diastolic_change,
                ROUND(AVG(DATEDIFF(lbb.latest_bp_date, bbb.baseline_bp_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_blood_pressure_{period} bbb
            JOIN tmp_latest_blood_pressure_{period} lbb ON bbb.user_id = lbb.user_id
            JOIN tmp_amazon_glp1_users_{period} glp ON bbb.user_id = glp.user_id
        """, f"Insert {time_period} GLP1 users BP analysis")
        
        # No GLP1 users BP analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_bp_analysis
            SELECT 
                'Blood Pressure Management' as metric_category,
                '{time_period}' as time_period,
                'No GLP1 Users' as user_group,
                COUNT(DISTINCT bbb.user_id) as total_users_with_data,
                ROUND(AVG(bbb.baseline_systolic), 1) as avg_baseline_systolic,
                ROUND(AVG(bbb.baseline_diastolic), 1) as avg_baseline_diastolic,
                ROUND(AVG(lbb.latest_systolic), 1) as avg_latest_systolic,
                ROUND(AVG(lbb.latest_diastolic), 1) as avg_latest_diastolic,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic), 1) as avg_systolic_change,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic), 1) as avg_diastolic_change,
                ROUND(AVG(DATEDIFF(lbb.latest_bp_date, bbb.baseline_bp_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_blood_pressure_{period} bbb
            JOIN tmp_latest_blood_pressure_{period} lbb ON bbb.user_id = lbb.user_id
            LEFT JOIN tmp_amazon_glp1_users_{period} glp ON bbb.user_id = glp.user_id
            WHERE glp.user_id IS NULL
        """, f"Insert {time_period} no GLP1 users BP analysis")

def create_hypertension_analysis(cursor):
    """Create comprehensive hypertension analysis for all time periods"""
    print(f"\n🩺 Creating hypertension analysis...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_hypertension_analysis", "Drop hypertension analysis table")
    
    # Create the table structure first
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_hypertension_analysis (
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
            uncontrolled_baseline_users INT,
            users_with_significant_bp_drop INT,
            percent_with_significant_bp_drop DECIMAL(10,2),
            uncontrolled_avg_systolic_change DECIMAL(10,1),
            uncontrolled_avg_diastolic_change DECIMAL(10,1),
            avg_days_between_readings DECIMAL(10,0)
        )
    """, "Create hypertension analysis table structure")
    
    # Insert results for each time period
    for time_period, period in [('All Users', 'all'), ('180 Days', '180')]:
        # All users hypertension analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_hypertension_analysis
            SELECT 
                'Hypertension Management' as metric_category,
                '{time_period}' as time_period,
                'All Users' as user_group,
                COUNT(DISTINCT bbb.user_id) as total_users_with_data,
                ROUND(AVG(bbb.baseline_systolic), 1) as avg_baseline_systolic,
                ROUND(AVG(bbb.baseline_diastolic), 1) as avg_baseline_diastolic,
                ROUND(AVG(lbb.latest_systolic), 1) as avg_latest_systolic,
                ROUND(AVG(lbb.latest_diastolic), 1) as avg_latest_diastolic,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic), 1) as avg_systolic_change,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic), 1) as avg_diastolic_change,
                COUNT(DISTINCT CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.user_id 
                END) as uncontrolled_baseline_users,
                COUNT(DISTINCT CASE 
                    WHEN (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)
                    AND ((bbb.baseline_systolic - lbb.latest_systolic) >= 10 
                         OR (bbb.baseline_diastolic - lbb.latest_diastolic) >= 5)
                    THEN bbb.user_id 
                END) as users_with_significant_bp_drop,
                ROUND((COUNT(DISTINCT CASE 
                    WHEN (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)
                    AND ((bbb.baseline_systolic - lbb.latest_systolic) >= 10 
                         OR (bbb.baseline_diastolic - lbb.latest_diastolic) >= 5)
                    THEN bbb.user_id 
                END) * 100.0 / NULLIF(COUNT(DISTINCT CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.user_id 
                END), 0)), 2) as percent_with_significant_bp_drop,
                ROUND(AVG(CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.baseline_systolic - lbb.latest_systolic 
                END), 1) as uncontrolled_avg_systolic_change,
                ROUND(AVG(CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.baseline_diastolic - lbb.latest_diastolic 
                END), 1) as uncontrolled_avg_diastolic_change,
                ROUND(AVG(DATEDIFF(lbb.latest_bp_date, bbb.baseline_bp_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_blood_pressure_{period} bbb
            JOIN tmp_latest_blood_pressure_{period} lbb ON bbb.user_id = lbb.user_id
            WHERE DATEDIFF(lbb.latest_bp_date, bbb.baseline_bp_date) >= 30
        """, f"Insert {time_period} all users hypertension analysis")
        
        # GLP1 users hypertension analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_hypertension_analysis
            SELECT 
                'Hypertension Management' as metric_category,
                '{time_period}' as time_period,
                'GLP1 Users' as user_group,
                COUNT(DISTINCT bbb.user_id) as total_users_with_data,
                ROUND(AVG(bbb.baseline_systolic), 1) as avg_baseline_systolic,
                ROUND(AVG(bbb.baseline_diastolic), 1) as avg_baseline_diastolic,
                ROUND(AVG(lbb.latest_systolic), 1) as avg_latest_systolic,
                ROUND(AVG(lbb.latest_diastolic), 1) as avg_latest_diastolic,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic), 1) as avg_systolic_change,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic), 1) as avg_diastolic_change,
                COUNT(DISTINCT CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.user_id 
                END) as uncontrolled_baseline_users,
                COUNT(DISTINCT CASE 
                    WHEN (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)
                    AND ((bbb.baseline_systolic - lbb.latest_systolic) >= 10 
                         OR (bbb.baseline_diastolic - lbb.latest_diastolic) >= 5)
                    THEN bbb.user_id 
                END) as users_with_significant_bp_drop,
                ROUND((COUNT(DISTINCT CASE 
                    WHEN (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)
                    AND ((bbb.baseline_systolic - lbb.latest_systolic) >= 10 
                         OR (bbb.baseline_diastolic - lbb.latest_diastolic) >= 5)
                    THEN bbb.user_id 
                END) * 100.0 / NULLIF(COUNT(DISTINCT CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.user_id 
                END), 0)), 2) as percent_with_significant_bp_drop,
                ROUND(AVG(CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.baseline_systolic - lbb.latest_systolic 
                END), 1) as uncontrolled_avg_systolic_change,
                ROUND(AVG(CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.baseline_diastolic - lbb.latest_diastolic 
                END), 1) as uncontrolled_avg_diastolic_change,
                ROUND(AVG(DATEDIFF(lbb.latest_bp_date, bbb.baseline_bp_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_blood_pressure_{period} bbb
            JOIN tmp_latest_blood_pressure_{period} lbb ON bbb.user_id = lbb.user_id
            JOIN tmp_amazon_glp1_users_{period} glp ON bbb.user_id = glp.user_id
        """, f"Insert {time_period} GLP1 users hypertension analysis")
        
        # No GLP1 users hypertension analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_hypertension_analysis
            SELECT 
                'Hypertension Management' as metric_category,
                '{time_period}' as time_period,
                'No GLP1 Users' as user_group,
                COUNT(DISTINCT bbb.user_id) as total_users_with_data,
                ROUND(AVG(bbb.baseline_systolic), 1) as avg_baseline_systolic,
                ROUND(AVG(bbb.baseline_diastolic), 1) as avg_baseline_diastolic,
                ROUND(AVG(lbb.latest_systolic), 1) as avg_latest_systolic,
                ROUND(AVG(lbb.latest_diastolic), 1) as avg_latest_diastolic,
                ROUND(AVG(bbb.baseline_systolic - lbb.latest_systolic), 1) as avg_systolic_change,
                ROUND(AVG(bbb.baseline_diastolic - lbb.latest_diastolic), 1) as avg_diastolic_change,
                COUNT(DISTINCT CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.user_id 
                END) as uncontrolled_baseline_users,
                COUNT(DISTINCT CASE 
                    WHEN (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)
                    AND ((bbb.baseline_systolic - lbb.latest_systolic) >= 10 
                         OR (bbb.baseline_diastolic - lbb.latest_diastolic) >= 5)
                    THEN bbb.user_id 
                END) as users_with_significant_bp_drop,
                ROUND((COUNT(DISTINCT CASE 
                    WHEN (bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90)
                    AND ((bbb.baseline_systolic - lbb.latest_systolic) >= 10 
                         OR (bbb.baseline_diastolic - lbb.latest_diastolic) >= 5)
                    THEN bbb.user_id 
                END) * 100.0 / NULLIF(COUNT(DISTINCT CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.user_id 
                END), 0)), 2) as percent_with_significant_bp_drop,
                ROUND(AVG(CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.baseline_systolic - lbb.latest_systolic 
                END), 1) as uncontrolled_avg_systolic_change,
                ROUND(AVG(CASE 
                    WHEN bbb.baseline_systolic >= 140 OR bbb.baseline_diastolic >= 90 
                    THEN bbb.baseline_diastolic - lbb.latest_diastolic 
                END), 1) as uncontrolled_avg_diastolic_change,
                ROUND(AVG(DATEDIFF(lbb.latest_bp_date, bbb.baseline_bp_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_blood_pressure_{period} bbb
            JOIN tmp_latest_blood_pressure_{period} lbb ON bbb.user_id = lbb.user_id
            LEFT JOIN tmp_amazon_glp1_users_{period} glp ON bbb.user_id = glp.user_id
            WHERE glp.user_id IS NULL
        """, f"Insert {time_period} no GLP1 users hypertension analysis")
        
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
            total_users_with_data INT,
            prediabetic_users INT,
            diabetic_users INT,
            avg_baseline_a1c DECIMAL(10,2),
            avg_latest_a1c DECIMAL(10,2),
            avg_a1c_improvement DECIMAL(10,2),
            prediabetic_avg_improvement DECIMAL(10,2),
            diabetic_avg_improvement DECIMAL(10,2)
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
    
    # Insert results for each demographic group for both All Users and 180 days
    for time_period, period in [('All Users', 'all'), ('180 Days', '180')]:
        for demo_name, demo_value, demo_field in demographics:
            # All users in demographic
            execute_with_timing(cursor, f"""
                INSERT INTO tmp_demographic_a1c_analysis
                SELECT 
                    'A1C Management' as metric_category,
                    '{time_period}' as time_period,
                    '{demo_name}' as user_group,
                    COUNT(DISTINCT ba1c.user_id) as total_users_with_data,
                    COUNT(DISTINCT CASE 
                        WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                        THEN ba1c.user_id 
                    END) as prediabetic_users,
                    COUNT(DISTINCT CASE 
                        WHEN ba1c.baseline_a1c >= 6.5 
                        THEN ba1c.user_id 
                    END) as diabetic_users,
                    ROUND(AVG(ba1c.baseline_a1c), 2) as avg_baseline_a1c,
                    ROUND(AVG(la1c.latest_a1c), 2) as avg_latest_a1c,
                    ROUND(AVG(ba1c.baseline_a1c - la1c.latest_a1c), 2) as avg_a1c_improvement,
                    ROUND(AVG(CASE 
                        WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                        THEN ba1c.baseline_a1c - la1c.latest_a1c 
                    END), 2) as prediabetic_avg_improvement,
                    ROUND(AVG(CASE 
                        WHEN ba1c.baseline_a1c >= 6.5 
                        THEN ba1c.baseline_a1c - la1c.latest_a1c 
                    END), 2) as diabetic_avg_improvement
                FROM tmp_baseline_a1c_{period} ba1c
                JOIN tmp_latest_a1c_{period} la1c ON ba1c.user_id = la1c.user_id
                JOIN users u ON ba1c.user_id = u.id
                WHERE u.{demo_field} = '{demo_value}'
            """, f"Insert {demo_name} {time_period} A1C analysis")
            
            # GLP1 users in demographic
            execute_with_timing(cursor, f"""
                INSERT INTO tmp_demographic_a1c_analysis
                SELECT 
                    'A1C Management' as metric_category,
                    '{time_period}' as time_period,
                    '{demo_name} GLP1 Users' as user_group,
                    COUNT(DISTINCT ba1c.user_id) as total_users_with_data,
                    COUNT(DISTINCT CASE 
                        WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                        THEN ba1c.user_id 
                    END) as prediabetic_users,
                    COUNT(DISTINCT CASE 
                        WHEN ba1c.baseline_a1c >= 6.5 
                        THEN ba1c.user_id 
                    END) as diabetic_users,
                    ROUND(AVG(ba1c.baseline_a1c), 2) as avg_baseline_a1c,
                    ROUND(AVG(la1c.latest_a1c), 2) as avg_latest_a1c,
                    ROUND(AVG(ba1c.baseline_a1c - la1c.latest_a1c), 2) as avg_a1c_improvement,
                    ROUND(AVG(CASE 
                        WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                        THEN ba1c.baseline_a1c - la1c.latest_a1c 
                    END), 2) as prediabetic_avg_improvement,
                    ROUND(AVG(CASE 
                        WHEN ba1c.baseline_a1c >= 6.5 
                        THEN ba1c.baseline_a1c - la1c.latest_a1c 
                    END), 2) as diabetic_avg_improvement
                FROM tmp_baseline_a1c_{period} ba1c
                JOIN tmp_latest_a1c_{period} la1c ON ba1c.user_id = la1c.user_id
                JOIN tmp_amazon_glp1_users_{period} glp ON ba1c.user_id = glp.user_id
                JOIN users u ON ba1c.user_id = u.id
                WHERE u.{demo_field} = '{demo_value}'
            """, f"Insert {demo_name} GLP1 {time_period} A1C users analysis")

def create_a1c_analysis(cursor):
    """Create comprehensive A1C analysis for all time periods"""
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
            avg_baseline_a1c DECIMAL(10,2),
            avg_latest_a1c DECIMAL(10,2),
            avg_a1c_improvement DECIMAL(10,2),
            prediabetic_avg_improvement DECIMAL(10,2),
            diabetic_avg_improvement DECIMAL(10,2),
            avg_days_between_readings DECIMAL(10,0)
        )
    """, "Create A1C analysis table structure")
    
    # Insert results for each time period
    for time_period, period in [('All Users', 'all'), ('180 Days', '180')]:
        # All users A1C analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_a1c_analysis
            SELECT 
                'A1C Management' as metric_category,
                '{time_period}' as time_period,
                'All Users' as user_group,
                COUNT(DISTINCT ba1c.user_id) as total_users_with_data,
                COUNT(DISTINCT CASE 
                    WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                    THEN ba1c.user_id 
                END) as prediabetic_users,
                COUNT(DISTINCT CASE 
                    WHEN ba1c.baseline_a1c >= 6.5 
                    THEN ba1c.user_id 
                END) as diabetic_users,
                ROUND(AVG(ba1c.baseline_a1c), 2) as avg_baseline_a1c,
                ROUND(AVG(la1c.latest_a1c), 2) as avg_latest_a1c,
                ROUND(AVG(ba1c.baseline_a1c - la1c.latest_a1c), 2) as avg_a1c_improvement,
                ROUND(AVG(CASE 
                    WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                    THEN ba1c.baseline_a1c - la1c.latest_a1c 
                END), 2) as prediabetic_avg_improvement,
                ROUND(AVG(CASE 
                    WHEN ba1c.baseline_a1c >= 6.5 
                    THEN ba1c.baseline_a1c - la1c.latest_a1c 
                END), 2) as diabetic_avg_improvement,
                ROUND(AVG(DATEDIFF(la1c.latest_a1c_date, ba1c.baseline_a1c_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_a1c_{period} ba1c
            JOIN tmp_latest_a1c_{period} la1c ON ba1c.user_id = la1c.user_id
        """, f"Insert {time_period} all users A1C analysis")
        
        # GLP1 users A1C analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_a1c_analysis
            SELECT 
                'A1C Management' as metric_category,
                '{time_period}' as time_period,
                'GLP1 Users' as user_group,
                COUNT(DISTINCT ba1c.user_id) as total_users_with_data,
                COUNT(DISTINCT CASE 
                    WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                    THEN ba1c.user_id 
                END) as prediabetic_users,
                COUNT(DISTINCT CASE 
                    WHEN ba1c.baseline_a1c >= 6.5 
                    THEN ba1c.user_id 
                END) as diabetic_users,
                ROUND(AVG(ba1c.baseline_a1c), 2) as avg_baseline_a1c,
                ROUND(AVG(la1c.latest_a1c), 2) as avg_latest_a1c,
                ROUND(AVG(ba1c.baseline_a1c - la1c.latest_a1c), 2) as avg_a1c_improvement,
                ROUND(AVG(CASE 
                    WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                    THEN ba1c.baseline_a1c - la1c.latest_a1c 
                END), 2) as prediabetic_avg_improvement,
                ROUND(AVG(CASE 
                    WHEN ba1c.baseline_a1c >= 6.5 
                    THEN ba1c.baseline_a1c - la1c.latest_a1c 
                END), 2) as diabetic_avg_improvement,
                ROUND(AVG(DATEDIFF(la1c.latest_a1c_date, ba1c.baseline_a1c_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_a1c_{period} ba1c
            JOIN tmp_latest_a1c_{period} la1c ON ba1c.user_id = la1c.user_id
            JOIN tmp_amazon_glp1_users_{period} glp ON ba1c.user_id = glp.user_id
        """, f"Insert {time_period} GLP1 users A1C analysis")
        
        # No GLP1 users A1C analysis
        execute_with_timing(cursor, f"""
            INSERT INTO tmp_a1c_analysis
            SELECT 
                'A1C Management' as metric_category,
                '{time_period}' as time_period,
                'No GLP1 Users' as user_group,
                COUNT(DISTINCT ba1c.user_id) as total_users_with_data,
                COUNT(DISTINCT CASE 
                    WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                    THEN ba1c.user_id 
                END) as prediabetic_users,
                COUNT(DISTINCT CASE 
                    WHEN ba1c.baseline_a1c >= 6.5 
                    THEN ba1c.user_id 
                END) as diabetic_users,
                ROUND(AVG(ba1c.baseline_a1c), 2) as avg_baseline_a1c,
                ROUND(AVG(la1c.latest_a1c), 2) as avg_latest_a1c,
                ROUND(AVG(ba1c.baseline_a1c - la1c.latest_a1c), 2) as avg_a1c_improvement,
                ROUND(AVG(CASE 
                    WHEN ba1c.baseline_a1c >= 5.7 AND ba1c.baseline_a1c < 6.5 
                    THEN ba1c.baseline_a1c - la1c.latest_a1c 
                END), 2) as prediabetic_avg_improvement,
                ROUND(AVG(CASE 
                    WHEN ba1c.baseline_a1c >= 6.5 
                    THEN ba1c.baseline_a1c - la1c.latest_a1c 
                END), 2) as diabetic_avg_improvement,
                ROUND(AVG(DATEDIFF(la1c.latest_a1c_date, ba1c.baseline_a1c_date)), 0) as avg_days_between_readings
            FROM tmp_baseline_a1c_{period} ba1c
            JOIN tmp_latest_a1c_{period} la1c ON ba1c.user_id = la1c.user_id
            LEFT JOIN tmp_amazon_glp1_users_{period} glp ON ba1c.user_id = glp.user_id
            WHERE glp.user_id IS NULL
        """, f"Insert {time_period} no GLP1 users A1C analysis")

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

def main_amazon_analysis(end_date='2025-09-30'):
    """Main function to run Amazon QBR analysis"""
    print(f"🚀 Starting Amazon QBR Analysis (as of {end_date})")
    
    with connect_to_db() as conn:
        with conn.cursor() as cursor:
            try:
                # Create base tables
                create_amazon_user_tables(cursor, end_date=end_date)
                create_amazon_glp1_tables(cursor, end_date=end_date)
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
                
                print(f"\n✅ Amazon QBR Analysis Complete!")
                
            finally:
                # Cleanup
                cleanup_tables = [
                    'tmp_amazon_users_all', 'tmp_amazon_users_180',
                    'tmp_amazon_glp1_users_all', 'tmp_amazon_glp1_users_180',
                    'tmp_baseline_weight_all', 'tmp_latest_weight_all',
                    'tmp_baseline_weight_180', 'tmp_latest_weight_180',
                    'tmp_baseline_blood_pressure_all', 'tmp_latest_blood_pressure_all',
                    'tmp_baseline_blood_pressure_180', 'tmp_latest_blood_pressure_180',
                    'tmp_baseline_a1c_all', 'tmp_latest_a1c_all',
                    'tmp_baseline_a1c_180', 'tmp_latest_a1c_180',
                    'tmp_weight_loss_analysis', 'tmp_demographic_weight_analysis', 'tmp_bp_analysis',
                    'tmp_hypertension_analysis', 'tmp_a1c_analysis', 'tmp_demographic_a1c_analysis'
                ]
                for table in cleanup_tables:
                    execute_with_timing(cursor, f"DROP TEMPORARY TABLE IF EXISTS {table}", f"Cleanup {table}")
                    
if __name__ == "__main__":
    main_amazon_analysis()