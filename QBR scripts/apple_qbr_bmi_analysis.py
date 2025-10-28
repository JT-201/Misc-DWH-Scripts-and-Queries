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

def create_apple_users_table(cursor, partner='Apple', end_date='2025-10-01'):
    """Create temporary table for partner users"""
    print(f"\n🍎 Creating {partner} users table (active on {end_date})...")
    
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_apple_users", "Drop partner users table")
    
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_apple_users AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name = '{partner}'
        AND s.status = 'ACTIVE';
    """, f"Create {partner} users table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_apple_users_user_id ON tmp_apple_users(user_id)", "Index partner users table")

def create_bmi_tables(cursor, start_date='2025-01-01', end_date='2025-10-01'):
    """Create baseline and latest BMI tables"""
    print(f"\n📊 Creating BMI tables (filtering for {start_date} to {end_date})...")
    
    # Create baseline BMI table
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
            JOIN tmp_apple_users au ON bv.user_id = au.user_id
            WHERE bv.value IS NOT NULL
              AND bv.value <= 100
              AND bv.effective_date >= '{start_date}'
              AND bv.effective_date <= '{end_date}'
        )
        SELECT user_id, bmi as baseline_bmi, effective_date as baseline_bmi_date
        FROM ranked_bmi WHERE rn = 1
    """, "Create baseline BMI table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_baseline_bmi_user_id ON tmp_baseline_bmi(user_id)", "Index baseline BMI table")
    
    # Create latest BMI table
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
            JOIN tmp_apple_users au ON bv.user_id = au.user_id
            WHERE bv.value IS NOT NULL
              AND bv.value <= 100
              AND bv.effective_date >= '{start_date}'
              AND bv.effective_date <= '{end_date}'
        )
        SELECT user_id, bmi as latest_bmi, effective_date as latest_bmi_date
        FROM ranked_bmi WHERE rn = 1
    """, "Create latest BMI table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_latest_bmi_user_id ON tmp_latest_bmi(user_id)", "Index latest BMI table")

def analyze_bmi_outcomes(cursor):
    """Analyze BMI outcomes"""
    print(f"\n📈 Analyzing BMI outcomes...")
    
    query = """
        SELECT 
            'BMI Analysis' as metric_category,
            
            -- Overall metrics
            COUNT(DISTINCT au.user_id) as total_users,
            
            -- BMI baseline and current averages
            ROUND(AVG(bb.baseline_bmi), 2) as bmi_baseline_avg,
            ROUND(AVG(lb.latest_bmi), 2) as bmi_current_avg,
            
            -- Sample size (users with both baseline and latest)
            COUNT(CASE WHEN bb.baseline_bmi IS NOT NULL AND lb.latest_bmi IS NOT NULL THEN 1 END) as bmi_sample_size,
            
            -- BMI change
            ROUND(AVG(bb.baseline_bmi - lb.latest_bmi), 2) as bmi_delta,
            ROUND(MIN(bb.baseline_bmi - lb.latest_bmi), 2) as bmi_delta_min,
            ROUND(MAX(bb.baseline_bmi - lb.latest_bmi), 2) as bmi_delta_max,
            
            -- Percentage change
            ROUND(AVG((bb.baseline_bmi - lb.latest_bmi) / bb.baseline_bmi * 100), 2) as bmi_pct_change,
            
            -- Users with BMI improvement
            COUNT(CASE WHEN (bb.baseline_bmi - lb.latest_bmi) > 0 THEN 1 END) as users_with_improvement,
            ROUND(COUNT(CASE WHEN (bb.baseline_bmi - lb.latest_bmi) > 0 THEN 1 END) * 100.0 / 
                  COUNT(CASE WHEN bb.baseline_bmi IS NOT NULL AND lb.latest_bmi IS NOT NULL THEN 1 END), 2) as pct_users_with_improvement,
            
            -- Average days between measurements
            ROUND(AVG(DATEDIFF(lb.latest_bmi_date, bb.baseline_bmi_date)), 0) as avg_days_between_readings
            
        FROM tmp_apple_users au
        LEFT JOIN tmp_baseline_bmi bb ON au.user_id = bb.user_id
        LEFT JOIN tmp_latest_bmi lb ON au.user_id = lb.user_id
    """
    
    cursor.execute(query)
    return cursor.fetchall()

def main(partner='Apple', start_date='2025-01-01', end_date='2025-10-01'):
    """Main execution function for BMI analysis"""
    
    script_start_time = time.time()
    
    try:
        print(f"🔗 Connecting to database...")
        print(f"📊 Configuration:")
        print(f"  🏢 Partner: {partner}")
        print(f"  📅 Analysis Period: {start_date} to {end_date}")
        
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        
        # Create partner users table
        create_apple_users_table(cursor, partner, end_date)
        
        # Create BMI tables
        create_bmi_tables(cursor, start_date, end_date)
        
        # Analyze BMI outcomes
        results = analyze_bmi_outcomes(cursor)
        
        # Export results
        if results:
            output_file = f'bmi_analysis_{partner}_{start_date}_to_{end_date}.csv'
            
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = results[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            
            print(f"\n✅ Results exported to: {output_file}")
            print(f"📈 Total records: {len(results)}")
            
            # Print summary
            print(f"\n📋 BMI Analysis Summary:")
            for result in results:
                print(f"  Total Users: {result['total_users']}")
                print(f"  Sample Size: {result['bmi_sample_size']}")
                print(f"  Baseline BMI: {result['bmi_baseline_avg']}")
                print(f"  Current BMI: {result['bmi_current_avg']}")
                print(f"  BMI Change: {result['bmi_delta']}")
                print(f"  Users with Improvement: {result['users_with_improvement']} ({result['pct_users_with_improvement']}%)")
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        cleanup_tables = ['tmp_apple_users', 'tmp_baseline_bmi', 'tmp_latest_bmi']
        
        try:
            for table in cleanup_tables:
                cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {table}")
        except:
            pass
        
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        
        total_duration = time.time() - script_start_time
        print(f"\n🏁 TOTAL RUNTIME: {total_duration:.2f}s")

if __name__ == "__main__":
    # Run with default parameters
    main()
    
    # Or customize:
    # main(partner='Apple', start_date='2025-01-01', end_date='2025-12-31')