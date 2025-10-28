import mysql.connector
import pandas as pd
import csv
import time
from config import get_db_config
from datetime import datetime

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

def create_required_temp_tables(cursor):
    """Create only the temp tables needed for engagement metrics"""
    
    print("🚀 Creating required temporary tables...")
    total_start_time = time.time()
    
    # # 1. Apple + Amazon Users
    # print("\n📊 Creating Apple + Amazon users table:")
    # execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_apple_users", "Drop Apple users table")
    # execute_with_timing(cursor, """
    #     CREATE TEMPORARY TABLE tmp_apple_users AS
    #     SELECT DISTINCT 
    #         bus.user_id,
    #         bus.partner
    #     FROM billable_user_statuses bus
    #     WHERE bus.partner IN ('Apple') AND bus.subscription_status = 'ACTIVE'
    # """, "Create Apple users table")
    # execute_with_timing(cursor, "CREATE INDEX idx_apple_users_user_id ON tmp_apple_users(user_id)", "Index Apple users table")
    
    # execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users", "Drop Amazon users table")
    # execute_with_timing(cursor, """
    #     CREATE TEMPORARY TABLE tmp_amazon_users AS
    #     SELECT DISTINCT 
    #         bus.user_id,
    #         bus.partner
    #     FROM billable_user_statuses bus
    #     WHERE bus.partner IN ('Amazon') AND bus.subscription_status = 'ACTIVE'
    # """, "Create Amazon users table")
    # execute_with_timing(cursor, "CREATE INDEX idx_amazon_users_user_id ON tmp_amazon_users(user_id)", "Index Amazon users table")
    
    # execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_apple_and_amazon_users", "Drop Apple+Amazon union table")
    # execute_with_timing(cursor, """
    #     CREATE TEMPORARY TABLE tmp_apple_and_amazon_users AS
    #     SELECT user_id FROM tmp_apple_users
    #     UNION
    #     SELECT user_id FROM tmp_amazon_users
    # """, "Create Apple+Amazon union table")
    # execute_with_timing(cursor, "CREATE INDEX idx_apple_amazon_user_id ON tmp_apple_and_amazon_users(user_id)", "Index Apple+Amazon union table")
    
    # 2. Current Year Quarters Retention Users (Engaged at least once per quarter in the current year)
    current_year = datetime.now().year

    print(f"\n📊 Creating current-year quarters retention table (active in all quarters, never medically ineligible or cancelled in {current_year}):")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_current_year_quarters_retention_users", "Drop current-year quarters retention table")
    execute_with_timing(cursor, f"""
        CREATE TEMPORARY TABLE tmp_current_year_quarters_retention_users AS
        WITH user_quarter_activity AS (
            SELECT 
                s.user_id,
                QUARTER(bus.created_at) AS quarter
            FROM subscriptions s
            JOIN billable_user_statuses bus ON s.user_id = bus.user_id
            WHERE bus.partner IN ('Apple', 'Amazon')
              AND s.status = 'Active'
              AND YEAR(bus.created_at) = {current_year}
              -- Exclude users who EVER had medical-ineligible or cancelled status in the current year
              AND NOT EXISTS (
                  SELECT 1 
                  FROM billable_user_statuses bus2
                  WHERE bus2.user_id = s.user_id
                    AND YEAR(bus2.created_at) = {current_year}
                    AND (bus2.user_status = 'medical_ineligible' 
                         OR bus2.subscription_status = 'Cancelled')
              )
            GROUP BY s.user_id, QUARTER(bus.created_at)
        ),
        quarters_per_user AS (
            SELECT 
                user_id,
                COUNT(DISTINCT quarter) AS active_quarters
            FROM user_quarter_activity
            GROUP BY user_id
        )
        SELECT 
            user_id,
            active_quarters AS engaged_quarters
        FROM quarters_per_user
        WHERE active_quarters = 4
    """, "Create current-year quarters retention table")
    execute_with_timing(cursor, "CREATE INDEX idx_current_year_quarters_retention_user_id ON tmp_current_year_quarters_retention_users(user_id)", "Index current-year quarters retention table")
    total_duration = time.time() - total_start_time
    print(f"\n🎉 Required temporary tables created in {total_duration:.2f}s")

def create_engagement_metrics(cursor):
    """Create engagement metric tables for care team interactions, module completion, and consultations"""
    
    print("\n🤝 Creating engagement metrics...")
    engagement_start_time = time.time()
    
    # Step 1: Average care team interactions per month (UPDATED to use current year quarters retention)
    print("\n💬 Step 1: Care team interactions per month")
    execute_with_timing(cursor, "DROP TEMPORARY TABLE IF EXISTS tmp_avg_care_team_interactions_per_user", "Drop care team interactions table")
    
    execute_with_timing(cursor, """
        CREATE TEMPORARY TABLE tmp_avg_care_team_interactions_per_user AS
        SELECT 
            cyqru.user_id,
            COUNT(DISTINCT ba.id) as total_interactions,
            cyqru.engaged_quarters
        FROM tmp_current_year_quarters_retention_users cyqru
        LEFT JOIN billable_activities ba ON cyqru.user_id = ba.user_id
            AND ba.type IN ('TEXT_MESSAGE_CARE_ONLY', 'VOICE_MESSAGE_CARE_ONLY', 'VIDEO_CALL_COMPLETED', 'COMPLETED_CONSULTATION', 'QUESTIONNAIRE_ANSWERED')
        GROUP BY cyqru.user_id, cyqru.engaged_quarters
    """, "Create care team interactions table")
    
    execute_with_timing(cursor, "CREATE INDEX idx_care_team_interactions_user_id ON tmp_avg_care_team_interactions_per_user(user_id)", "Index care team interactions table")
    
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
    """, "Create module completion table")
    
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
    """Get engagement metrics for a cohort (UPDATED to use current year quarters retention)"""
    # If the cohort table IS the retention table, don't join to it again
    if cohort_table == 'tmp_current_year_quarters_retention_users':
        return f"""
            SELECT 
                '{cohort_name}' as cohort,
                COUNT(DISTINCT ct.user_id) as total_users,
                COUNT(DISTINCT ct.user_id) as users_with_quarterly_retention,
                
                -- Care team interaction metrics
                ROUND(AVG(IFNULL(ctm.total_interactions, 0)), 2) as avg_care_team_interactions,
                COUNT(CASE WHEN ctm.total_interactions IS NOT NULL THEN 1 END) as care_team_interactions_n,
                
                -- Module completion metrics  
                ROUND(AVG(IFNULL(mc.total_modules_completed, 0)), 2) as avg_modules_completed,
                COUNT(CASE WHEN mc.total_modules_completed IS NOT NULL THEN 1 END) as modules_completion_n,
                ROUND(SUM(CASE WHEN mc.completed_all_modules THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_completed_all_modules,
                SUM(CASE WHEN mc.completed_all_modules THEN 1 ELSE 0 END) as completed_all_modules_n,
                
                -- Physician consultation metrics
                ROUND(AVG(IFNULL(cnoc.completed_consultations, 0)), 2) as avg_completed_consultations,
                COUNT(CASE WHEN cnoc.completed_consultations IS NOT NULL THEN 1 END) as consultations_n
                
            FROM {cohort_table} ct
            LEFT JOIN tmp_avg_care_team_interactions_per_user ctm ON ct.user_id = ctm.user_id
            LEFT JOIN tmp_module_completion mc ON ct.user_id = mc.user_id  
            LEFT JOIN tmp_completed_non_orderonly_consultations cnoc ON ct.user_id = cnoc.user_id
        """
    else:
        # For other cohort tables, join to the retention table
        return f"""
            SELECT 
                '{cohort_name}' as cohort,
                COUNT(DISTINCT ct.user_id) as total_users,
                COUNT(DISTINCT cyqru.user_id) as users_with_quarterly_retention,
                
                -- Care team interaction metrics
                ROUND(AVG(IFNULL(ctm.total_interactions, 0)), 2) as avg_care_team_interactions,
                COUNT(CASE WHEN ctm.total_interactions IS NOT NULL THEN 1 END) as care_team_interactions_n,
                
                -- Module completion metrics  
                ROUND(AVG(IFNULL(mc.total_modules_completed, 0)), 2) as avg_modules_completed,
                COUNT(CASE WHEN mc.total_modules_completed IS NOT NULL THEN 1 END) as modules_completion_n,
                ROUND(SUM(CASE WHEN mc.completed_all_modules THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pct_completed_all_modules,
                SUM(CASE WHEN mc.completed_all_modules THEN 1 ELSE 0 END) as completed_all_modules_n,
                
                -- Physician consultation metrics
                ROUND(AVG(IFNULL(cnoc.completed_consultations, 0)), 2) as avg_completed_consultations,
                COUNT(CASE WHEN cnoc.completed_consultations IS NOT NULL THEN 1 END) as consultations_n
                
            FROM {cohort_table} ct
            LEFT JOIN tmp_current_year_quarters_retention_users cyqru ON ct.user_id = cyqru.user_id
            LEFT JOIN tmp_avg_care_team_interactions_per_user ctm ON ct.user_id = ctm.user_id
            LEFT JOIN tmp_module_completion mc ON ct.user_id = mc.user_id  
            LEFT JOIN tmp_completed_non_orderonly_consultations cnoc ON ct.user_id = cnoc.user_id
        """

def main():
    """Main execution for engagement metrics only"""
    
    # Define your cohorts - adjust as needed
    cohorts = {
        'Apple+Amazon': 'tmp_current_year_quarters_retention_users'
    }
    
    script_start_time = time.time()
    
    try:
        print("🔗 Connecting to database...")
        conn = connect_to_db()
        cursor = conn.cursor(dictionary=True)
        
        # Create required temp tables
        create_required_temp_tables(cursor)
        
        # Create engagement metrics
        create_engagement_metrics(cursor)
        
        # Process cohorts
        print("\n📊 Processing cohorts for engagement metrics:")
        all_engagement_results = []
        
        for cohort_name, cohort_table in cohorts.items():
            print(f"\n  🎯 Processing: {cohort_name}")
            
            engagement_query = get_engagement_metrics_query(cohort_table, cohort_name)
            cursor.execute(engagement_query)
            engagement_results = cursor.fetchall()
            
            if engagement_results:
                all_engagement_results.extend(engagement_results)
                print(f"    ✅ {cohort_name}: Engagement metrics retrieved")
        
        # Export results
        if all_engagement_results:
            engagement_file = 'engagement_metrics_only.csv'
            engagement_fieldnames = list(all_engagement_results[0].keys())
            
            with open(engagement_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=engagement_fieldnames)
                writer.writeheader()
                writer.writerows(all_engagement_results)
            
            print(f"\n📄 Export Results:")
            print(f"  ✅ Engagement metrics exported to {engagement_file}")
            print(f"  🤝 Total rows: {len(all_engagement_results)}")
            
            # Print detailed breakdown
            print(f"\n🤝 Engagement Metrics by Cohort:")
            for result in all_engagement_results:
                cohort = result['cohort']
                interactions = result['avg_care_team_interactions']
                modules = result['avg_modules_completed']
                pct_all_modules = result['pct_completed_all_modules']
                consultations = result['avg_completed_consultations']
                print(f"  {cohort}:")
                print(f"    Total users: {result['total_users']}")
                print(f"    Users with quarterly retention: {result['users_with_quarterly_retention']}")
                print(f"    Avg care team interactions: {interactions}")
                print(f"    Avg modules completed: {modules}")
                print(f"    % completed all modules: {pct_all_modules}%")
                print(f"    Avg consultations: {consultations}")
        
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        raise
    finally:
        # Cleanup
        cleanup_tables = [
            'tmp_apple_users',
            'tmp_amazon_users',
            'tmp_apple_and_amazon_users',
            'tmp_current_year_quarters_retention_users',
            'tmp_avg_care_team_interactions_per_user',
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
        
        total_duration = time.time() - script_start_time
        print(f"\n🏁 TOTAL RUNTIME: {total_duration:.2f}s")

if __name__ == "__main__":
    main()