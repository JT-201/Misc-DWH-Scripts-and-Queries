import mysql.connector
import time
import pandas as pd
import numpy as np
from config import get_db_config

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
PARTNER_NAME = "SmithRx"
ANALYSIS_START = "2025-01-01"
ANALYSIS_END = "2025-12-31"

# ------------------------------------------------------------
# DATABASE HELPERS
# ------------------------------------------------------------
def connect_to_db():
    config = get_db_config()
    return mysql.connector.connect(**config)

def get_data(conn, query, description):
    start_time = time.time()
    print(f"  📥 Fetching {description}...")
    try:
        df = pd.read_sql(query, conn)
        duration = time.time() - start_time
        print(f"    ⏱️  {description}: {len(df):,} rows in {duration:.2f}s")
        return df
    except Exception as e:
        print(f"    ❌ Error fetching {description}: {e}")
        return pd.DataFrame()

# ------------------------------------------------------------
# LOGIC FUNCTIONS
# ------------------------------------------------------------

def get_partner_users(conn, partner, end_date):
    """Get all active users"""
    query = f"""
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_payers bus ON bus.user_id = s.user_id
        WHERE bus.name = '{partner}'
        AND s.status = 'ACTIVE'
    """
    return get_data(conn, query, f"{partner} Active Users")

def calculate_6month_retention(conn, df_users, partner, end_date):
    """Python version of 6-month consecutive engagement logic"""
    print(f"\n🏥 Calculating 6-month retention cohort...")
    
    # optimize by filtering only relevant users in SQL
    query = f"""
        SELECT bus.user_id, DATE_FORMAT(bus.date, '%Y-%m-01') as activity_month
        FROM billable_user_statuses bus
        JOIN partner_payers pe ON pe.user_id = bus.user_id
        WHERE pe.name = '{partner}'
        AND bus.is_billable = 1
        AND bus.date <= '{end_date}'
        GROUP BY bus.user_id, DATE_FORMAT(bus.date, '%Y-%m-01')
    """
    df_activity = get_data(conn, query, "Billable Activity")
    
    if df_activity.empty:
        return pd.DataFrame(columns=['user_id', 'start_date'])

    # Consecutive logic
    df_activity['month_dt'] = pd.to_datetime(df_activity['activity_month'])
    df_activity = df_activity.sort_values(['user_id', 'month_dt'])
    df_activity['month_idx'] = df_activity['month_dt'].dt.year * 12 + df_activity['month_dt'].dt.month
    df_activity['group_id'] = df_activity['month_idx'] - df_activity.groupby('user_id').cumcount()
    
    streak_counts = df_activity.groupby(['user_id', 'group_id']).size().reset_index(name='consecutive_months')
    retained_ids = streak_counts[streak_counts['consecutive_months'] >= 6]['user_id'].unique()
    
    df_retained = df_users[df_users['user_id'].isin(retained_ids)].copy()
    print(f"  📊 Retention Rate: {(len(df_retained)/len(df_users)*100):.1f}% ({len(df_retained)}/{len(df_users)})")
    return df_retained

def get_clinical_data(conn, table, val_col, date_col, partner, max_date):
    query = f"""
        SELECT d.user_id, d.{val_col} as value, d.{date_col} as effective_date
        FROM {table} d
        JOIN partner_payers pe ON pe.user_id = d.user_id
        WHERE pe.name = '{partner}' AND d.{val_col} IS NOT NULL AND d.{date_col} <= '{max_date}'
    """
    df = get_data(conn, query, table)
    if not df.empty: df['effective_date'] = pd.to_datetime(df['effective_date'])
    return df

def get_blood_pressure(conn, partner, max_date):
    query = f"""
        SELECT d.user_id, d.systolic, d.diastolic, d.effective_date
        FROM blood_pressure_values d
        JOIN partner_payers pe ON pe.user_id = d.user_id
        WHERE pe.name = '{partner}' AND d.systolic IS NOT NULL AND d.effective_date <= '{max_date}'
    """
    df = get_data(conn, query, "blood_pressure_values")
    if not df.empty: df['effective_date'] = pd.to_datetime(df['effective_date'])
    return df

def get_glp1_data(conn, partner, end_date):
    query = f"""
        SELECT p.patient_user_id as user_id, p.prescribed_at, p.days_of_supply, p.total_refills
        FROM prescriptions p
        JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
        JOIN medications m ON m.id = ndcs.medication_id
        JOIN partner_payers pe ON pe.user_id = p.patient_user_id
        WHERE pe.name = '{partner}'
        AND (m.name LIKE '%Wegovy%' OR m.name LIKE '%Zepbound%' OR m.name LIKE '%Ozempic%' OR m.name LIKE '%Mounjaro%')
        AND p.prescribed_at <= '{end_date}'
    """
    df = get_data(conn, query, "GLP1 Rx")
    if not df.empty: df['prescribed_at'] = pd.to_datetime(df['prescribed_at'])
    return df

def get_questionnaire_flag(conn, question_id, answer_val):
    query = f"""
        SELECT DISTINCT user_id 
        FROM questionnaire_records 
        WHERE question_id = '{question_id}' AND answer_value = {answer_val}
    """
    return get_data(conn, query, "Questionnaire (GLP1 Disc)")

def process_metric(df_cohort, df_data, metric_name, start_date, end_date):
    """Calculate Baseline (Start-30d) and Latest (Analysis Period)"""
    if df_data.empty: return df_cohort
    
    df_m = df_data.merge(df_cohort[['user_id', 'start_date']], on='user_id', how='inner')
    
    # Baseline: [start_date - 30, end_date] -> Take First
    df_m['base_min'] = df_m['start_date'] - pd.Timedelta(days=30)
    mask_base = (df_m['effective_date'] >= df_m['base_min']) & (df_m['effective_date'] <= pd.to_datetime(end_date))
    base = df_m[mask_base].sort_values('effective_date').groupby('user_id').first().reset_index()
    
    # Latest: [analysis_start, analysis_end] -> Take Last
    mask_curr = (df_m['effective_date'] >= pd.to_datetime(start_date)) & (df_m['effective_date'] <= pd.to_datetime(end_date))
    curr = df_m[mask_curr].sort_values('effective_date', ascending=False).groupby('user_id').first().reset_index()
    
    # Rename and Merge
    base = base[['user_id', 'value', 'effective_date']].rename(columns={'value': f'base_{metric_name}', 'effective_date': f'base_{metric_name}_date'})
    curr = curr[['user_id', 'value', 'effective_date']].rename(columns={'value': f'curr_{metric_name}', 'effective_date': f'curr_{metric_name}_date'})
    
    return df_cohort.merge(base, on='user_id', how='left').merge(curr, on='user_id', how='left')

def process_bp(df_cohort, df_bp, start_date, end_date):
    """Special handling for BP (Systolic/Diastolic)"""
    if df_bp.empty: return df_cohort
    
    df_m = df_bp.merge(df_cohort[['user_id', 'start_date']], on='user_id', how='inner')
    
    # Baseline
    df_m['base_min'] = df_m['start_date'] - pd.Timedelta(days=30)
    mask_base = (df_m['effective_date'] >= df_m['base_min']) & (df_m['effective_date'] <= pd.to_datetime(end_date))
    base = df_m[mask_base].sort_values('effective_date').groupby('user_id').first().reset_index()
    
    # Latest
    mask_curr = (df_m['effective_date'] >= pd.to_datetime(start_date)) & (df_m['effective_date'] <= pd.to_datetime(end_date))
    curr = df_m[mask_curr].sort_values('effective_date', ascending=False).groupby('user_id').first().reset_index()
    
    cols = ['user_id', 'systolic', 'diastolic', 'effective_date']
    base = base[cols].rename(columns={'systolic': 'base_sys', 'diastolic': 'base_dia', 'effective_date': 'base_bp_date'})
    curr = curr[cols].rename(columns={'systolic': 'curr_sys', 'diastolic': 'curr_dia', 'effective_date': 'curr_bp_date'})
    
    return df_cohort.merge(base, on='user_id', how='left').merge(curr, on='user_id', how='left')

# ------------------------------------------------------------
# MAIN ANALYSIS
# ------------------------------------------------------------

def generate_summary_stats(df, label):
    """Generate the exact rows required for QBR Excel"""
    if df.empty: return []
    
    # 1. Weight Stats
    w_df = df.dropna(subset=['base_weight', 'curr_weight'])
    # Ensure 30 days gap
    w_df = w_df[(w_df['curr_weight_date'] - w_df['base_weight_date']).dt.days >= 30]
    
    w_stats = {
        'weight_baseline_avg': w_df['base_weight'].mean(),
        'weight_current_avg': w_df['curr_weight'].mean(),
        'weight_sample_size': len(w_df),
        'weight_loss_pct': w_df['weight_loss_pct'].mean(),
        'weight_loss_lbs': w_df['weight_loss_lbs'].mean(),
        'pct_lost_5pct': (len(w_df[w_df['weight_loss_pct'] >= 5])/len(w_df)*100) if len(w_df) > 0 else 0,
        'pct_lost_10pct': (len(w_df[w_df['weight_loss_pct'] >= 10])/len(w_df)*100) if len(w_df) > 0 else 0
    }
    
    # 2. BMI Stats
    b_df = df.dropna(subset=['base_bmi', 'curr_bmi'])
    b_df = b_df[(b_df['curr_bmi_date'] - b_df['base_bmi_date']).dt.days >= 30]
    
    b_stats = {
        'bmi_baseline_avg': b_df['base_bmi'].mean(),
        'bmi_current_avg': b_df['curr_bmi'].mean(),
        'bmi_sample_size': len(b_df),
        'bmi_delta': (b_df['base_bmi'] - b_df['curr_bmi']).mean()
    }
    
    # 3. A1C Stats (General)
    a_df = df.dropna(subset=['base_a1c', 'curr_a1c'])
    a_df = a_df[(a_df['curr_a1c_date'] - a_df['base_a1c_date']).dt.days >= 30]
    
    # 4. A1C Stats (Diabetic: Base >= 6.5)
    d_df = a_df[a_df['base_a1c'] >= 6.5]
    
    a_stats = {
        'a1c_baseline_avg': a_df['base_a1c'].mean(),
        'a1c_current_avg': a_df['curr_a1c'].mean(),
        'a1c_sample_size': len(a_df),
        'a1c_delta': (a_df['base_a1c'] - a_df['curr_a1c']).mean(),
        # Diabetic specific
        'a1c_6_5_plus_baseline_avg': d_df['base_a1c'].mean(),
        'a1c_6_5_plus_current_avg': d_df['curr_a1c'].mean(),
        'a1c_6_5_plus_sample_size': len(d_df),
        'a1c_6_5_plus_delta': (d_df['base_a1c'] - d_df['curr_a1c']).mean()
    }
    
    # 5. BP Stats (Normal & Hypertensive)
    bp_df = df.dropna(subset=['base_sys', 'curr_sys'])
    bp_df = bp_df[(bp_df['curr_bp_date'] - bp_df['base_bp_date']).dt.days >= 30]
    
    # Hypertensive (Sys>=140 OR Dia>=90)
    htn_df = bp_df[(bp_df['base_sys'] >= 140) | (bp_df['base_dia'] >= 90)]
    
    bp_stats = {
        # Normal
        'bp_normal_baseline_systolic_avg': bp_df['base_sys'].mean(),
        'bp_normal_baseline_diastolic_avg': bp_df['base_dia'].mean(),
        'bp_normal_latest_systolic_avg': bp_df['curr_sys'].mean(),
        'bp_normal_latest_diastolic_avg': bp_df['curr_dia'].mean(),
        'bp_normal_sample_size': len(bp_df),
        'bp_normal_systolic_delta': (bp_df['base_sys'] - bp_df['curr_sys']).mean(),
        'bp_normal_diastolic_delta': (bp_df['base_dia'] - bp_df['curr_dia']).mean(),
        # HTN
        'bp_htn_baseline_systolic_avg': htn_df['base_sys'].mean(),
        'bp_htn_baseline_diastolic_avg': htn_df['base_dia'].mean(),
        'bp_htn_latest_systolic_avg': htn_df['curr_sys'].mean(),
        'bp_htn_latest_diastolic_avg': htn_df['curr_dia'].mean(),
        'bp_htn_sample_size': len(htn_df),
        'bp_htn_systolic_delta': (htn_df['base_sys'] - htn_df['curr_sys']).mean(),
        'bp_htn_diastolic_delta': (htn_df['base_dia'] - htn_df['curr_dia']).mean()
    }
    
    # Combine all
    row = {'metric_category': label, 'total_users': len(df)}
    row.update(w_stats)
    row.update(b_stats)
    row.update(a_stats)
    row.update(bp_stats)
    
    # Round all floats to 2 decimals
    for k, v in row.items():
        if isinstance(v, float):
            row[k] = round(v, 2)
            
    return [row]

def main():
    script_start_time = time.time()
    conn = connect_to_db()
    
    try:
        print(f"🔗 Connected. Analyzing {PARTNER_NAME}...")
        
        # 1. Base Users & Retention
        df_all = get_partner_users(conn, PARTNER_NAME, ANALYSIS_END)
        df_cohort = calculate_6month_retention(conn, df_all, PARTNER_NAME, ANALYSIS_END)
        
        if df_cohort.empty: return

        # 2. Fetch Raw Data
        raw_w = get_clinical_data(conn, 'body_weight_values_cleaned', 'value * 2.20462', 'effective_date', PARTNER_NAME, ANALYSIS_END)
        raw_b = get_clinical_data(conn, 'bmi_values_cleaned', 'value', 'effective_date', PARTNER_NAME, ANALYSIS_END)
        raw_a = get_clinical_data(conn, 'a1c_values', 'value', 'effective_date', PARTNER_NAME, ANALYSIS_END)
        raw_bp = get_blood_pressure(conn, PARTNER_NAME, ANALYSIS_END)
        raw_glp = get_glp1_data(conn, PARTNER_NAME, ANALYSIS_END)
        raw_disc = get_questionnaire_flag(conn, 'A8z9j98E0sxR', 1)
        
        # 3. Identify GLP1 Status (Logic: >90 days supply, <10% gap)
        if not raw_glp.empty:
            raw_glp['total_days'] = raw_glp['days_of_supply'] * (1 + raw_glp['total_refills'].fillna(0))
            raw_glp['end_date'] = raw_glp['prescribed_at'] + pd.to_timedelta(raw_glp['total_days'], unit='D')
            
            user_stats = raw_glp.groupby('user_id').agg({'prescribed_at': 'min', 'end_date': 'max', 'total_days': 'sum'}).reset_index()
            user_stats['period'] = (user_stats['end_date'] - user_stats['prescribed_at']).dt.days
            
            # Gap logic
            user_stats['gap_pct'] = 0.0
            mask = user_stats['period'] > 0
            user_stats.loc[mask, 'gap_pct'] = ((user_stats['period'] - user_stats['total_days']) / user_stats['period']) * 100
            cutoff_date = pd.to_datetime(ANALYSIS_END) - pd.Timedelta(days=90)
            glp1_ids = user_stats[(user_stats['total_days'] >= 90) & (user_stats['gap_pct'] <= 10.0) & (user_stats['end_date'] > cutoff_date)]['user_id'].unique()
            df_cohort['is_glp1'] = df_cohort['user_id'].isin(glp1_ids).astype(int)
        else:
            df_cohort['is_glp1'] = 0

        # 4. Identify Discontinued
        disc_ids = raw_disc['user_id'].unique() if not raw_disc.empty else []
        df_cohort['is_glp1_disc'] = df_cohort['user_id'].isin(disc_ids).astype(int)

        # 5. Process Metrics
        print("⚙️  Processing metrics in Python...")
        df = process_metric(df_cohort, raw_w, 'weight', ANALYSIS_START, ANALYSIS_END)
        df = process_metric(df, raw_b, 'bmi', ANALYSIS_START, ANALYSIS_END)
        df = process_metric(df, raw_a, 'a1c', ANALYSIS_START, ANALYSIS_END)
        df = process_bp(df, raw_bp, ANALYSIS_START, ANALYSIS_END)
        
        # Calc weight deltas
        df['weight_loss_pct'] = np.nan
        df['weight_loss_lbs'] = np.nan
        mask_w = df['base_weight'].notna() & df['curr_weight'].notna()
        df.loc[mask_w, 'weight_loss_lbs'] = df.loc[mask_w, 'base_weight'] - df.loc[mask_w, 'curr_weight']
        df.loc[mask_w, 'weight_loss_pct'] = (df.loc[mask_w, 'weight_loss_lbs'] / df.loc[mask_w, 'base_weight']) * 100

        # 6. Generate QBR Segments
        results = []
        # Overall
        results.extend(generate_summary_stats(df, f'{PARTNER_NAME} - Overall'))
        # GLP1
        results.extend(generate_summary_stats(df[df['is_glp1'] == 1], f'{PARTNER_NAME} - GLP1'))
        # Non-GLP1
        results.extend(generate_summary_stats(df[df['is_glp1'] == 0], f'{PARTNER_NAME} - Non-GLP1'))
        # GLP1 Discontinued (Must be GLP1 AND have questionnaire flag)
        # Note: Original logic seemed to imply is_glp1_user = 1 AND questionnaire. 
        # Check if "Discontinued" implies they WERE GLP1 users. Assuming yes based on variable naming.
        results.extend(generate_summary_stats(df[(df['is_glp1'] == 1) & (df['is_glp1_disc'] == 1)], f'{PARTNER_NAME} - GLP1 Disc'))

        # 7. Create Placeholders (to match Excel structure)
        # Just creating empty lists or basic counts for the other tabs
        placeholders = {
            'Demographics': [{'metric': 'Placeholder', 'count': 0}],
            'Program Goals': [{'metric': 'Placeholder', 'count': 0}],
            'Medical Conditions': [{'metric': 'Placeholder', 'count': 0}]
        }

        # 8. Export
        fname = f'qbr_analysis_{PARTNER_NAME.replace(" ", "_")}_{ANALYSIS_START}_to_{ANALYSIS_END}.csv'
        print(f"\n💾 Saving to {fname}...")
        
        # Combine all results into one big list
        pd.DataFrame(results).to_csv(fname, index=False)
                
        print("✅ Done!")
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
