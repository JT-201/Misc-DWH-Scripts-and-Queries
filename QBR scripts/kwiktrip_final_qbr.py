import mysql.connector
import time
import pandas as pd
import numpy as np
from config import get_db_config

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
PARTNER_NAME = "Kwik Trip"
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
    return get_data(conn, query, "Active Users")

def get_clinical_data(conn, partner, table, val_col, date_col, max_date):
    """Generic fetch for clinical values"""
    query = f"""
        SELECT d.user_id, d.{val_col} as value, d.{date_col} as effective_date
        FROM {table} d
        JOIN partner_payers pe ON pe.user_id = d.user_id
        WHERE pe.name = '{partner}' AND d.{val_col} IS NOT NULL AND d.{date_col} <= '{max_date}'
    """
    return get_data(conn, query, f"Clinical Data: {table}")

def get_bp_data(conn, partner, max_date):
    """Fetch BP data specifically"""
    query = f"""
        SELECT d.user_id, d.systolic, d.diastolic, d.effective_date
        FROM blood_pressure_values d
        JOIN partner_payers pe ON pe.user_id = d.user_id
        WHERE pe.name = '{partner}' AND d.systolic IS NOT NULL AND d.effective_date <= '{max_date}'
    """
    return get_data(conn, query, "Blood Pressure Data")

def get_glp1_users(conn, partner, end_date):
    """Identify GLP1 users via Rx"""
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
    return get_data(conn, query, "GLP1 Prescriptions")

def get_glp1_disc_users(conn, question_id, answer_val):
    """Identify users who self-reported stopping GLP1"""
    query = f"""
        SELECT DISTINCT user_id 
        FROM questionnaire_records 
        WHERE question_id = '{question_id}' AND answer_value = {answer_val}
    """
    return get_data(conn, query, "GLP1 Discontinued (Questionnaire)")

def process_clinical_metric(users_df, data_df, value_col, date_col, metric_prefix):
    """Merges clinical data with users, finds baseline (first) and current (last) values."""
    if data_df.empty:
        return users_df
    
    # Ensure date is datetime
    data_df = data_df.copy()
    data_df[date_col] = pd.to_datetime(data_df[date_col], errors='coerce')

    # Merge
    merged = pd.merge(users_df[['user_id']], data_df, on='user_id', how='inner')
    
    # Sort by date
    merged.sort_values(by=['user_id', date_col], inplace=True)
    
    # Get First (Baseline)
    baseline = merged.groupby('user_id').first().reset_index()
    baseline = baseline.rename(columns={value_col: f'base_{metric_prefix}', date_col: f'base_{metric_prefix}_date'})
    
    # Get Last (Current)
    current = merged.groupby('user_id').last().reset_index()
    current = current.rename(columns={value_col: f'curr_{metric_prefix}', date_col: f'curr_{metric_prefix}_date'})
    
    # Combine
    final = pd.merge(users_df, baseline[['user_id', f'base_{metric_prefix}', f'base_{metric_prefix}_date']], on='user_id', how='left')
    final = pd.merge(final, current[['user_id', f'curr_{metric_prefix}', f'curr_{metric_prefix}_date']], on='user_id', how='left')
    
    return final

def process_bp_metric(users_df, bp_df):
    """Specific logic for BP (Systolic/Diastolic)"""
    if bp_df.empty:
        return users_df

    # Ensure date is datetime
    bp_df = bp_df.copy()
    bp_df['effective_date'] = pd.to_datetime(bp_df['effective_date'], errors='coerce')

    merged = pd.merge(users_df[['user_id']], bp_df, on='user_id', how='inner')
    merged.sort_values(by=['user_id', 'effective_date'], inplace=True)

    # Baseline
    base = merged.groupby('user_id').first().reset_index()
    base = base.rename(columns={'systolic': 'base_sys', 'diastolic': 'base_dia', 'effective_date': 'base_bp_date'})
    
    # Current
    curr = merged.groupby('user_id').last().reset_index()
    curr = curr.rename(columns={'systolic': 'curr_sys', 'diastolic': 'curr_dia', 'effective_date': 'curr_bp_date'})

    final = pd.merge(users_df, base[['user_id', 'base_sys', 'base_dia', 'base_bp_date']], on='user_id', how='left')
    final = pd.merge(final, curr[['user_id', 'curr_sys', 'curr_dia', 'curr_bp_date']], on='user_id', how='left')
    
    return final

def generate_summary_stats(df, label):
    """Generate the QBR summary metrics row"""
    if df.empty: return []
    
    row = {'metric_category': label, 'total_users': len(df)}
    
    # ---------------------------------------------------------
    # 1. WEIGHT & BMI
    # ---------------------------------------------------------
    if 'base_weight' in df.columns and 'curr_weight' in df.columns:
        w_df = df.dropna(subset=['base_weight', 'curr_weight'])
        # Filter: >= 30 days
        w_df = w_df[(w_df['curr_weight_date'] - w_df['base_weight_date']).dt.days >= 30]
        
        weight_stats = {
            'weight_baseline_avg': w_df['base_weight'].mean(),
            'weight_current_avg': w_df['curr_weight'].mean(),
            'weight_sample_size': len(w_df),
            'weight_loss_pct': w_df['weight_loss_pct'].mean(),
            'weight_loss_lbs': w_df['weight_loss_lbs'].mean(),
            'pct_lost_5pct': (len(w_df[w_df['weight_loss_pct'] >= 5])/len(w_df)*100) if len(w_df) > 0 else 0,
            'pct_lost_10pct': (len(w_df[w_df['weight_loss_pct'] >= 10])/len(w_df)*100) if len(w_df) > 0 else 0,
        }
    else:
        weight_stats = {}

    if 'base_bmi' in df.columns and 'curr_bmi' in df.columns:
        b_df = df.dropna(subset=['base_bmi', 'curr_bmi'])
        b_df = b_df[(b_df['curr_bmi_date'] - b_df['base_bmi_date']).dt.days >= 30]
        
        bmi_stats = {
            'bmi_baseline_avg': b_df['base_bmi'].mean(),
            'bmi_current_avg': b_df['curr_bmi'].mean(),
            'bmi_sample_size': len(b_df),
            'bmi_delta': (b_df['base_bmi'] - b_df['curr_bmi']).mean()
        }
    else:
        bmi_stats = {}

    # ---------------------------------------------------------
    # 2. A1C (Overall + Cohorts)
    # ---------------------------------------------------------
    if 'base_a1c' in df.columns and 'curr_a1c' in df.columns:
        a_df = df.dropna(subset=['base_a1c', 'curr_a1c'])
        a_df = a_df[(a_df['curr_a1c_date'] - a_df['base_a1c_date']).dt.days >= 30]
        
        # Cohorts
        a_6_5 = a_df[a_df['base_a1c'] >= 6.5]
        a_7_0 = a_df[a_df['base_a1c'] >= 7.0]
        a_8_0 = a_df[a_df['base_a1c'] >= 8.0]
        
        a1c_stats = {
            # Overall
            'a1c_baseline_avg': a_df['base_a1c'].mean(),
            'a1c_current_avg': a_df['curr_a1c'].mean(),
            'a1c_sample_size': len(a_df),
            'a1c_delta': (a_df['base_a1c'] - a_df['curr_a1c']).mean(),
            
            # >= 6.5
            'a1c_6_5_plus_baseline_avg': a_6_5['base_a1c'].mean(),
            'a1c_6_5_plus_current_avg': a_6_5['curr_a1c'].mean(),
            'a1c_6_5_plus_sample_size': len(a_6_5),
            'a1c_6_5_plus_delta': (a_6_5['base_a1c'] - a_6_5['curr_a1c']).mean(),

            # >= 7.0
            'a1c_7_plus_baseline_avg': a_7_0['base_a1c'].mean(),
            'a1c_7_plus_current_avg': a_7_0['curr_a1c'].mean(),
            'a1c_7_plus_sample_size': len(a_7_0),
            'a1c_7_plus_delta': (a_7_0['base_a1c'] - a_7_0['curr_a1c']).mean(),

            # >= 8.0
            'a1c_8_plus_baseline_avg': a_8_0['base_a1c'].mean(),
            'a1c_8_plus_current_avg': a_8_0['curr_a1c'].mean(),
            'a1c_8_plus_sample_size': len(a_8_0),
            'a1c_8_plus_delta': (a_8_0['base_a1c'] - a_8_0['curr_a1c']).mean(),
        }
    else:
        a1c_stats = {}
    
    # ---------------------------------------------------------
    # 3. Blood Pressure (Overall + Cohorts)
    # ---------------------------------------------------------
    if 'base_sys' in df.columns and 'curr_sys' in df.columns:
        bp_df = df.dropna(subset=['base_sys', 'curr_sys'])
        bp_df = bp_df[(bp_df['curr_bp_date'] - bp_df['base_bp_date']).dt.days >= 30]
        
        # Cohorts
        bp_htn = bp_df[(bp_df['base_sys'] >= 140) | (bp_df['base_dia'] >= 90)]
        bp_130 = bp_df[(bp_df['base_sys'] >= 130) | (bp_df['base_dia'] >= 80)]
        
        bp_stats = {
            # Overall
            'bp_normal_baseline_systolic_avg': bp_df['base_sys'].mean(),
            'bp_normal_baseline_diastolic_avg': bp_df['base_dia'].mean(),
            'bp_normal_latest_systolic_avg': bp_df['curr_sys'].mean(),
            'bp_normal_latest_diastolic_avg': bp_df['curr_dia'].mean(),
            'bp_normal_sample_size': len(bp_df),
            'bp_normal_systolic_delta': (bp_df['base_sys'] - bp_df['curr_sys']).mean(),
            'bp_normal_diastolic_delta': (bp_df['base_dia'] - bp_df['curr_dia']).mean(),
            
            # HTN (>= 140/90)
            'bp_htn_baseline_systolic_avg': bp_htn['base_sys'].mean(),
            'bp_htn_baseline_diastolic_avg': bp_htn['base_dia'].mean(),
            'bp_htn_latest_systolic_avg': bp_htn['curr_sys'].mean(),
            'bp_htn_latest_diastolic_avg': bp_htn['curr_dia'].mean(),
            'bp_htn_sample_size': len(bp_htn),
            'bp_htn_systolic_delta': (bp_htn['base_sys'] - bp_htn['curr_sys']).mean(),
            'bp_htn_diastolic_delta': (bp_htn['base_dia'] - bp_htn['curr_dia']).mean(),

            # Elevated (>= 130/80)
            'bp_130_baseline_systolic_avg': bp_130['base_sys'].mean(),
            'bp_130_baseline_diastolic_avg': bp_130['base_dia'].mean(),
            'bp_130_latest_systolic_avg': bp_130['curr_sys'].mean(),
            'bp_130_latest_diastolic_avg': bp_130['curr_dia'].mean(),
            'bp_130_sample_size': len(bp_130),
            'bp_130_systolic_delta': (bp_130['base_sys'] - bp_130['curr_sys']).mean(),
            'bp_130_diastolic_delta': (bp_130['base_dia'] - bp_130['curr_dia']).mean()
        }
    else:
        bp_stats = {}
    
    # ---------------------------------------------------------
    # 4. Final Row Construction
    # ---------------------------------------------------------
    row.update(weight_stats)
    row.update(bmi_stats)
    row.update(a1c_stats)
    row.update(bp_stats)
    
    # Rounding
    for k, v in row.items():
        if isinstance(v, float):
            row[k] = round(v, 2)
            
    return [row]

def main():
    print(f"🚀 Starting QBR Analysis for {PARTNER_NAME} ({ANALYSIS_START} to {ANALYSIS_END})")
    
    conn = connect_to_db()
    if not conn:
        return

    # 1. Get Population
    users = get_partner_users(conn, PARTNER_NAME, ANALYSIS_END)
    print(f"  👉 Total Active Users: {len(users):,}")
    
    # 2. Get Clinical Data
    
    # A) WEIGHT - UPDATED to use 'body_weight_values_cleaned' and convert kg->lbs
    w_data = get_clinical_data(conn, PARTNER_NAME, "body_weight_values_cleaned", "value * 2.20462", "effective_date", ANALYSIS_END)

    # B) BMI - UPDATED to use 'bmi_values_cleaned'
    b_data = get_clinical_data(conn, PARTNER_NAME, "bmi_values_cleaned", "value", "effective_date", ANALYSIS_END)

    # C) A1C & BP
    a_data = get_clinical_data(conn, PARTNER_NAME, "a1c_values", "value", "effective_date", ANALYSIS_END)
    bp_data = get_bp_data(conn, PARTNER_NAME, ANALYSIS_END)
    
    # 3. Get GLP1 Status
    glp1_rx = get_glp1_users(conn, PARTNER_NAME, ANALYSIS_END)
    glp1_disc = get_glp1_disc_users(conn, "GLP1_DISC_Q_ID", 1) 
    
    conn.close()
    
    # 4. Process Data
    print("\n⚙️ Processing Data...")
    
    df = users.copy()
    
    # Initialize necessary columns with NaN
    cols_to_init = [
        'base_weight', 'curr_weight', 'base_bmi', 'curr_bmi',
        'base_a1c', 'curr_a1c',
        'base_sys', 'base_dia', 'curr_sys', 'curr_dia',
        'weight_loss_pct', 'weight_loss_lbs'
    ]
    for col in cols_to_init:
        if col not in df.columns:
            df[col] = np.nan
            
    # Initialize Date columns
    date_cols = [
        'base_weight_date', 'curr_weight_date', 'base_bmi_date', 'curr_bmi_date',
        'base_a1c_date', 'curr_a1c_date', 'base_bp_date', 'curr_bp_date'
    ]
    for col in date_cols:
        if col not in df.columns:
            df[col] = pd.to_datetime(np.nan)

    # Merge Weight
    if not w_data.empty:
        df = df.drop(columns=['base_weight', 'curr_weight', 'base_weight_date', 'curr_weight_date'], errors='ignore')
        df = process_clinical_metric(df, w_data, 'value', 'effective_date', 'weight')

    # Merge BMI
    if not b_data.empty:
        df = df.drop(columns=['base_bmi', 'curr_bmi', 'base_bmi_date', 'curr_bmi_date'], errors='ignore')
        df = process_clinical_metric(df, b_data, 'value', 'effective_date', 'bmi')

    # Merge A1C
    if not a_data.empty:
         df = df.drop(columns=['base_a1c', 'curr_a1c', 'base_a1c_date', 'curr_a1c_date'], errors='ignore')
         df = process_clinical_metric(df, a_data, 'value', 'effective_date', 'a1c')
    
    # Merge BP
    if not bp_data.empty:
        df = df.drop(columns=['base_sys', 'base_dia', 'curr_sys', 'curr_dia', 'base_bp_date', 'curr_bp_date'], errors='ignore')
        df = process_bp_metric(df, bp_data)
        
    # FINAL SAFETY CHECK: Convert all date columns to datetime
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Calc Derived Metrics
    if 'base_weight' in df.columns and 'curr_weight' in df.columns:
        df['weight_loss_lbs'] = df['base_weight'] - df['curr_weight']
        df['weight_loss_pct'] = (df['weight_loss_lbs'] / df['base_weight']) * 100
    
    # Tag GLP1
    glp1_ids = set(glp1_rx['user_id'].unique()) if not glp1_rx.empty else set()
    df['is_glp1'] = df['user_id'].apply(lambda x: 1 if x in glp1_ids else 0)
    
    disc_ids = set(glp1_disc['user_id'].unique()) if not glp1_disc.empty else set()
    df['is_glp1_disc'] = df['user_id'].apply(lambda x: 1 if x in disc_ids else 0)
    
    # 5. Generate Stats
    results = []
    
    results.extend(generate_summary_stats(df, f'{PARTNER_NAME} - Overall'))
    results.extend(generate_summary_stats(df[df['is_glp1'] == 1], f'{PARTNER_NAME} - GLP1'))
    results.extend(generate_summary_stats(df[df['is_glp1'] == 0], f'{PARTNER_NAME} - Non-GLP1'))
    results.extend(generate_summary_stats(df[(df['is_glp1'] == 1) & (df['is_glp1_disc'] == 1)], f'{PARTNER_NAME} - GLP1 Disc'))

    # 6. Export
    fname = f'qbr_analysis_{PARTNER_NAME.replace(" ", "_")}_{ANALYSIS_START}_to_{ANALYSIS_END}.csv'
    print(f"\n💾 Saving to {fname}...")
    pd.DataFrame(results).to_csv(fname, index=False)
    print("✅ Done!")

if __name__ == "__main__":
    main()