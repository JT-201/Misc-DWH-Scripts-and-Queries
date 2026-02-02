import mysql.connector
import time
import pandas as pd
import numpy as np
from config import get_db_config

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
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
# FETCH
# ------------------------------------------------------------
def get_active_users(conn, end_date):
    query = f"""
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN user_program_memberships upm ON upm.user_id = s.user_id
        WHERE s.status = 'ACTIVE'
        AND upm.program IN ('HEALTHY_WEIGHT_JOURNEY', 'weightloss')
        AND s.start_date <= '{end_date}'
    """
    return get_data(conn, query, "Active Users")

def get_med_history(conn, end_date):
    query = f"""
        SELECT p.patient_user_id as user_id, 
               p.prescribed_at,
               p.days_of_supply,
               COALESCE(p.total_refills, 0) as total_refills,
               m.name as med_name
        FROM prescriptions p
        JOIN medication_ndcs mn ON p.prescribed_ndc = mn.ndc
        JOIN medications m ON m.id = mn.medication_id
        WHERE m.therapy_type = 'WM'
        AND p.prescribed_at <= '{end_date}'
    """
    return get_data(conn, query, "WM Med History")

def get_clinical_data(conn, table, val_col, date_col, max_date):
    query = f"""
        SELECT user_id, {val_col} as value, {date_col} as effective_date
        FROM {table}
        WHERE {val_col} IS NOT NULL AND {date_col} <= '{max_date}'
    """
    return get_data(conn, query, f"Clinical Data: {table}")

# ------------------------------------------------------------
# LOGIC
# ------------------------------------------------------------

def filter_by_bmi(users_df, bmi_df, min_bmi=30):
    if users_df.empty or bmi_df.empty: return pd.DataFrame()
    df = pd.merge(users_df, bmi_df, on='user_id', how='inner')
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['effective_date'] = pd.to_datetime(df['effective_date'])
    
    # Baseline: >= start - 30 days
    df = df[df['effective_date'] >= (df['start_date'] - pd.Timedelta(days=30))]
    
    # First record
    baseline = df.sort_values('effective_date').groupby('user_id').first().reset_index()
    
    obese_ids = baseline[baseline['value'] >= min_bmi]['user_id'].unique()
    print(f"  ⚖️  BMI Filter (>=30): {len(obese_ids):,} users")
    return users_df[users_df['user_id'].isin(obese_ids)]

def filter_consistent_weigh_ins(users_df, weight_df, min_months=6):
    """
    Filters for users who have logged at least one weight 
    in 'min_months' consecutive calendar months.
    """
    if users_df.empty or weight_df.empty: return pd.DataFrame()
    
    # Filter weight data to current user list
    df = weight_df[weight_df['user_id'].isin(users_df['user_id'])].copy()
    
    # Normalize dates to the 1st of the month (Frequency = Monthly)
    df['effective_date'] = pd.to_datetime(df['effective_date'])
    df['activity_month'] = df['effective_date'].dt.to_period('M').dt.to_timestamp()
    
    # Deduplicate: We only need ONE record per month per user
    df = df.drop_duplicates(subset=['user_id', 'activity_month'])
    df = df.sort_values(['user_id', 'activity_month'])
    
    # Calculate Streaks
    df['prev_month'] = df.groupby('user_id')['activity_month'].shift(1)
    df['diff_days'] = (df['activity_month'] - df['prev_month']).dt.days
    
    # New streak starts if diff is NaN (first record) or gap is > 32 days (missed a month)
    df['new_streak'] = (df['diff_days'] > 32) | (df['diff_days'].isna())
    df['streak_id'] = df.groupby('user_id')['new_streak'].cumsum()
    
    # Measure Streak Lengths
    streaks = df.groupby(['user_id', 'streak_id']).agg(
        consecutive_months=('activity_month', 'count')
    ).reset_index()
    
    valid_ids = streaks[streaks['consecutive_months'] >= min_months]['user_id'].unique()
    
    print(f"  📅 Engagement Filter (Weights 1x/mo for {min_months}+ mo): {len(valid_ids):,} users")
    return users_df[users_df['user_id'].isin(valid_ids)]

def identify_lifestyle_ids(all_ids, df_meds, analysis_end):
    """Returns IDs of 'Lifestyle' users (No recent meds, low lifetime exposure)"""
    if df_meds.empty: return set(all_ids)
    
    df = df_meds.copy()
    df['prescribed_at'] = pd.to_datetime(df['prescribed_at'])
    df['rx_days'] = df['days_of_supply'] * (1 + df['total_refills'].fillna(0))
    df['end_date'] = df['prescribed_at'] + pd.to_timedelta(df['rx_days'], unit='D')
    
    user_stats = df.groupby('user_id').agg(
        total_days=('rx_days', 'sum'),
        last_end=('end_date', 'max')
    ).reset_index()
    
    cutoff = pd.to_datetime(analysis_end) - pd.Timedelta(days=90)
    
    # Exclude if: Active recently OR Total history > 90 days
    fail_mask = (user_stats['last_end'] >= cutoff) | (user_stats['total_days'] > 90)
    excluded_ids = set(user_stats[fail_mask]['user_id'])
    
    return set(all_ids) - excluded_ids

def identify_glp1_ids(all_ids, df_meds, analysis_end):
    """
    Identifies GLP1 users:
    1. On GLP1 for >= 90 days (Period)
    2. Gap < 10%
    3. Active coverage in last 90 days
    """
    if df_meds.empty: return set()
    
    glp1_terms = ['Wegovy', 'Zepbound', 'Ozempic', 'Mounjaro']
    mask_glp = df_meds['med_name'].str.contains('|'.join(glp1_terms), case=False, na=False)
    df = df_meds[mask_glp].copy()
    
    if df.empty: return set()
    
    df['prescribed_at'] = pd.to_datetime(df['prescribed_at'])
    df['rx_days'] = df['days_of_supply'] * (1 + df['total_refills'].fillna(0))
    df['end_date'] = df['prescribed_at'] + pd.to_timedelta(df['rx_days'], unit='D')
    
    stats = df.groupby('user_id').agg(
        first_fill=('prescribed_at', 'min'),
        last_end=('end_date', 'max'),
        total_covered=('rx_days', 'sum')
    ).reset_index()
    
    stats['total_period_days'] = (stats['last_end'] - stats['first_fill']).dt.days
    
    mask_duration = stats['total_period_days'] >= 90
    stats['gap_pct'] = (stats['total_period_days'] - stats['total_covered']) / stats['total_period_days']
    mask_adherence = stats['gap_pct'] < 0.10
    
    cutoff = pd.to_datetime(analysis_end) - pd.Timedelta(days=90)
    mask_active = stats['last_end'] >= cutoff
    
    valid_ids = set(stats[mask_duration & mask_adherence & mask_active]['user_id'])
    return valid_ids.intersection(all_ids)

def calc_weight_loss(cohort_df, weight_df):
    if cohort_df.empty or weight_df.empty: return pd.DataFrame()
    
    # Filter weight data to only this cohort
    df_w = weight_df[weight_df['user_id'].isin(cohort_df['user_id'])].copy()
    df_w = df_w.sort_values(['user_id', 'effective_date'])
    df_w['effective_date'] = pd.to_datetime(df_w['effective_date'])
    cohort_df['start_date'] = pd.to_datetime(cohort_df['start_date'])
    
    results = []
    w_dict = df_w.groupby('user_id')
    
    for _, row in cohort_df.iterrows():
        uid = row['user_id']
        if uid not in w_dict.groups: continue
        uw = w_dict.get_group(uid)
        
        # Baseline: >= start - 30d
        base_candidates = uw[uw['effective_date'] >= (row['start_date'] - pd.Timedelta(days=30))]
        if base_candidates.empty: continue
        base_rec = base_candidates.iloc[0]
        
        # Current
        curr_rec = uw.iloc[-1]
        
        days_diff = (curr_rec['effective_date'] - base_rec['effective_date']).days
        if days_diff >= 30:
            res = {
                'user_id': uid,
                'baseline_weight': base_rec['value'],
                'current_weight': curr_rec['value']
            }
            results.append(res)
            
    return pd.DataFrame(results)

def get_stats(df):
    if df.empty:
        return {'n': 0, 'avg': 0, 'median': 0, 'pct_5': 0, 'pct_10': 0}
    
    loss = ((df['baseline_weight'] - df['current_weight']) / df['baseline_weight']) * 100
    return {
        'n': len(df),
        'avg': loss.mean(),
        'median': loss.median(),
        'pct_5': (loss >= 5).mean() * 100,
        'pct_10': (loss >= 10).mean() * 100
    }

def main():
    print(f"🚀 Starting Analysis (Strict 6-Month Weight Consistency)")
    conn = connect_to_db()
    
    try:
        # 1. Base Population
        users = get_active_users(conn, ANALYSIS_END)
        bmi = get_clinical_data(conn, "bmi_values_cleaned", "value", "effective_date", ANALYSIS_END)
        users = filter_by_bmi(users, bmi) # BMI >= 30
        
        # 2. Fetch Weight Data (EARLY FETCH)
        # We need this now for the consistency filter, and we'll reuse it for outcomes
        weight = get_clinical_data(conn, "body_weight_values_cleaned", "value * 2.20462", "effective_date", ANALYSIS_END)

        # 3. Apply Strict Weight Consistency Filter
        users = filter_consistent_weigh_ins(users, weight, min_months=6)
        
        if users.empty:
            print("No users met base criteria.")
            return
            
        # 4. Define Groups
        meds = get_med_history(conn, ANALYSIS_END)
        all_ids = users['user_id'].unique()
        
        # Group A: Lifestyle
        life_ids = identify_lifestyle_ids(all_ids, meds, ANALYSIS_END)
        df_life = users[users['user_id'].isin(life_ids)].copy()
        
        # Group B: GLP-1 (Strict Adherence)
        glp1_ids = identify_glp1_ids(all_ids, meds, ANALYSIS_END)
        df_glp1 = users[users['user_id'].isin(glp1_ids)].copy()
        
        # Group C: Overall
        df_overall = users.copy()
        
        # 5. Calculate Outcomes (Reusing 'weight' dataframe)
        res_life = calc_weight_loss(df_life, weight)
        res_glp1 = calc_weight_loss(df_glp1, weight)
        res_over = calc_weight_loss(df_overall, weight)
        
        # 6. Stats
        s_life = get_stats(res_life)
        s_glp1 = get_stats(res_glp1)
        s_over = get_stats(res_over)
        
        # 7. Output
        print("\n" + "="*85)
        print(f"{'METRIC':<20} | {'LIFESTYLE':<15} | {'GLP-1 (Adherent)':<18} | {'OVERALL':<15}")
        print("-" * 85)
        print(f"{'Count (N)':<20} | {s_life['n']:<15,} | {s_glp1['n']:<18,} | {s_over['n']:<15,}")
        print(f"{'Avg Weight Loss':<20} | {s_life['avg']:<14.2f}% | {s_glp1['avg']:<17.2f}% | {s_over['avg']:<14.2f}%")
        print(f"{'Median Weight Loss':<20} | {s_life['median']:<14.2f}% | {s_glp1['median']:<17.2f}% | {s_over['median']:<14.2f}%")
        print(f"{'> 5% Loss':<20} | {s_life['pct_5']:<14.1f}% | {s_glp1['pct_5']:<17.1f}% | {s_over['pct_5']:<14.1f}%")
        print(f"{'> 10% Loss':<20} | {s_life['pct_10']:<14.1f}% | {s_glp1['pct_10']:<17.1f}% | {s_over['pct_10']:<14.1f}%")
        print("="*85 + "\n")
        res_life['group'] = 'Lifestyle'
        res_glp1['group'] = 'GLP1'
        res_over['group'] = 'Overall'
        pd.concat([res_life, res_glp1, res_over]).to_csv("three_way_strict_weights.csv", index=False)
        print("💾 Saved 'three_way_strict_weights.csv'")
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
