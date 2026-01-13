import mysql.connector
import time
import pandas as pd
import warnings
import numpy as np
from datetime import timedelta
from config import get_db_config

PARTNER = "Apple"
REPORT_DATE = "2025-12-31"

# PG/SLA cutoffs (Apple 12/2025)
TENURE_MIN_DAYS = 90
BMI_THRESHOLD = 30
WEIGHTS_PER_BILLABLE_MONTH_MIN = 10
BP_PER_BILLABLE_MONTH_MIN = 5
A1C_MIN_LABS_IN_12MO = 2

# GLP-1 med matching (keep broad; names come through meds table)
GLP1_NAME_LIKE = ["%Wegovy%", "%Zepbound%", "%Ozempic%", "%Mounjaro%"]


# -----------------------
# DB helpers
# -----------------------
def connect_to_db():
    # get_db_config() handles dev/prod host/port/user/db
    return mysql.connector.connect(**get_db_config())

def get_data(query, description):
    # Simple wrapper to time queries + return a dataframe
    start_time = time.time()
    conn = connect_to_db()
    try:
        warnings.filterwarnings("ignore", category=UserWarning)
        print(f"  📥 Fetching {description}...")
        df = pd.read_sql(query, conn)
        duration = time.time() - start_time
        print(f"    ⏱️  {description}: {len(df):,} rows in {duration:.2f}s")
        return df
    finally:
        conn.close()


# -----------------------
# Baseline/latest helpers
# -----------------------
def calculate_baseline(df, value_col, date_col="effective_date", outcome_name="baseline"):
    # First observed value per user (by date)
    if df.empty:
        return pd.DataFrame(columns=["user_id", outcome_name, f"{outcome_name}_date"])

    df_sorted = df.sort_values(by=["user_id", date_col], ascending=[True, True])
    baseline = df_sorted.groupby("user_id", as_index=False).first()
    baseline = baseline.rename(columns={value_col: outcome_name, date_col: f"{outcome_name}_date"})
    return baseline[["user_id", outcome_name, f"{outcome_name}_date"]]

def calculate_latest(df, value_col, date_col="effective_date", outcome_name="latest"):
    # Most recent value per user (as-of REPORT_DATE)
    if df.empty:
        return pd.DataFrame(columns=["user_id", outcome_name, f"{outcome_name}_date"])

    df_sorted = df.sort_values(by=["user_id", date_col], ascending=[True, False])
    latest = df_sorted.groupby("user_id", as_index=False).first()
    latest = latest.rename(columns={value_col: outcome_name, date_col: f"{outcome_name}_date"})
    return latest[["user_id", outcome_name, f"{outcome_name}_date"]]


# -----------------------
# Billable-month utilities (billable_user_statuses)
# -----------------------
def build_billable_months(df_bus: pd.DataFrame) -> pd.DataFrame:
    # Treat a month as billable if there’s at least one day with is_billable=1
    if df_bus.empty:
        return pd.DataFrame(columns=["user_id", "month_start"])

    d = df_bus.copy()
    d["date"] = pd.to_datetime(d["date"])
    d["month_start"] = d["date"].dt.to_period("M").dt.to_timestamp()
    d = d[d["is_billable"] == 1]
    return d[["user_id", "month_start"]].drop_duplicates()

def count_measurements_per_billable_month(
    meas_df: pd.DataFrame,
    billable_months: pd.DataFrame,
    date_col: str,
    min_per_month: int
) -> pd.DataFrame:
    """
    For each user:
      - look at months where they were billable
      - require >= min_per_month measurements in each of those months

    If someone is billable in a month and has 0 measurements, they fail.
    """
    if billable_months.empty:
        return pd.DataFrame(columns=["user_id", "flag_meets_monthly_requirement"])

    users = billable_months[["user_id"]].drop_duplicates()

    if meas_df.empty:
        users["flag_meets_monthly_requirement"] = 0
        return users

    m = meas_df.copy()
    m[date_col] = pd.to_datetime(m[date_col])
    m["month_start"] = m[date_col].dt.to_period("M").dt.to_timestamp()

    counts = m.groupby(["user_id", "month_start"]).size().reset_index(name="cnt")

    merged = billable_months.merge(counts, on=["user_id", "month_start"], how="left")
    merged["cnt"] = merged["cnt"].fillna(0)
    merged["met"] = (merged["cnt"] >= min_per_month).astype(int)

    agg = merged.groupby("user_id").agg(
        months_total=("month_start", "nunique"),
        months_met=("met", "sum")
    ).reset_index()

    agg["flag_meets_monthly_requirement"] = (agg["months_total"] == agg["months_met"]).astype(int)
    return agg[["user_id", "flag_meets_monthly_requirement"]]


# -----------------------
# GLP coverage as-of report date
# -----------------------
def compute_glp1_active_as_of(df_glp1: pd.DataFrame, report_date: str) -> pd.DataFrame:
    # Active GLP = estimated supply end date covers through report_date
    if df_glp1.empty:
        return pd.DataFrame(columns=["user_id", "flag_active_glp1_on_report_date", "last_rx_end_date"])

    r = df_glp1.copy()
    r["prescribed_at"] = pd.to_datetime(r["prescribed_at"])
    r["total_refills"] = pd.to_numeric(r["total_refills"], errors="coerce").fillna(0)
    r["days_of_supply"] = pd.to_numeric(r["days_of_supply"], errors="coerce").fillna(0)

    r["rx_days_coverage"] = r["days_of_supply"] * (1 + r["total_refills"])
    r["rx_end_date"] = r["prescribed_at"] + pd.to_timedelta(r["rx_days_coverage"], unit="D")

    g = r.groupby("user_id").agg(last_rx_end_date=("rx_end_date", "max")).reset_index()
    report_dt = pd.to_datetime(report_date)
    g["flag_active_glp1_on_report_date"] = (g["last_rx_end_date"] >= report_dt).astype(int)
    return g[["user_id", "flag_active_glp1_on_report_date", "last_rx_end_date"]]


# -----------------------
# Summary helpers (rollups for the sheet)
# -----------------------
def summarize_weight(df_in: pd.DataFrame, label: str) -> dict:
    # Weight summary expects baseline + current present
    d = df_in[df_in["baseline_weight_lbs"].notna() & df_in["current_weight_lbs"].notna()].copy()
    if d.empty:
        return {
            "Metric": label,
            "avg_baseline": np.nan, "avg_current": np.nan,
            "median_baseline": np.nan, "median_current": np.nan,
            "delta_avg": np.nan, "delta_median": np.nan,
            "sample_size": 0
        }

    delta = d["baseline_weight_lbs"] - d["current_weight_lbs"]
    return {
        "Metric": label,
        "avg_baseline": float(d["baseline_weight_lbs"].mean()),
        "avg_current": float(d["current_weight_lbs"].mean()),
        "median_baseline": float(d["baseline_weight_lbs"].median()),
        "median_current": float(d["current_weight_lbs"].median()),
        "delta_avg": float(delta.mean()),
        "delta_median": float(delta.median()),
        "sample_size": int(d["user_id"].nunique())
    }

def summarize_bp(df_in: pd.DataFrame, label: str) -> dict:
    # BP summary expects baseline + current present
    d = df_in[df_in["baseline_systolic"].notna() & df_in["current_systolic"].notna()].copy()
    if d.empty:
        return {"Metric": label, "avg_baseline": np.nan, "avg_current": np.nan, "delta_avg": np.nan, "sample_size": 0}

    base_sys = d["baseline_systolic"].mean()
    base_dia = d["baseline_diastolic"].mean()
    cur_sys = d["current_systolic"].mean()
    cur_dia = d["current_diastolic"].mean()

    return {
        "Metric": label,
        "avg_baseline": f"{base_sys:.2f}/{base_dia:.2f}",
        "avg_current": f"{cur_sys:.2f}/{cur_dia:.2f}",
        "delta_avg": f"{(base_sys-cur_sys):.2f}/{(base_dia-cur_dia):.2f}",
        "sample_size": int(d["user_id"].nunique())
    }

def summarize_a1c(df_in: pd.DataFrame, label: str) -> dict:
    # A1C summary expects baseline + current present
    d = df_in[df_in["baseline_a1c"].notna() & df_in["current_a1c"].notna()].copy()
    if d.empty:
        return {"Metric": label, "avg_baseline": np.nan, "avg_current": np.nan, "delta_avg": np.nan, "sample_size": 0}

    delta = d["baseline_a1c"] - d["current_a1c"]
    return {
        "Metric": label,
        "avg_baseline": float(d["baseline_a1c"].mean()),
        "avg_current": float(d["current_a1c"].mean()),
        "delta_avg": float(delta.mean()),
        "sample_size": int(d["user_id"].nunique())
    }


def main(partner=PARTNER, report_date=REPORT_DATE):
    print(f"\n🚀 Apple PG Clinical — Partner={partner} As-Of={report_date}")

    # 1) Cohort: Apple users who are ACTIVE on the report date + have >=90 days tenure
    base_users = get_data(f"""
        SELECT DISTINCT
            bus.user_id,
            s.start_date,
            DATEDIFF('{report_date}', s.start_date) AS tenure_days
        FROM billable_user_statuses bus
        JOIN subscriptions s ON s.user_id = bus.user_id
        WHERE bus.partner = '{partner}'
          AND bus.date = '{report_date}'
          AND bus.subscription_status = 'ACTIVE'
          AND s.start_date <= '{report_date}'
          AND DATEDIFF('{report_date}', s.start_date) >= {TENURE_MIN_DAYS}
    """, "Base cohort (ACTIVE on report date + tenure >= 90)")

    if base_users.empty:
        print("⚠️ No users found. Stopping.")
        return

    # Build an IN (...) list. user_id is binary in prod, so keep the 0x... format.
    raw_ids = base_users["user_id"].tolist()
    if raw_ids and isinstance(raw_ids[0], (bytes, bytearray)):
        formatted_ids = ", ".join([f"0x{uid.hex()}" for uid in raw_ids])
    else:
        formatted_ids = str(tuple(raw_ids))
        if len(raw_ids) == 1:
            formatted_ids = formatted_ids.replace(",", "")

    # 2) Pull billable_user_statuses history from the earliest start_date in the cohort
    min_start = base_users["start_date"].min()
    df_bus = get_data(f"""
        SELECT user_id, date, is_billable, subscription_status, user_status
        FROM billable_user_statuses
        WHERE partner = '{partner}'
          AND date >= '{pd.to_datetime(min_start).date()}'
          AND date <= '{report_date}'
          AND user_id IN ({formatted_ids})
    """, "billable_user_statuses (cohort history)")

    # 3) Pull clinical measures for cohort users only
    df_weights = get_data(f"""
        SELECT user_id, effective_date, value * 2.20462 AS weight_lbs
        FROM body_weight_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date <= '{report_date}'
          AND value IS NOT NULL
    """, "Weight logs (cohort)")

    df_bmi = get_data(f"""
        SELECT user_id, effective_date, value AS bmi
        FROM bmi_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date <= '{report_date}'
          AND value IS NOT NULL
    """, "BMI logs (cohort)")

    df_bp = get_data(f"""
        SELECT user_id, effective_date, systolic, diastolic
        FROM blood_pressure_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date <= '{report_date}'
          AND systolic IS NOT NULL
          AND diastolic IS NOT NULL
    """, "BP logs (cohort)")

    df_a1c = get_data(f"""
        SELECT user_id, effective_date, value AS a1c
        FROM a1c_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date <= '{report_date}'
          AND value IS NOT NULL
    """, "A1C logs (cohort)")

    glp_like = " OR ".join([f"m.name LIKE '{x}'" for x in GLP1_NAME_LIKE])
    df_glp1 = get_data(f"""
        SELECT p.patient_user_id AS user_id, p.days_of_supply, p.total_refills, p.prescribed_at
        FROM prescriptions p
        JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
        JOIN medications m ON m.id = ndcs.medication_id
        WHERE p.patient_user_id IN ({formatted_ids})
          AND p.prescribed_at IS NOT NULL
          AND ({glp_like})
    """, "GLP1 Rx (cohort)")

    # 4) Normalize dates once before doing any grouping
    base_users["start_date"] = pd.to_datetime(base_users["start_date"])
    df_bus["date"] = pd.to_datetime(df_bus["date"])

    for df in [df_weights, df_bmi, df_bp, df_a1c]:
        if not df.empty:
            df["effective_date"] = pd.to_datetime(df["effective_date"])
    if not df_glp1.empty:
        df_glp1["prescribed_at"] = pd.to_datetime(df_glp1["prescribed_at"])

    # 5) Baseline + current values (as-of report_date)
    base_weight = calculate_baseline(df_weights, "weight_lbs", outcome_name="baseline_weight_lbs")
    cur_weight = calculate_latest(df_weights, "weight_lbs", outcome_name="current_weight_lbs")

    base_bmi = calculate_baseline(df_bmi, "bmi", outcome_name="baseline_bmi")
    cur_bmi = calculate_latest(df_bmi, "bmi", outcome_name="current_bmi")

    base_a1c = calculate_baseline(df_a1c, "a1c", outcome_name="baseline_a1c")
    cur_a1c = calculate_latest(df_a1c, "a1c", outcome_name="current_a1c")

    # BP is two fields, so handle baseline/current explicitly
    if not df_bp.empty:
        bp_base = (
            df_bp.sort_values(["user_id", "effective_date"], ascending=[True, True])
            .groupby("user_id", as_index=False)
            .first()
            .rename(columns={
                "systolic": "baseline_systolic",
                "diastolic": "baseline_diastolic",
                "effective_date": "baseline_bp_date"
            })[["user_id", "baseline_systolic", "baseline_diastolic", "baseline_bp_date"]]
        )

        bp_cur = (
            df_bp.sort_values(["user_id", "effective_date"], ascending=[True, False])
            .groupby("user_id", as_index=False)
            .first()
            .rename(columns={
                "systolic": "current_systolic",
                "diastolic": "current_diastolic",
                "effective_date": "current_bp_date"
            })[["user_id", "current_systolic", "current_diastolic", "current_bp_date"]]
        )
    else:
        bp_base = pd.DataFrame(columns=["user_id", "baseline_systolic", "baseline_diastolic", "baseline_bp_date"])
        bp_cur = pd.DataFrame(columns=["user_id", "current_systolic", "current_diastolic", "current_bp_date"])

    # 6) Monthly engagement checks are based on billable months, not calendar tenure
    billable_months = build_billable_months(df_bus)

    weight_monthly_flag = count_measurements_per_billable_month(
        df_weights, billable_months, "effective_date", WEIGHTS_PER_BILLABLE_MONTH_MIN
    ).rename(columns={"flag_meets_monthly_requirement": "flag_10_weights_each_billable_month"})

    bp_monthly_flag = count_measurements_per_billable_month(
        df_bp, billable_months, "effective_date", BP_PER_BILLABLE_MONTH_MIN
    ).rename(columns={"flag_meets_monthly_requirement": "flag_5_bp_each_billable_month"})

    # 7) GLP: treat “on GLP” as supply covering the report date (not just historical use)
    glp_active = compute_glp1_active_as_of(df_glp1, report_date)

    # 8) A1C: count distinct lab dates in the last 12 months
    report_dt = pd.to_datetime(report_date)
    start_12mo = report_dt - timedelta(days=365)
    if not df_a1c.empty:
        a1c_12mo = df_a1c[(df_a1c["effective_date"] >= start_12mo) & (df_a1c["effective_date"] <= report_dt)]
        a1c_counts = a1c_12mo.groupby("user_id")["effective_date"].nunique().reset_index(name="a1c_labs_12mo")
    else:
        a1c_counts = pd.DataFrame(columns=["user_id", "a1c_labs_12mo"])

    # 9) Assemble one wide user-level table for QA + rollups
    master = base_users.copy()
    for t in [
        base_weight, cur_weight,
        base_bmi, cur_bmi,
        base_a1c, cur_a1c,
        bp_base, bp_cur,
        weight_monthly_flag, bp_monthly_flag,
        glp_active, a1c_counts
    ]:
        master = master.merge(t, on="user_id", how="left")

    # Fill missing flags to 0 so cohort logic doesn’t blow up
    master["flag_10_weights_each_billable_month"] = master["flag_10_weights_each_billable_month"].fillna(0).astype(int)
    master["flag_5_bp_each_billable_month"] = master["flag_5_bp_each_billable_month"].fillna(0).astype(int)
    master["flag_active_glp1_on_report_date"] = master["flag_active_glp1_on_report_date"].fillna(0).astype(int)
    master["a1c_labs_12mo"] = master["a1c_labs_12mo"].fillna(0).astype(int)

    # Baseline clinical gates used in the SLA cohort definitions
    master["flag_baseline_bmi_ge_30"] = (pd.to_numeric(master["baseline_bmi"], errors="coerce") >= BMI_THRESHOLD).astype(int)
    master["flag_baseline_bp_140_90"] = (
        (pd.to_numeric(master["baseline_systolic"], errors="coerce") >= 140) |
        (pd.to_numeric(master["baseline_diastolic"], errors="coerce") >= 90)
    ).astype(int)
    master["flag_baseline_a1c_gt_9"] = (pd.to_numeric(master["baseline_a1c"], errors="coerce") > 9).astype(int)

    # Simple baseline-to-current deltas (sheet wants baseline/current + change)
    master["weight_delta_lbs"] = master["baseline_weight_lbs"] - master["current_weight_lbs"]

    # 10) Cohorts for 12/2025 reporting (ignoring bariatric rows)
    master["cohort_wl_bmi30_not_on_glp"] = (
        (master["flag_baseline_bmi_ge_30"] == 1) &
        (master["flag_active_glp1_on_report_date"] == 0) &
        (master["flag_10_weights_each_billable_month"] == 1)
    ).astype(int)

    master["cohort_wl_bmi30_on_glp"] = (
        (master["flag_baseline_bmi_ge_30"] == 1) &
        (master["flag_active_glp1_on_report_date"] == 1) &
        (master["flag_10_weights_each_billable_month"] == 1)
    ).astype(int)

    master["cohort_bp_baseline_140_90"] = (
        (master["flag_baseline_bp_140_90"] == 1) &
        (master["flag_5_bp_each_billable_month"] == 1)
    ).astype(int)

    master["cohort_a1c_baseline_gt_9"] = (
        (master["flag_baseline_a1c_gt_9"] == 1) &
        (master["a1c_labs_12mo"] >= A1C_MIN_LABS_IN_12MO)
    ).astype(int)

    # 11) Rollups that mirror the spreadsheet (avg/median + sample size)
    summary = pd.DataFrame([
        summarize_weight(
            master[master["cohort_wl_bmi30_not_on_glp"] == 1],
            "Weight Loss – BMI>=30 NOT on GLP (tenure>=90; 10 weights each Billable Month)"
        ),
        summarize_weight(
            master[master["cohort_wl_bmi30_on_glp"] == 1],
            "Weight Loss – BMI>=30 ON GLP (tenure>=90; 10 weights each Billable Month)"
        ),
        summarize_bp(
            master[master["cohort_bp_baseline_140_90"] == 1],
            "BP Reduction – baseline >=140/90 (tenure>=90; 5 BPs each Billable Month)"
        ),
        summarize_a1c(
            master[master["cohort_a1c_baseline_gt_9"] == 1],
            "A1C Reduction – baseline > 9 (tenure>=90; >=2 A1C labs in 12 months)"
        )
    ])

    # 12) Write both a QA-friendly user extract and the final rollup table
    user_out = f"apple_pg_user_level_{partner}_{report_date}.csv"
    summary_out = f"apple_pg_summary_{partner}_{report_date}.csv"
    master.to_csv(user_out, index=False)
    summary.to_csv(summary_out, index=False)

    print(f"\n✅ Saved user-level: {user_out} ({len(master):,} users)")
    print(f"✅ Saved summary:   {summary_out} ({len(summary):,} rows)")

if __name__ == "__main__":
    main()
