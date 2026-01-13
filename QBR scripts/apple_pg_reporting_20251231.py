import mysql.connector
import time
import pandas as pd
import warnings
import numpy as np
from datetime import timedelta
from config import get_db_config

PARTNER = "Apple"
REPORT_DATE = "2025-12-31"

TENURE_MIN_DAYS = 90
BMI_THRESHOLD = 30

# Weight engagement (Option A): >=10 each of Oct/Nov/Dec 2025
WEIGHTS_PER_MONTH_MIN = 10
ENG_START = "2025-10-01"
ENG_END = "2025-12-31"
ENG_MONTHS = [pd.Period("2025-10", freq="M"), pd.Period("2025-11", freq="M"), pd.Period("2025-12", freq="M")]

# BP engagement: >=5 total in last 30 days (inclusive of report_date)
BP_LAST_N_DAYS = 30
BP_MIN_READINGS_LAST_N_DAYS = 5

# A1C cohort: baseline >= 9
A1C_MIN_LABS_IN_12MO = 2
A1C_BASELINE_THRESHOLD = 9.0  # >= 9

# Baseline window for WEIGHT/BMI/BP only
BASELINE_WINDOW_DAYS = 30

# Data guardrails (fixes out-of-bounds timestamps)
MIN_VALID_DATE = "2000-01-01"

GLP1_NAME_LIKE = ["%Wegovy%", "%Zepbound%", "%Ozempic%", "%Mounjaro%"]


# -----------------------
# DB helpers
# -----------------------
def connect_to_db():
    return mysql.connector.connect(**get_db_config())

def get_data(query, description):
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
# Utility: safe datetime conversion
# -----------------------
def coerce_dt(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return df
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df[df[col].notna()].copy()


# -----------------------
# Baselines
# -----------------------
def baseline_within_window(meas_df: pd.DataFrame,
                           start_df: pd.DataFrame,
                           value_col: str,
                           meas_date_col: str,
                           start_col: str = "start_date",
                           window_days: int = 30,
                           outcome_prefix: str = "baseline") -> pd.DataFrame:
    """
    Baseline = earliest measurement within [start_date - window_days, start_date + window_days].
    Used for weight/BMI/BP.
    """
    if meas_df.empty:
        return pd.DataFrame(columns=["user_id", f"{outcome_prefix}_{value_col}", f"{outcome_prefix}_{value_col}_date"])

    m = meas_df.copy()
    m = coerce_dt(m, meas_date_col)

    s = start_df[["user_id", start_col]].copy()
    s = coerce_dt(s, start_col)

    m = m.merge(s, on="user_id", how="inner")
    m["win_start"] = m[start_col] - pd.to_timedelta(window_days, unit="D")
    m["win_end"] = m[start_col] + pd.to_timedelta(window_days, unit="D")

    m = m[(m[meas_date_col] >= m["win_start"]) & (m[meas_date_col] <= m["win_end"])]

    if m.empty:
        return pd.DataFrame(columns=["user_id", f"{outcome_prefix}_{value_col}", f"{outcome_prefix}_{value_col}_date"])

    m = m.sort_values(["user_id", meas_date_col], ascending=[True, True])
    b = m.groupby("user_id", as_index=False).first()

    b = b.rename(columns={
        value_col: f"{outcome_prefix}_{value_col}",
        meas_date_col: f"{outcome_prefix}_{value_col}_date"
    })
    return b[["user_id", f"{outcome_prefix}_{value_col}", f"{outcome_prefix}_{value_col}_date"]]


def baseline_after_start(meas_df: pd.DataFrame,
                         start_df: pd.DataFrame,
                         value_col: str,
                         meas_date_col: str,
                         start_col: str = "start_date",
                         outcome_prefix: str = "baseline") -> pd.DataFrame:
    """
    Baseline = earliest measurement ON/AFTER start_date.
    Used for A1C (labs often happen well after start).
    """
    if meas_df.empty:
        return pd.DataFrame(columns=["user_id", f"{outcome_prefix}_{value_col}", f"{outcome_prefix}_{value_col}_date"])

    m = meas_df.copy()
    m = coerce_dt(m, meas_date_col)

    s = start_df[["user_id", start_col]].copy()
    s = coerce_dt(s, start_col)

    m = m.merge(s, on="user_id", how="inner")
    m = m[m[meas_date_col] >= m[start_col]]

    if m.empty:
        return pd.DataFrame(columns=["user_id", f"{outcome_prefix}_{value_col}", f"{outcome_prefix}_{value_col}_date"])

    m = m.sort_values(["user_id", meas_date_col], ascending=[True, True])
    b = m.groupby("user_id", as_index=False).first()

    b = b.rename(columns={
        value_col: f"{outcome_prefix}_{value_col}",
        meas_date_col: f"{outcome_prefix}_{value_col}_date"
    })
    return b[["user_id", f"{outcome_prefix}_{value_col}", f"{outcome_prefix}_{value_col}_date"]]


def latest_as_of(meas_df: pd.DataFrame, value_col: str, date_col: str, outcome_prefix: str = "current"):
    if meas_df.empty:
        return pd.DataFrame(columns=["user_id", f"{outcome_prefix}_{value_col}", f"{outcome_prefix}_{value_col}_date"])

    m = meas_df.copy()
    m = coerce_dt(m, date_col)
    m = m.sort_values(["user_id", date_col], ascending=[True, False])
    l = m.groupby("user_id", as_index=False).first()

    l = l.rename(columns={
        value_col: f"{outcome_prefix}_{value_col}",
        date_col: f"{outcome_prefix}_{value_col}_date"
    })
    return l[["user_id", f"{outcome_prefix}_{value_col}", f"{outcome_prefix}_{value_col}_date"]]


# -----------------------
# Engagement flags
# -----------------------
def monthly_threshold_flag(meas_df: pd.DataFrame, date_col: str, threshold: int, label: str) -> pd.DataFrame:
    """
    Flag = 1 if user has >= threshold in EACH of Oct/Nov/Dec 2025.
    Missing month => fail.
    """
    if meas_df.empty:
        return pd.DataFrame(columns=["user_id", f"flag_{label}_oct_nov_dec"])

    m = meas_df.copy()
    m = coerce_dt(m, date_col)
    m = m[(m[date_col] >= pd.to_datetime(ENG_START)) & (m[date_col] <= pd.to_datetime(ENG_END))].copy()

    if m.empty:
        return pd.DataFrame(columns=["user_id", f"flag_{label}_oct_nov_dec"])

    m["period"] = m[date_col].dt.to_period("M")
    counts = m.groupby(["user_id", "period"]).size().reset_index(name="cnt")

    all_users = pd.DataFrame({"user_id": m["user_id"].unique()})
    grid = all_users.assign(key=1).merge(
        pd.DataFrame({"period": ENG_MONTHS, "key": 1}), on="key", how="outer"
    ).drop(columns=["key"])

    grid = grid.merge(counts, on=["user_id", "period"], how="left")
    grid["cnt"] = grid["cnt"].fillna(0)

    grid["met"] = (grid["cnt"] >= threshold).astype(int)
    agg = grid.groupby("user_id").agg(months_met=("met", "sum")).reset_index()
    agg[f"flag_{label}_oct_nov_dec"] = (agg["months_met"] == 3).astype(int)

    return agg[["user_id", f"flag_{label}_oct_nov_dec"]]


def last_n_days_total_flag(meas_df: pd.DataFrame, date_col: str, threshold: int, label: str, report_date: str, n_days: int):
    """
    Flag = 1 if user has >= threshold total measurements in [report_date - (n_days-1), report_date].
    For n_days=30 and report_date=12/31, window is 12/02–12/31 inclusive.
    """
    if meas_df.empty:
        return pd.DataFrame(columns=["user_id", f"flag_{label}_last_{n_days}_days"])

    m = meas_df.copy()
    m = coerce_dt(m, date_col)

    r = pd.to_datetime(report_date)
    start = r - pd.to_timedelta(n_days - 1, unit="D")
    m = m[(m[date_col] >= start) & (m[date_col] <= r)].copy()

    if m.empty:
        return pd.DataFrame(columns=["user_id", f"flag_{label}_last_{n_days}_days"])

    counts = m.groupby("user_id").size().reset_index(name="cnt")
    counts[f"flag_{label}_last_{n_days}_days"] = (counts["cnt"] >= threshold).astype(int)
    return counts[["user_id", f"flag_{label}_last_{n_days}_days"]]


# -----------------------
# GLP active-as-of report date
# -----------------------
def compute_glp1_active_as_of(df_glp1: pd.DataFrame, report_date: str) -> pd.DataFrame:
    if df_glp1.empty:
        return pd.DataFrame(columns=["user_id", "flag_active_glp1_on_report_date", "last_rx_end_date"])

    r = df_glp1.copy()
    r = coerce_dt(r, "prescribed_at")
    r["total_refills"] = pd.to_numeric(r["total_refills"], errors="coerce").fillna(0)
    r["days_of_supply"] = pd.to_numeric(r["days_of_supply"], errors="coerce").fillna(0)

    r["rx_days_coverage"] = r["days_of_supply"] * (1 + r["total_refills"])
    r["rx_end_date"] = r["prescribed_at"] + pd.to_timedelta(r["rx_days_coverage"], unit="D")

    g = r.groupby("user_id").agg(last_rx_end_date=("rx_end_date", "max")).reset_index()
    report_dt = pd.to_datetime(report_date)
    g["flag_active_glp1_on_report_date"] = (g["last_rx_end_date"] >= report_dt).astype(int)
    return g[["user_id", "flag_active_glp1_on_report_date", "last_rx_end_date"]]


# -----------------------
# Summaries
# -----------------------
def summarize_weight_pct(df_in: pd.DataFrame, label: str) -> dict:
    d = df_in.copy()
    d = d[d["baseline_weight_lbs"].notna() & d["current_weight_lbs"].notna()].copy()
    if d.empty:
        return {"Metric": label, "avg_weight_loss_pct": np.nan, "median_weight_loss_pct": np.nan, "avg_lbs_lost": np.nan, "sample_size": 0}

    d["weight_loss_lbs"] = d["baseline_weight_lbs"] - d["current_weight_lbs"]
    d["weight_loss_pct"] = d["weight_loss_lbs"] / d["baseline_weight_lbs"]

    return {
        "Metric": label,
        "avg_weight_loss_pct": float(d["weight_loss_pct"].mean()),
        "median_weight_loss_pct": float(d["weight_loss_pct"].median()),
        "avg_lbs_lost": float(d["weight_loss_lbs"].mean()),
        "sample_size": int(d["user_id"].nunique())
    }

def summarize_a1c(df_in: pd.DataFrame, label: str) -> dict:
    d = df_in.copy()
    d = d[d["baseline_a1c"].notna() & d["current_a1c"].notna()].copy()
    if d.empty:
        return {"Metric": label, "avg_baseline_a1c": np.nan, "avg_current_a1c": np.nan, "avg_delta_a1c": np.nan, "sample_size": 0}

    delta = d["baseline_a1c"] - d["current_a1c"]
    return {
        "Metric": label,
        "avg_baseline_a1c": float(d["baseline_a1c"].mean()),
        "avg_current_a1c": float(d["current_a1c"].mean()),
        "avg_delta_a1c": float(delta.mean()),
        "sample_size": int(d["user_id"].nunique())
    }

def summarize_bp_improvement(df_in: pd.DataFrame, label: str) -> dict:
    d = df_in.copy()
    if d.empty:
        return {"Metric": label, "eligible_n": 0, "pct_improved": np.nan, "avg_sbp_reduction": np.nan, "avg_dbp_reduction": np.nan}

    d["sbp_reduction"] = d["baseline_systolic"] - d["latest_systolic"]
    d["dbp_reduction"] = d["baseline_diastolic"] - d["latest_diastolic"]
    d["improved_flag"] = ((d["sbp_reduction"] >= 10) | (d["dbp_reduction"] >= 5)).astype(int)

    return {
        "Metric": label,
        "eligible_n": int(d["user_id"].nunique()),
        "pct_improved": float(d["improved_flag"].mean()),
        "avg_sbp_reduction": float(d["sbp_reduction"].mean()),
        "avg_dbp_reduction": float(d["dbp_reduction"].mean()),
    }


def main(partner=PARTNER, report_date=REPORT_DATE):
    print(f"\n🚀 Apple PG Clinical — Partner={partner} As-Of={report_date}")

    # 1) Base cohort = ACTIVE as-of report_date + tenure >= 90
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

    base_users["start_date"] = pd.to_datetime(base_users["start_date"], errors="coerce")
    base_users = base_users[base_users["start_date"].notna()].copy()

    # IN (...) list for binary ids
    raw_ids = base_users["user_id"].tolist()
    if raw_ids and isinstance(raw_ids[0], (bytes, bytearray)):
        formatted_ids = ", ".join([f"0x{uid.hex()}" for uid in raw_ids])
    else:
        formatted_ids = str(tuple(raw_ids))
        if len(raw_ids) == 1:
            formatted_ids = formatted_ids.replace(",", "")

    # 2) Measures (cohort only) with date guardrails
    df_weights = get_data(f"""
        SELECT user_id, effective_date, value * 2.20462 AS weight_lbs
        FROM body_weight_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date BETWEEN '{MIN_VALID_DATE}' AND '{report_date}'
          AND value IS NOT NULL
    """, "Weight logs (cohort)")

    df_bmi = get_data(f"""
        SELECT user_id, effective_date, value AS bmi
        FROM bmi_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date BETWEEN '{MIN_VALID_DATE}' AND '{report_date}'
          AND value IS NOT NULL
    """, "BMI logs (cohort)")

    df_bp = get_data(f"""
        SELECT user_id, effective_date, systolic, diastolic
        FROM blood_pressure_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date BETWEEN '{MIN_VALID_DATE}' AND '{report_date}'
          AND systolic IS NOT NULL
          AND diastolic IS NOT NULL
    """, "BP logs (cohort)")

    df_a1c = get_data(f"""
        SELECT user_id, effective_date, value AS a1c
        FROM a1c_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date BETWEEN '{MIN_VALID_DATE}' AND '{report_date}'
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
          AND p.prescribed_at BETWEEN '{MIN_VALID_DATE}' AND '{report_date}'
          AND ({glp_like})
    """, "GLP1 Rx (cohort)")

    # 3) Coerce dates safely
    df_weights = coerce_dt(df_weights, "effective_date")
    df_bmi = coerce_dt(df_bmi, "effective_date")
    df_bp = coerce_dt(df_bp, "effective_date")
    df_a1c = coerce_dt(df_a1c, "effective_date")
    df_glp1 = coerce_dt(df_glp1, "prescribed_at")

    # 4) Baselines
    base_weight = baseline_within_window(df_weights, base_users, "weight_lbs", "effective_date",
                                         window_days=BASELINE_WINDOW_DAYS, outcome_prefix="baseline")
    base_weight = base_weight.rename(columns={"baseline_weight_lbs": "baseline_weight_lbs",
                                              "baseline_weight_lbs_date": "baseline_weight_date"})

    base_bmi = baseline_within_window(df_bmi, base_users, "bmi", "effective_date",
                                      window_days=BASELINE_WINDOW_DAYS, outcome_prefix="baseline")
    base_bmi = base_bmi.rename(columns={"baseline_bmi": "baseline_bmi",
                                        "baseline_bmi_date": "baseline_bmi_date"})

    # A1C baseline: first ON/AFTER start_date
    base_a1c = baseline_after_start(df_a1c, base_users, "a1c", "effective_date", outcome_prefix="baseline")
    base_a1c = base_a1c.rename(columns={"baseline_a1c": "baseline_a1c",
                                        "baseline_a1c_date": "baseline_a1c_date"})

    # 5) Current/latest values as-of report_date
    cur_weight = latest_as_of(df_weights, "weight_lbs", "effective_date", outcome_prefix="current").rename(
        columns={"current_weight_lbs": "current_weight_lbs", "current_weight_lbs_date": "current_weight_date"}
    )
    cur_a1c = latest_as_of(df_a1c, "a1c", "effective_date", outcome_prefix="current").rename(
        columns={"current_a1c": "current_a1c", "current_a1c_date": "current_a1c_date"}
    )

    # BP baseline within window + latest BP
    if not df_bp.empty:
        bp = df_bp.merge(base_users[["user_id", "start_date"]], on="user_id", how="inner")
        bp["win_start"] = bp["start_date"] - pd.to_timedelta(BASELINE_WINDOW_DAYS, unit="D")
        bp["win_end"] = bp["start_date"] + pd.to_timedelta(BASELINE_WINDOW_DAYS, unit="D")

        bp_win = bp[(bp["effective_date"] >= bp["win_start"]) & (bp["effective_date"] <= bp["win_end"])].copy()
        if not bp_win.empty:
            bp_win = bp_win.sort_values(["user_id", "effective_date"], ascending=[True, True])
            bp_base = bp_win.groupby("user_id", as_index=False).first().rename(columns={
                "systolic": "baseline_systolic",
                "diastolic": "baseline_diastolic",
                "effective_date": "baseline_bp_date"
            })[["user_id", "baseline_systolic", "baseline_diastolic", "baseline_bp_date"]]
        else:
            bp_base = pd.DataFrame(columns=["user_id", "baseline_systolic", "baseline_diastolic", "baseline_bp_date"])

        bp_latest = df_bp.sort_values(["user_id", "effective_date"], ascending=[True, False]).groupby("user_id", as_index=False).first()
        bp_latest = bp_latest.rename(columns={
            "systolic": "latest_systolic",
            "diastolic": "latest_diastolic",
            "effective_date": "latest_bp_date"
        })[["user_id", "latest_systolic", "latest_diastolic", "latest_bp_date"]]

        # Guard: don't let "latest" be earlier than baseline
        bp_merged = bp_base.merge(bp_latest, on="user_id", how="left")
        bp_merged["latest_ok"] = (
            bp_merged["latest_bp_date"].notna()
            & bp_merged["baseline_bp_date"].notna()
            & (pd.to_datetime(bp_merged["latest_bp_date"]) >= pd.to_datetime(bp_merged["baseline_bp_date"]))
        )
        bp_merged.loc[~bp_merged["latest_ok"], ["latest_systolic", "latest_diastolic", "latest_bp_date"]] = np.nan

        bp_base = bp_merged[["user_id", "baseline_systolic", "baseline_diastolic", "baseline_bp_date"]]
        bp_latest = bp_merged[["user_id", "latest_systolic", "latest_diastolic", "latest_bp_date"]]
    else:
        bp_base = pd.DataFrame(columns=["user_id","baseline_systolic","baseline_diastolic","baseline_bp_date"])
        bp_latest = pd.DataFrame(columns=["user_id","latest_systolic","latest_diastolic","latest_bp_date"])

    # 6) Engagement flags
    weight_eng = monthly_threshold_flag(df_weights, "effective_date", WEIGHTS_PER_MONTH_MIN, "10_weights")
    weight_eng = weight_eng.rename(columns={"flag_10_weights_oct_nov_dec": "flag_10_weights_oct_nov_dec"})

    bp_eng_30d = last_n_days_total_flag(df_bp, "effective_date",
                                        BP_MIN_READINGS_LAST_N_DAYS, "5_bp",
                                        report_date, BP_LAST_N_DAYS)
    bp_eng_30d = bp_eng_30d.rename(columns={f"flag_5_bp_last_{BP_LAST_N_DAYS}_days": "flag_5_bp_last_30_days"})

    # 7) GLP active as-of report_date
    glp_active = compute_glp1_active_as_of(df_glp1, report_date)

    # 8) A1C labs in last 12 months
    report_dt = pd.to_datetime(report_date)
    start_12mo = report_dt - timedelta(days=365)
    if not df_a1c.empty:
        a1c_12mo = df_a1c[(df_a1c["effective_date"] >= start_12mo) & (df_a1c["effective_date"] <= report_dt)]
        a1c_counts = a1c_12mo.groupby("user_id")["effective_date"].nunique().reset_index(name="a1c_labs_12mo")
    else:
        a1c_counts = pd.DataFrame(columns=["user_id", "a1c_labs_12mo"])

    # 9) Master merge
    master = base_users.copy()
    for t in [base_weight, cur_weight, base_bmi, base_a1c, cur_a1c, bp_base, bp_latest, weight_eng, bp_eng_30d, glp_active, a1c_counts]:
        master = master.merge(t, on="user_id", how="left")

    master["flag_10_weights_oct_nov_dec"] = master["flag_10_weights_oct_nov_dec"].fillna(0).astype(int)
    master["flag_5_bp_last_30_days"] = master["flag_5_bp_last_30_days"].fillna(0).astype(int)
    master["flag_active_glp1_on_report_date"] = master["flag_active_glp1_on_report_date"].fillna(0).astype(int)
    master["a1c_labs_12mo"] = master["a1c_labs_12mo"].fillna(0).astype(int)

    # BMI flags
    bmi_num = pd.to_numeric(master["baseline_bmi"], errors="coerce")
    master["flag_baseline_bmi_ge_30"] = (bmi_num >= BMI_THRESHOLD).astype(int)
    master["flag_baseline_bmi_lt_30"] = (bmi_num < BMI_THRESHOLD).astype(int)

    # Weight loss %
    master["weight_loss_lbs"] = master["baseline_weight_lbs"] - master["current_weight_lbs"]
    master["weight_loss_pct"] = master["weight_loss_lbs"] / master["baseline_weight_lbs"]

    # Uncontrolled BP recycled logic + improvement
    master["is_uncontrolled_bp"] = (
        ((pd.to_numeric(master["baseline_systolic"], errors="coerce") >= 140) |
         (pd.to_numeric(master["baseline_diastolic"], errors="coerce") >= 90)) &
        master["baseline_systolic"].notna() & master["latest_systolic"].notna() &
        master["baseline_bp_date"].notna() & master["latest_bp_date"].notna() &
        ((pd.to_datetime(master["latest_bp_date"]) - pd.to_datetime(master["baseline_bp_date"])).dt.days >= 30)
    ).astype(int)

    master["sbp_reduction"] = master["baseline_systolic"] - master["latest_systolic"]
    master["dbp_reduction"] = master["baseline_diastolic"] - master["latest_diastolic"]
    master["flag_bp_improved_10_5"] = ((master["sbp_reduction"] >= 10) | (master["dbp_reduction"] >= 5)).astype(int)

    # 10) Cohorts (overlap is OK)
    master["cohort_wl_bmi30_not_on_glp"] = (
        (master["flag_10_weights_oct_nov_dec"] == 1) &
        (master["flag_baseline_bmi_ge_30"] == 1) &
        (master["flag_active_glp1_on_report_date"] == 0)
    ).astype(int)

    master["cohort_wl_bmi30_on_glp"] = (
        (master["flag_10_weights_oct_nov_dec"] == 1) &
        (master["flag_baseline_bmi_ge_30"] == 1) &
        (master["flag_active_glp1_on_report_date"] == 1)
    ).astype(int)

    # BMI<30 cohort DOES NOT require weight engagement
    master["cohort_wl_bmi_lt30_any_glp"] = (master["flag_baseline_bmi_lt_30"] == 1).astype(int)

    # BP cohort: uncontrolled baseline + >=5 BP readings in last 30 days
    master["cohort_bp_uncontrolled"] = (
        (master["is_uncontrolled_bp"] == 1) &
        (master["flag_5_bp_last_30_days"] == 1)
    ).astype(int)

    # A1C cohort: baseline >=9, >=2 labs in last 12 mo, >=30 days between baseline/current
    master["cohort_a1c_baseline_ge_9"] = (
        (pd.to_numeric(master["baseline_a1c"], errors="coerce") >= A1C_BASELINE_THRESHOLD) &
        (master["a1c_labs_12mo"] >= A1C_MIN_LABS_IN_12MO) &
        master["current_a1c"].notna() &
        ((pd.to_datetime(master["current_a1c_date"], errors="coerce") - pd.to_datetime(master["baseline_a1c_date"], errors="coerce")).dt.days >= 30)
    ).astype(int)

    # 11) Summary output
    summary = pd.DataFrame([
        summarize_weight_pct(master[master["cohort_wl_bmi30_not_on_glp"] == 1],
                             "Weight Loss – BMI>=30 NOT on GLP (10 weights in Oct/Nov/Dec)"),
        summarize_weight_pct(master[master["cohort_wl_bmi30_on_glp"] == 1],
                             "Weight Loss – BMI>=30 ON GLP (10 weights in Oct/Nov/Dec)"),
        summarize_weight_pct(master[master["cohort_wl_bmi_lt30_any_glp"] == 1],
                             "Weight Loss – BMI<30 (any GLP; no weight engagement requirement)"),
        summarize_bp_improvement(master[master["cohort_bp_uncontrolled"] == 1],
                                 "BP Reduction – baseline >=140/90 + >=5 BP in last 30 days; improved (>=10 SBP or >=5 DBP)"),
        summarize_a1c(master[master["cohort_a1c_baseline_ge_9"] == 1],
                      "A1C Reduction – baseline >=9 + >=2 labs in 12 mo")
    ])

    # 12) Exports
    user_out = f"apple_pg_user_level_{partner}_{report_date}.csv"
    summary_out = f"apple_pg_summary_{partner}_{report_date}.csv"
    master.to_csv(user_out, index=False)
    summary.to_csv(summary_out, index=False)

    print(f"\n✅ Saved user-level: {user_out} ({len(master):,} users)")
    print(f"✅ Saved summary:   {summary_out} ({len(summary):,} rows)")

    # Gate checks
    def gate_counts(df, name, mask):
        print(f"{name}: {int(mask.sum()):,}")

    print("\n🔎 Gate checks")
    gate_counts(master, "BMI<30 cohort", master["cohort_wl_bmi_lt30_any_glp"] == 1)
    gate_counts(master, "BP engaged last 30 days", master["flag_5_bp_last_30_days"] == 1)
    gate_counts(master, "Uncontrolled baseline BP", master["is_uncontrolled_bp"] == 1)
    gate_counts(master, "BP cohort final", master["cohort_bp_uncontrolled"] == 1)
    gate_counts(master, "A1C baseline present", master["baseline_a1c"].notna())
    gate_counts(master, "A1C labs>=2 (12mo)", master["a1c_labs_12mo"] >= 2)
    gate_counts(master, "A1C baseline>=9", pd.to_numeric(master["baseline_a1c"], errors="coerce") >= A1C_BASELINE_THRESHOLD)
    gate_counts(master, "A1C cohort final", master["cohort_a1c_baseline_ge_9"] == 1)


if __name__ == "__main__":
    main()
