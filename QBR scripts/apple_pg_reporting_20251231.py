# Pull PGs on Q4 2025. Change 'partner' to client of interest. Outputs member level csv, summary csv
import mysql.connector
import time
import pandas as pd
import warnings
import numpy as np
from datetime import timedelta
from config import get_db_config

# -----------------------
# DB helpers
# -----------------------
def connect_to_db():
    config = get_db_config()
    return mysql.connector.connect(**config)

def get_data(query, description):
    """Fetch data into a pandas DataFrame"""
    start_time = time.time()
    conn = connect_to_db()
    try:
        warnings.filterwarnings("ignore", category=UserWarning)
        print(f"  📥 Fetching {description}...")
        df = pd.read_sql(query, conn)
        duration = time.time() - start_time
        print(f"    ⏱️  {description}: {len(df)} rows in {duration:.2f}s")
        return df
    finally:
        conn.close()

# -----------------------
# Baseline / latest helpers
# -----------------------
def calculate_baseline(df, value_col, date_col="effective_date", outcome_name="baseline"):
    """FIRST value per user by date"""
    if df.empty:
        return pd.DataFrame(columns=["user_id", outcome_name, f"{outcome_name}_date"])

    df_sorted = df.sort_values(by=["user_id", date_col], ascending=[True, True])
    baseline = df_sorted.groupby("user_id").first().reset_index()
    baseline = baseline.rename(columns={value_col: outcome_name, date_col: f"{outcome_name}_date"})
    return baseline[["user_id", outcome_name, f"{outcome_name}_date"]]

def calculate_latest(df, value_col, date_col="effective_date", outcome_name="latest"):
    """LAST value per user by date"""
    if df.empty:
        return pd.DataFrame(columns=["user_id", outcome_name, f"{outcome_name}_date"])

    df_sorted = df.sort_values(by=["user_id", date_col], ascending=[True, False])
    latest = df_sorted.groupby("user_id").first().reset_index()
    latest = latest.rename(columns={value_col: outcome_name, date_col: f"{outcome_name}_date"})
    return latest[["user_id", outcome_name, f"{outcome_name}_date"]]

# -----------------------
# STRICT: "each Billable Month" engagement
# -----------------------
# def strict_monthly_threshold_all_active_months(
#     df_events: pd.DataFrame,
#     df_users: pd.DataFrame,
#     user_col: str,
#     event_date_col: str,
#     start_date_col: str,
#     end_date_str: str,
#     threshold: int,
#     out_col: str,
# ) -> pd.DataFrame:
#     """
#     Flag=1 if user has >= threshold events in EVERY month from:
#       first full month after start_date
#     through:
#       report month (end_date_str)
#     Months with 0 events count as 0 (fail).
#     """
#     if df_users.empty:
#         return pd.DataFrame(columns=[user_col, out_col])

#     end_dt = pd.to_datetime(end_date_str)
#     end_period = pd.Period(end_dt, freq="M")

#     u = df_users[[user_col, start_date_col]].copy()
#     u[start_date_col] = pd.to_datetime(u[start_date_col], errors="coerce")
#     u = u.dropna(subset=[start_date_col])

#     # First billable month = next month begin
#     u["start_period"] = (u[start_date_col] + pd.offsets.MonthBegin(1)).dt.to_period("M")
#     u["end_period"] = end_period

#     # months_in_window inclusive
#     u["months_in_window"] = (u["end_period"] - u["start_period"]).apply(lambda x: x.n + 1 if pd.notnull(x) else 0)
#     u.loc[u["start_period"] > u["end_period"], "months_in_window"] = 0
#     u["months_in_window"] = u["months_in_window"].fillna(0).astype(int)

#     # If no events at all, everyone fails (unless you want "no eligible months" => 0)
#     if df_events.empty:
#         u[out_col] = 0
#         return u[[user_col, out_col]]

#     e = df_events[[user_col, event_date_col]].copy()
#     e[event_date_col] = pd.to_datetime(e[event_date_col], errors="coerce")
#     e = e.dropna(subset=[event_date_col])
#     e["period"] = e[event_date_col].dt.to_period("M")

#     # Join window bounds and filter per user
#     e = e.merge(u[[user_col, "start_period", "end_period"]], on=user_col, how="inner")
#     e = e[(e["period"] >= e["start_period"]) & (e["period"] <= e["end_period"])]

#     counts = e.groupby([user_col, "period"]).size().reset_index(name="cnt")

#     # Build per-user month grid
#     rows = []
#     for _, r in u.iterrows():
#         if r["months_in_window"] <= 0:
#             continue
#         periods = pd.period_range(r["start_period"], r["end_period"], freq="M")
#         rows.append(pd.DataFrame({user_col: r[user_col], "period": periods}))

#     if not rows:
#         out = u[[user_col]].copy()
#         out[out_col] = 0
#         return out

#     grid = pd.concat(rows, ignore_index=True)
#     grid = grid.merge(counts, on=[user_col, "period"], how="left")
#     grid["cnt"] = grid["cnt"].fillna(0)

#     grid["met"] = (grid["cnt"] >= threshold).astype(int)
#     agg = grid.groupby(user_col).agg(months_met=("met", "sum"), months_total=("met", "size")).reset_index()
#     agg[out_col] = (agg["months_met"] == agg["months_total"]).astype(int)

#     return agg[[user_col, out_col]]

def strict_monthly_threshold_billable_months(
    df_events: pd.DataFrame,
    df_users: pd.DataFrame,
    df_billable_months: pd.DataFrame,
    user_col: str,
    event_date_col: str,
    start_date_col: str,
    end_date_str: str,
    threshold: int,
    out_col: str,
) -> pd.DataFrame:
    """
    Flag=1 if user has >= threshold events in EVERY *billable* month from:
      first full month after start_date
    through:
      report month (end_date_str)
    Non-billable months are ignored (not required).
    Billable months come from billable_user_statuses.is_billable=1 (any day in month).
    """
    if df_users.empty:
        return pd.DataFrame(columns=[user_col, out_col])

    end_dt = pd.to_datetime(end_date_str)
    end_period = pd.Period(end_dt, freq="M")

    u = df_users[[user_col, start_date_col]].copy()
    u[start_date_col] = pd.to_datetime(u[start_date_col], errors="coerce")
    u = u.dropna(subset=[start_date_col]).copy()

    # Start counting from first full month after signup
    u["start_period"] = (u[start_date_col] + pd.offsets.MonthBegin(1)).dt.to_period("M")
    u["end_period"] = end_period

    # Required months = billable months within [start_period, end_period]
    bm = df_billable_months.copy()
    if bm.empty:
        out = u[[user_col]].copy()
        out[out_col] = 0
        return out[[user_col, out_col]]

    bm = bm.merge(u[[user_col, "start_period", "end_period"]], on=user_col, how="inner")
    bm = bm[(bm["period"] >= bm["start_period"]) & (bm["period"] <= bm["end_period"])].copy()

    # How many required billable months?
    required = bm.groupby(user_col)["period"].nunique().reset_index(name="required_billable_months")

    # If a user has 0 billable months in-window, decide behavior:
    # Usually: fail the strict rule (since there are no required months to validate)
    # If you prefer "no billable months => pass", flip this later.
    if df_events.empty:
        out = u[[user_col]].merge(required, on=user_col, how="left")
        out["required_billable_months"] = out["required_billable_months"].fillna(0).astype(int)
        out[out_col] = 0
        return out[[user_col, out_col]]

    # Monthly event counts
    e = df_events[[user_col, event_date_col]].copy()
    e[event_date_col] = pd.to_datetime(e[event_date_col], errors="coerce")
    e = e.dropna(subset=[event_date_col]).copy()
    e["period"] = e[event_date_col].dt.to_period("M")

    counts = e.groupby([user_col, "period"]).size().reset_index(name="cnt")

    # Evaluate ONLY billable months (months with 0 events become cnt=0 and fail)
    check = bm.merge(counts, on=[user_col, "period"], how="left")
    check["cnt"] = check["cnt"].fillna(0)
    check["met"] = (check["cnt"] >= threshold).astype(int)

    met = check.groupby(user_col)["met"].sum().reset_index(name="billable_months_met")

    out = (
        u[[user_col]]
        .merge(required, on=user_col, how="left")
        .merge(met, on=user_col, how="left")
    )
    out["required_billable_months"] = out["required_billable_months"].fillna(0).astype(int)
    out["billable_months_met"] = out["billable_months_met"].fillna(0).astype(int)

    out[out_col] = (
        (out["required_billable_months"] > 0) &
        (out["billable_months_met"] == out["required_billable_months"])
    ).astype(int)

    return out[[user_col, out_col]]

# -----------------------
# BP standard engagement helper (avg/month from month after signup)
# -----------------------
def calculate_avg_monthly_events_full_months(
    df_events,
    df_users,
    user_col="user_id",
    event_date_col="effective_date",
    start_date_col="start_date",
    window_end="2025-12-31",
    avg_col_name="avg_monthly_bp_checks",
    flag_col_name="meets_bp_engagement_rule",
    threshold=5,
):
    """
    Average monthly events from the first day of the month AFTER signup through window_end,
    counting months with zero events.
    """
    if df_users.empty:
        return pd.DataFrame(columns=[user_col, avg_col_name, flag_col_name])

    window_end = pd.to_datetime(window_end)

    u = df_users[[user_col, start_date_col]].copy()
    u[start_date_col] = pd.to_datetime(u[start_date_col], errors="coerce")

    u["window_start"] = (u[start_date_col] + pd.offsets.MonthBegin(1)).dt.normalize()
    u = u.dropna(subset=["window_start"])
    u["window_end"] = window_end

    start_y = u["window_start"].dt.year
    start_m = u["window_start"].dt.month
    end_y = u["window_end"].dt.year
    end_m = u["window_end"].dt.month

    u["months_in_window"] = (end_y - start_y) * 12 + (end_m - start_m) + 1
    u.loc[u["window_start"] > u["window_end"], "months_in_window"] = 0
    u["months_in_window"] = u["months_in_window"].fillna(0).astype(int)

    if df_events.empty:
        u[avg_col_name] = 0.0
        u[flag_col_name] = 0
        return u[[user_col, avg_col_name, flag_col_name]]

    e = df_events[[user_col, event_date_col]].copy()
    e[event_date_col] = pd.to_datetime(e[event_date_col], errors="coerce")
    e = e.dropna(subset=[event_date_col])

    e = e.merge(u[[user_col, "window_start", "window_end"]], on=user_col, how="inner")
    e = e[(e[event_date_col] >= e["window_start"]) & (e[event_date_col] <= e["window_end"])]

    total_counts = e.groupby(user_col).size().reset_index(name="total_events_in_window")
    out = u.merge(total_counts, on=user_col, how="left")
    out["total_events_in_window"] = out["total_events_in_window"].fillna(0)

    out[avg_col_name] = out.apply(
        lambda r: (r["total_events_in_window"] / r["months_in_window"]) if r["months_in_window"] > 0 else 0.0,
        axis=1,
    )
    out[flag_col_name] = (out[avg_col_name] >= threshold).astype(int)
    return out[[user_col, avg_col_name, flag_col_name]]

# -----------------------
# Summary helpers (WITH percent reductions)
# -----------------------
def summarize_pair(df_in: pd.DataFrame, label: str, baseline_col: str, current_col: str) -> dict:
    """
    Apple-sheet style columns + percent change
    pct_change = (baseline - current)/baseline
    """
    d = df_in[["user_id", baseline_col, current_col]].copy()
    d[baseline_col] = pd.to_numeric(d[baseline_col], errors="coerce")
    d[current_col] = pd.to_numeric(d[current_col], errors="coerce")
    d = d.dropna(subset=[baseline_col, current_col])
    d = d[d[baseline_col] > 0]

    if d.empty:
        return {
            "Metric": label,
            "avg_baseline": np.nan,
            "avg_current": np.nan,
            "median_baseline": np.nan,
            "median_current": np.nan,
            "delta_avg": np.nan,
            "delta_median": np.nan,
            "avg_pct_reduction": np.nan,
            "median_pct_reduction": np.nan,
            "sample_size": 0,
            "avg_systolic_reduction": np.nan,
            "avg_diastolic_reduction": np.nan,
            "median_systolic_reduction": np.nan,
            "median_diastolic_reduction": np.nan,
            "avg_systolic_reduction_pct": np.nan,
            "avg_diastolic_reduction_pct": np.nan,
            "median_systolic_reduction_pct": np.nan,
            "median_diastolic_reduction_pct": np.nan,
        }

    delta = d[baseline_col] - d[current_col]
    pct = delta / d[baseline_col]

    return {
        "Metric": label,
        "avg_baseline": float(d[baseline_col].mean()),
        "avg_current": float(d[current_col].mean()),
        "median_baseline": float(d[baseline_col].median()),
        "median_current": float(d[current_col].median()),
        "delta_avg": float(delta.mean()),
        "delta_median": float(delta.median()),
        "avg_pct_reduction": float(pct.mean()),
        "median_pct_reduction": float(pct.median()),
        "sample_size": int(d["user_id"].nunique()),
        # BP-only fields (kept for consistent columns)
        "avg_systolic_reduction": np.nan,
        "avg_diastolic_reduction": np.nan,
        "median_systolic_reduction": np.nan,
        "median_diastolic_reduction": np.nan,
        "avg_systolic_reduction_pct": np.nan,
        "avg_diastolic_reduction_pct": np.nan,
        "median_systolic_reduction_pct": np.nan,
        "median_diastolic_reduction_pct": np.nan,
    }

def summarize_bp(df_in: pd.DataFrame, label: str) -> dict:
    """
    Apple-style baseline/current/delta based on systolic,
    plus diastolic reductions and percent reductions.
    """
    cols = ["user_id", "baseline_systolic", "latest_systolic", "baseline_diastolic", "latest_diastolic"]
    d = df_in[cols].copy()

    for c in cols[1:]:
        d[c] = pd.to_numeric(d[c], errors="coerce")

    d = d.dropna(subset=["baseline_systolic", "latest_systolic", "baseline_diastolic", "latest_diastolic"])
    d = d[(d["baseline_systolic"] > 0) & (d["baseline_diastolic"] > 0)]

    if d.empty:
        return {
            "Metric": label,
            "avg_baseline": np.nan,
            "avg_current": np.nan,
            "median_baseline": np.nan,
            "median_current": np.nan,
            "delta_avg": np.nan,
            "delta_median": np.nan,
            "avg_pct_reduction": np.nan,
            "median_pct_reduction": np.nan,
            "sample_size": 0,
            "avg_systolic_reduction": np.nan,
            "avg_diastolic_reduction": np.nan,
            "median_systolic_reduction": np.nan,
            "median_diastolic_reduction": np.nan,
            "avg_systolic_reduction_pct": np.nan,
            "avg_diastolic_reduction_pct": np.nan,
            "median_systolic_reduction_pct": np.nan,
            "median_diastolic_reduction_pct": np.nan,
        }

    sbp_delta = d["baseline_systolic"] - d["latest_systolic"]
    dbp_delta = d["baseline_diastolic"] - d["latest_diastolic"]

    sbp_pct = sbp_delta / d["baseline_systolic"]
    dbp_pct = dbp_delta / d["baseline_diastolic"]

    # For the main Apple columns, keep systolic baseline/current/delta
    return {
        "Metric": label,
        "avg_baseline": float(d["baseline_systolic"].mean()),
        "avg_current": float(d["latest_systolic"].mean()),
        "median_baseline": float(d["baseline_systolic"].median()),
        "median_current": float(d["latest_systolic"].median()),
        "delta_avg": float(sbp_delta.mean()),
        "delta_median": float(sbp_delta.median()),
        # "overall pct" for the row: systolic pct reduction
        "avg_pct_reduction": float(sbp_pct.mean()),
        "median_pct_reduction": float(sbp_pct.median()),
        "sample_size": int(d["user_id"].nunique()),
        "avg_systolic_reduction": float(sbp_delta.mean()),
        "avg_diastolic_reduction": float(dbp_delta.mean()),
        "median_systolic_reduction": float(sbp_delta.median()),
        "median_diastolic_reduction": float(dbp_delta.median()),
        "avg_systolic_reduction_pct": float(sbp_pct.mean()),
        "avg_diastolic_reduction_pct": float(dbp_pct.mean()),
        "median_systolic_reduction_pct": float(sbp_pct.median()),
        "median_diastolic_reduction_pct": float(dbp_pct.median()),
    }

# -----------------------
# Main
# -----------------------
def main(partner="Apple", end_date="2025-12-31"):
    print(f"\n🚀 Starting Read-Only ETL for {partner} (End Date: {end_date})...")

    # 1) Base Users (users -> partner_employers -> subscriptions) + readable_id
    users_query = f"""
        SELECT
            u.id AS user_id,
            u.readable_id AS user_readable_id,
            MIN(s.start_date) AS start_date,
            MAX(s.cancellation_date) AS cancellation_date,
            DATEDIFF('{end_date}', MIN(s.start_date)) AS tenure_days
        FROM users u
        JOIN partner_employers pe
            ON pe.user_id = u.id
        JOIN subscriptions s
            ON s.user_id = u.id
        WHERE pe.name = '{partner}'
          AND s.start_date <= '{end_date}'
          -- Active as-of report date:
          AND (s.cancellation_date IS NULL OR s.cancellation_date > '{end_date}')
        GROUP BY u.id, u.readable_id
        HAVING DATEDIFF('{end_date}', MIN(s.start_date)) >= 90
    ;
    """

    df_users = get_data(users_query, "Base Users (Strict Active + Tenure>=90)")

    if df_users.empty:
        print("⚠️ No users found. Stopping.")
        return

    # --- Fix Binary IDs for IN (...) ---
    raw_ids = df_users["user_id"].tolist()
    if raw_ids and isinstance(raw_ids[0], (bytes, bytearray)):
        formatted_ids = ", ".join([f"0x{uid.hex()}" for uid in raw_ids])
    else:
        formatted_ids = str(tuple(raw_ids))
        if len(raw_ids) == 1:
            formatted_ids = formatted_ids.replace(",", "")

    # 2) Pull measures
    billable_query = f"""
        SELECT
            user_id,
            date,
            is_billable
        FROM billable_user_statuses
        WHERE user_id IN ({formatted_ids})
        AND partner = '{partner}'
        AND date <= '{end_date}'
    """
    df_billable = get_data(billable_query, "Billable User Statuses")
        
        # --- Billable months (month grain) ---
    if not df_billable.empty:
        df_billable = df_billable.copy()
        df_billable["date"] = pd.to_datetime(df_billable["date"], errors="coerce")
        df_billable = df_billable.dropna(subset=["date"]).copy()

        # month bucket
        df_billable["period"] = df_billable["date"].dt.to_period("M")

        # If ANY day in the month is billable, treat the whole month as billable-required
        billable_months = (
            df_billable.groupby(["user_id", "period"])["is_billable"]
            .max()
            .reset_index()
        )
        billable_months = billable_months[billable_months["is_billable"] == 1][["user_id", "period"]]
    else:
        billable_months = pd.DataFrame(columns=["user_id", "period"])


    weight_query = f"""
        SELECT user_id, effective_date, value * 2.20462 as weight_lbs
        FROM body_weight_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date <= '{end_date}'
          AND value IS NOT NULL
    """
    df_weights = get_data(weight_query, "Weight Logs")

    bmi_query = f"""
        SELECT user_id, effective_date, value as bmi
        FROM bmi_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date <= '{end_date}'
          AND value IS NOT NULL
    """
    df_bmi = get_data(bmi_query, "BMI Logs")

    bp_query = f"""
        SELECT user_id, effective_date, systolic, diastolic
        FROM blood_pressure_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date <= '{end_date}'
          AND systolic IS NOT NULL
          AND diastolic IS NOT NULL
    """
    df_bp = get_data(bp_query, "BP Logs")

    a1c_query = f"""
        SELECT user_id, effective_date, value as a1c
        FROM a1c_values
        WHERE user_id IN ({formatted_ids})
          AND effective_date <= '{end_date}'
          AND value IS NOT NULL
    """
    df_a1c = get_data(a1c_query, "A1C Logs")

    glp1_query = f"""
        SELECT
            p.patient_user_id as user_id,
            p.days_of_supply,
            p.total_refills,
            p.prescribed_at
        FROM prescriptions p
        JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
        JOIN medications m ON m.id = ndcs.medication_id
        WHERE p.patient_user_id IN ({formatted_ids})
        AND p.prescribed_at IS NOT NULL
        AND p.prescribed_at <= '{end_date}'
        AND (
                m.name LIKE '%Wegovy%'
            OR m.name LIKE '%Zepbound%'
            OR m.name LIKE '%Ozempic%'
            OR m.name LIKE '%Mounjaro%'
        )
    """
    df_glp1 = get_data(glp1_query, "GLP1 Rx (Wegovy/Zepbound, <= report date)")


    chronic_meds_query = f"""
        SELECT DISTINCT p.patient_user_id as user_id
        FROM prescriptions p
        JOIN medication_ndcs ndcs ON p.prescribed_ndc = ndcs.ndc
        JOIN medications m ON m.id = ndcs.medication_id
        WHERE p.patient_user_id IN ({formatted_ids})
          AND (
                m.name LIKE '%Metformin%'
             OR m.name LIKE '%Insulin%'
             OR m.name LIKE '%Glipizide%'
             OR m.name LIKE '%Lisinopril%'
             OR m.name LIKE '%Amlodipine%'
             OR m.name LIKE '%Atorvastatin%'
          )
    """
    df_chronic_meds = get_data(chronic_meds_query, "Chronic Meds Rx")

    # 3) Date conversions
    print("\n🧠 Processing Data in Python...")
    df_users["start_date"] = pd.to_datetime(df_users["start_date"], errors="coerce")

    if not df_glp1.empty:
        df_glp1["prescribed_at"] = pd.to_datetime(df_glp1["prescribed_at"], errors="coerce")

    for df in [df_weights, df_bmi, df_bp, df_a1c]:
        if not df.empty:
            df["effective_date"] = pd.to_datetime(df["effective_date"], errors="coerce")
            df.dropna(subset=["effective_date"], inplace=True)

    # 4) Baselines + latests
    print("  ⚖️  Calculating Baselines/Latests...")
    base_weight = calculate_baseline(df_weights, "weight_lbs", outcome_name="baseline_weight_lbs")
    latest_weight = calculate_latest(df_weights, "weight_lbs", outcome_name="latest_weight_lbs")

    base_bmi = calculate_baseline(df_bmi, "bmi", outcome_name="baseline_bmi")

    base_a1c = calculate_baseline(df_a1c, "a1c", outcome_name="baseline_a1c")
    latest_a1c = calculate_latest(df_a1c, "a1c", outcome_name="latest_a1c")

    if not df_bp.empty:
        df_bp_sorted = df_bp.sort_values(by=["user_id", "effective_date"], ascending=[True, True])
        base_bp_row = df_bp_sorted.groupby("user_id").first().reset_index()
        base_bp = base_bp_row[["user_id", "systolic", "diastolic"]].rename(
            columns={"systolic": "baseline_systolic", "diastolic": "baseline_diastolic"}
        )

        df_bp_latest = df_bp.sort_values(by=["user_id", "effective_date"], ascending=[True, False])
        latest_bp_row = df_bp_latest.groupby("user_id").first().reset_index()
        latest_bp = latest_bp_row[["user_id", "systolic", "diastolic", "effective_date"]].rename(
            columns={"systolic": "latest_systolic", "diastolic": "latest_diastolic", "effective_date": "latest_bp_date"}
        )
    else:
        base_bp = pd.DataFrame(columns=["user_id", "baseline_systolic", "baseline_diastolic"])
        latest_bp = pd.DataFrame(columns=["user_id", "latest_systolic", "latest_diastolic", "latest_bp_date"])

    # 5) Standard engagement (avg over all months) — weights
    print("  📅 Calculating Standard Engagement...")
    if not df_weights.empty:
        df_weights["month"] = df_weights["effective_date"].dt.to_period("M")
        engagement = df_weights.groupby(["user_id", "month"]).size().reset_index(name="count")
        engagement_summary = engagement.groupby("user_id").agg(avg_monthly_weigh_ins=("count", "mean")).reset_index()
        engagement_summary["meets_engagement_rule"] = (engagement_summary["avg_monthly_weigh_ins"] >= 10).astype(int)
    else:
        engagement_summary = pd.DataFrame(columns=["user_id", "avg_monthly_weigh_ins", "meets_engagement_rule"])

    # BP standard engagement (avg per month from month after signup)
    print("  📅 Calculating Standard BP Engagement...")
    bp_engagement_summary = calculate_avg_monthly_events_full_months(
        df_events=df_bp,
        df_users=df_users,
        user_col="user_id",
        event_date_col="effective_date",
        start_date_col="start_date",
        window_end=end_date,
        avg_col_name="avg_monthly_bp_checks",
        flag_col_name="meets_bp_engagement_rule",
        threshold=5,
    )
    # 6) Meds
    print("  💊 Calculating Medication Usage...")

    if not df_glp1.empty:
        # Ensure types
        df_glp1 = df_glp1.copy()
        df_glp1["prescribed_at"] = pd.to_datetime(df_glp1["prescribed_at"], errors="coerce")
        df_glp1 = df_glp1.dropna(subset=["prescribed_at"])

        df_glp1["days_of_supply"] = pd.to_numeric(df_glp1["days_of_supply"], errors="coerce").fillna(0)
        df_glp1["total_refills"] = pd.to_numeric(df_glp1["total_refills"], errors="coerce").fillna(0)

        # SQL-equivalent:
        # total_prescription_days = days_of_supply + days_of_supply * COALESCE(total_refills,0)
        df_glp1["rx_days_coverage"] = df_glp1["days_of_supply"] * (1 + df_glp1["total_refills"])

        # prescription_end_date = prescribed_at + total_prescription_days
        df_glp1["rx_end_date"] = df_glp1["prescribed_at"] + pd.to_timedelta(df_glp1["rx_days_coverage"], unit="D")

        glp1_grouped = (
            df_glp1.groupby("user_id")
            .agg(
                first_rx_date=("prescribed_at", "min"),
                last_covered_day=("rx_end_date", "max"),
                total_covered_days=("rx_days_coverage", "sum"),
            )
            .reset_index()
        )

        # total_period_days = DATEDIFF(last_covered_day, first_rx_date)
        glp1_grouped["total_period_days"] = (glp1_grouped["last_covered_day"] - glp1_grouped["first_rx_date"]).dt.days

        # gap_percentage = ((period - covered) * 100 / period) else 0
        glp1_grouped["gap_percentage"] = np.where(
            glp1_grouped["total_period_days"] > 0,
            ((glp1_grouped["total_period_days"] - glp1_grouped["total_covered_days"]) * 100.0 / glp1_grouped["total_period_days"]),
            0.0,
        )

        report_dt = pd.to_datetime(end_date)
        cutoff_90 = report_dt - pd.Timedelta(days=90)

        # "At least 1 covered day in last 90 days"
        glp1_grouped["flag_active_glp1_on_report_date"] = (glp1_grouped["last_covered_day"] >= cutoff_90).astype(int)

        # SQL definition of "on GLP1"
        glp1_grouped["glp1_compliant"] = (
            (glp1_grouped["gap_percentage"] < 10.0) &
            (glp1_grouped["total_covered_days"] >= 90) &
            (glp1_grouped["last_covered_day"] >= cutoff_90)
        ).astype(int)

        # IMPORTANT:
        # If you want "is_glp1_user" to mean "ON GLP1 by the above strict definition"
        glp1_grouped["is_glp1_user"] = glp1_grouped["glp1_compliant"].astype(int)

        glp1_summary = glp1_grouped[
            [
                "user_id",
                "is_glp1_user",
                "glp1_compliant",
                "flag_active_glp1_on_report_date",
                "gap_percentage",
                "total_covered_days",
                "first_rx_date",
                "last_covered_day",
                "total_period_days",
            ]
        ]
    else:
        glp1_summary = pd.DataFrame(
            columns=[
                "user_id",
                "is_glp1_user",
                "glp1_compliant",
                "flag_active_glp1_on_report_date",
                "gap_percentage",
                "total_covered_days",
                "first_rx_date",
                "last_covered_day",
                "total_period_days",
            ]
        )

    # Chronic meds block stays the same
    if not df_chronic_meds.empty:
        df_chronic_meds["flag_on_chronic_medication"] = 1
        chronic_summary = df_chronic_meds[["user_id", "flag_on_chronic_medication"]].drop_duplicates()
    else:
        chronic_summary = pd.DataFrame(columns=["user_id", "flag_on_chronic_medication"])
    print("  🚦 Calculating STRICT Billable-Month Engagement...")

    strict_weight = strict_monthly_threshold_billable_months(
        df_events=df_weights,
        df_users=df_users,
        df_billable_months=billable_months,
        user_col="user_id",
        event_date_col="effective_date",
        start_date_col="start_date",
        end_date_str=end_date,
        threshold=10,
        out_col="flag_10_weights_each_billable_month",
    )

    strict_bp = strict_monthly_threshold_billable_months(
        df_events=df_bp,
        df_users=df_users,
        df_billable_months=billable_months,
        user_col="user_id",
        event_date_col="effective_date",
        start_date_col="start_date",
        end_date_str=end_date,
        threshold=5,
        out_col="flag_5_bp_each_billable_month",
    )
    tmp = strict_weight.merge(
        billable_months.groupby("user_id")["period"].nunique().reset_index(name="billable_months_total"),
        on="user_id",
        how="left",
    )
    print(tmp["billable_months_total"].describe())
    print(tmp["flag_10_weights_each_billable_month"].value_counts(dropna=False))

    # # 7) STRICT "each Billable Month" engagement flags (starts month AFTER start_date)
    # print("  🚦 Calculating STRICT Billable-Month Engagement...")
    # strict_weight = strict_monthly_threshold_all_active_months(
    #     df_events=df_weights,
    #     df_users=df_users,
    #     user_col="user_id",
    #     event_date_col="effective_date",
    #     start_date_col="start_date",
    #     end_date_str=end_date,
    #     threshold=10,
    #     out_col="flag_10_weights_each_billable_month",
    # )

    # strict_bp = strict_monthly_threshold_all_active_months(
    #     df_events=df_bp,
    #     df_users=df_users,
    #     user_col="user_id",
    #     event_date_col="effective_date",
    #     start_date_col="start_date",
    #     end_date_str=end_date,
    #     threshold=5,
    #     out_col="flag_5_bp_each_billable_month",
    # )

    # 8) Rolling A1C labs (last 12 months) flag
    print("  🔄 Calculating Rolling Year A1C...")
    if not df_a1c.empty:
        report_dt = pd.to_datetime(end_date)
        start_rolling = report_dt - timedelta(days=365)
        a1c_rolling = df_a1c[(df_a1c["effective_date"] >= start_rolling) & (df_a1c["effective_date"] <= report_dt)]
        counts_rolling = a1c_rolling.groupby("user_id")["effective_date"].nunique().reset_index(name="labs_in_rolling_year")
        counts_rolling["flag_2_plus_a1c_rolling_year"] = (counts_rolling["labs_in_rolling_year"] >= 2).astype(int)
        df_a1c_rolling_flag = counts_rolling[["user_id", "flag_2_plus_a1c_rolling_year"]]
    else:
        df_a1c_rolling_flag = pd.DataFrame(columns=["user_id", "flag_2_plus_a1c_rolling_year"])

    # 9) Merge master
    print("  🔗 Merging Master Table...")
    master = df_users.merge(engagement_summary, on="user_id", how="left")
    master = master.merge(bp_engagement_summary, on="user_id", how="left")
    master = master.merge(glp1_summary, on="user_id", how="left")
    master = master.merge(chronic_summary, on="user_id", how="left")

    master = master.merge(base_weight, on="user_id", how="left")
    master = master.merge(latest_weight, on="user_id", how="left")  # latest weight included
    master = master.merge(base_bmi, on="user_id", how="left")

    master = master.merge(base_bp, on="user_id", how="left")
    master = master.merge(latest_bp, on="user_id", how="left")

    master = master.merge(base_a1c, on="user_id", how="left")
    master = master.merge(latest_a1c, on="user_id", how="left")

    master = master.merge(strict_weight, on="user_id", how="left")
    master = master.merge(strict_bp, on="user_id", how="left")
    master = master.merge(df_a1c_rolling_flag, on="user_id", how="left")

    # 10) Clinical flags & deltas
    print("  🚩 Generating Clinical Flags & Deltas...")
    master["flag_baseline_bp_140_90"] = master.apply(
        lambda row: 1
        if (pd.notnull(row.get("baseline_systolic")) and (row["baseline_systolic"] >= 140 or row["baseline_diastolic"] >= 90))
        else 0,
        axis=1,
    )
    master["flag_baseline_bmi_gt_30"] = master.apply(
        lambda row: 1 if (pd.notnull(row.get("baseline_bmi")) and row["baseline_bmi"] >= 30) else 0,
        axis=1,
    )
    master["flag_baseline_a1c_gt_9"] = master.apply(
        lambda row: 1 if (pd.notnull(row.get("baseline_a1c")) and row["baseline_a1c"] > 9.0) else 0,
        axis=1,
    )

    master["systolic_change"] = pd.to_numeric(master["baseline_systolic"], errors="coerce") - pd.to_numeric(master["latest_systolic"], errors="coerce")
    master["diastolic_change"] = pd.to_numeric(master["baseline_diastolic"], errors="coerce") - pd.to_numeric(master["latest_diastolic"], errors="coerce")

    # 11) Cleanup / fills
    fill_zero_cols = [
        "meets_engagement_rule",
        "avg_monthly_weigh_ins",
        "meets_bp_engagement_rule",
        "avg_monthly_bp_checks",
        "is_glp1_user",
        "glp1_compliant",
        "flag_active_glp1_on_report_date",
        "flag_on_chronic_medication",
        "flag_10_weights_each_billable_month",
        "flag_5_bp_each_billable_month",
        "flag_2_plus_a1c_rolling_year",
    ]
    for col in fill_zero_cols:
        if col in master.columns:
            master[col] = master[col].fillna(0)

    # 12) SUMMARY EXPORT (STRICT billable-month rules)
    print("  📊 Building Summary Table (Strict Billable-Month Rules)...")
    df = master.copy()

    wt_base = df[
        (df["baseline_bmi"].notna())
        & (pd.to_numeric(df["baseline_bmi"], errors="coerce") >= 30)
        & (df["flag_10_weights_each_billable_month"] == 1)
        & (df["baseline_weight_lbs"].notna())
        & (df["latest_weight_lbs"].notna())
    ].copy()

    wt_not_glp = wt_base[pd.to_numeric(wt_base["is_glp1_user"], errors="coerce").fillna(0) == 0]
    wt_on_glp = wt_base[pd.to_numeric(wt_base["is_glp1_user"], errors="coerce").fillna(0) == 1]

    bp_base = df[
        (df["flag_baseline_bp_140_90"] == 1)
        & (df["flag_5_bp_each_billable_month"] == 1)
        & (df["baseline_systolic"].notna())
        & (df["latest_systolic"].notna())
        & (df["baseline_diastolic"].notna())
        & (df["latest_diastolic"].notna())
    ].copy()

    a1c_base = df[
        (df["baseline_a1c"].notna())
        & (pd.to_numeric(df["baseline_a1c"], errors="coerce") > 9.0)
        & (df["flag_2_plus_a1c_rolling_year"] == 1)
        & (df["latest_a1c"].notna())
    ].copy()

    summary_rows = [
        summarize_pair(
            wt_not_glp,
            "Weight Loss – BMI>=30 NOT on GLP (tenure>=90; 10 weights each Billable Month)",
            "baseline_weight_lbs",
            "latest_weight_lbs",
        ),
        summarize_pair(
            wt_on_glp,
            "Weight Loss – BMI>=30 ON GLP (tenure>=90; 10 weights each Billable Month)",
            "baseline_weight_lbs",
            "latest_weight_lbs",
        ),
        summarize_bp(
            bp_base,
            "BP Reduction – baseline >=140/90 (tenure>=90; 5 BPs each Billable Month)",
        ),
        summarize_pair(
            a1c_base,
            "A1C Reduction – baseline > 9 (tenure>=90; >=2 A1C labs in 12 months)",
            "baseline_a1c",
            "latest_a1c",
        ),
    ]

    summary_df = pd.DataFrame(summary_rows)

    # Order columns like your Apple sheet + percent columns + BP extras
    ordered_cols = [
        "Metric",
        "avg_baseline",
        "avg_current",
        "median_baseline",
        "median_current",
        "delta_avg",
        "delta_median",
        "avg_pct_reduction",
        "median_pct_reduction",
        "sample_size",
        "avg_systolic_reduction",
        "avg_diastolic_reduction",
        "median_systolic_reduction",
        "median_diastolic_reduction",
        "avg_systolic_reduction_pct",
        "avg_diastolic_reduction_pct",
        "median_systolic_reduction_pct",
        "median_diastolic_reduction_pct",
    ]
    summary_df = summary_df[[c for c in ordered_cols if c in summary_df.columns]]

    summary_filename = f"pg_dashboard_summary_{partner}_{end_date}.csv"
    summary_df.to_csv(summary_filename, index=False)
    print(f"✅ Saved summary to {summary_filename} ({len(summary_df)} rows)")

    # 13) Ensure readable_id first + export member-level
    preferred_cols = ["user_readable_id"]
    existing_preferred = [c for c in preferred_cols if c in master.columns]
    other_cols = [c for c in master.columns if c not in existing_preferred]
    master = master[existing_preferred + other_cols]

    filename = f"pg_dashboard_source_{partner}_FINAL_READONLY.csv"
    master.to_csv(filename, index=False)
    print(f"\n✅ Success! Saved {len(master)} rows to {filename}")

if __name__ == "__main__":
    main()
