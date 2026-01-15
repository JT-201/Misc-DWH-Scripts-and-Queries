import mysql.connector
import time
import pandas as pd
import warnings
import numpy as np
from datetime import timedelta
from config import get_db_config

# ------------------------------------------------------------
# Apple PG Clinical — As-of snapshot
# ------------------------------------------------------------
PARTNER = "Apple"
REPORT_DATE = "2025-12-31"

# Global eligibility
TENURE_MIN_DAYS = 90

# Thresholds / cohort splits
BMI_THRESHOLD = 30

# Baseline window for WEIGHT/BMI/A1C (baseline is first value within +/- window around start_date)
BASELINE_WINDOW_DAYS = 30

# Date guardrail (prevents out-of-bounds pandas timestamps)
MIN_VALID_DATE = "2000-01-01"

# Weight engagement (monthly)
WEIGHTS_PER_MONTH_MIN = 10

# A1C cohort requirements
A1C_BASELINE_THRESHOLD = 9.0  # >= 9
A1C_MIN_LABS_IN_12MO = 2

# GLP detection / qualification
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
# Safe datetime conversion
# -----------------------
def coerce_dt(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return df
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df[df[col].notna()].copy()


# -----------------------
# Baselines / latest
# -----------------------
def baseline_within_window(
    meas_df: pd.DataFrame,
    start_df: pd.DataFrame,
    value_col: str,
    meas_date_col: str,
    start_col: str = "start_date",
    window_days: int = 30,
    out_value_name: str = "baseline_value",
    out_date_name: str = "baseline_date",
) -> pd.DataFrame:
    """
    Baseline = earliest measurement within [start_date - window_days, start_date + window_days]
    """
    if meas_df.empty:
        return pd.DataFrame(columns=["user_id", out_value_name, out_date_name])

    m = meas_df.copy()
    m = coerce_dt(m, meas_date_col)

    s = start_df[["user_id", start_col]].copy()
    s = coerce_dt(s, start_col)

    m = m.merge(s, on="user_id", how="inner")
    m["win_start"] = m[start_col] - pd.to_timedelta(window_days, unit="D")
    m["win_end"] = m[start_col] + pd.to_timedelta(window_days, unit="D")

    m = m[(m[meas_date_col] >= m["win_start"]) & (m[meas_date_col] <= m["win_end"])]

    if m.empty:
        return pd.DataFrame(columns=["user_id", out_value_name, out_date_name])

    m = m.sort_values(["user_id", meas_date_col], ascending=[True, True])
    b = m.groupby("user_id", as_index=False).first()

    b = b.rename(columns={value_col: out_value_name, meas_date_col: out_date_name})
    return b[["user_id", out_value_name, out_date_name]]


def latest_as_of(
    meas_df: pd.DataFrame,
    value_col: str,
    date_col: str,
    out_value_name: str = "current_value",
    out_date_name: str = "current_date",
) -> pd.DataFrame:
    """
    Latest measurement as-of report_date (already filtered in SQL)
    """
    if meas_df.empty:
        return pd.DataFrame(columns=["user_id", out_value_name, out_date_name])

    m = meas_df.copy()
    m = coerce_dt(m, date_col)

    m = m.sort_values(["user_id", date_col], ascending=[True, False])
    l = m.groupby("user_id", as_index=False).first()
    l = l.rename(columns={value_col: out_value_name, date_col: out_date_name})
    return l[["user_id", out_value_name, out_date_name]]


# -----------------------
# Engagement flags
# -----------------------
def weights_10_each_month_from_next_month(
    df_weights: pd.DataFrame,
    base_users: pd.DataFrame,
    report_date: str,
    min_per_month: int = 10,
    out_col: str = "flag_10_weights_each_month_from_next_month",
) -> pd.DataFrame:
    """
    For each user:
      - Start counting months from the month AFTER their start_date month
      - Require >= min_per_month weights in EVERY month through report_date's month (inclusive)
      - If user starts in the report month => no required months -> pass (but tenure>=90 prevents this typically)
    """
    users = base_users[["user_id", "start_date"]].copy()
    users["start_date"] = pd.to_datetime(users["start_date"], errors="coerce")
    users = users[users["start_date"].notna()].copy()

    report_p = pd.Period(report_date, freq="M")
    users["start_period"] = users["start_date"].dt.to_period("M")
    users["first_required_period"] = users["start_period"] + 1

    if df_weights.empty:
        out = users[["user_id"]].copy()
        out[out_col] = 0
        out.loc[users["first_required_period"] > report_p, out_col] = 1
        return out

    w = df_weights[["user_id", "effective_date"]].copy()
    w["effective_date"] = pd.to_datetime(w["effective_date"], errors="coerce")
    w = w[w["effective_date"].notna()].copy()
    w["period"] = w["effective_date"].dt.to_period("M")

    counts = w.groupby(["user_id", "period"]).size().reset_index(name="cnt")

    expanded = []
    no_req_users = []
    for row in users.itertuples(index=False):
        uid = row.user_id
        first_req = row.first_required_period
        if first_req > report_p:
            no_req_users.append(uid)
            continue
        periods = pd.period_range(first_req, report_p, freq="M")
        for p in periods:
            expanded.append((uid, p))

    req = pd.DataFrame(expanded, columns=["user_id", "period"])
    if req.empty:
        out = users[["user_id"]].copy()
        out[out_col] = 0
        if no_req_users:
            out.loc[out["user_id"].isin(no_req_users), out_col] = 1
        return out

    m = req.merge(counts, on=["user_id", "period"], how="left")
    m["cnt"] = m["cnt"].fillna(0)
    m["met"] = (m["cnt"] >= min_per_month).astype(int)

    agg = m.groupby("user_id").agg(
        months_required=("period", "nunique"),
        months_met=("met", "sum")
    ).reset_index()

    agg[out_col] = (agg["months_required"] == agg["months_met"]).astype(int)

    out = users[["user_id"]].merge(agg[["user_id", out_col]], on="user_id", how="left")
    out[out_col] = out[out_col].fillna(0).astype(int)

    if no_req_users:
        out.loc[out["user_id"].isin(no_req_users), out_col] = 1

    return out


def flag_continuous_engagement_6mo(df_bus: pd.DataFrame, report_date: str) -> pd.DataFrame:
    """
    Continuous engagement = at least 1 billable day in each of the last 6 calendar months (including report month).
    Requires: billable_user_statuses(user_id, date, is_billable).
    """
    if df_bus.empty:
        return pd.DataFrame(columns=["user_id", "flag_continuous_engagement_6mo"])

    d = df_bus.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d[d["date"].notna()].copy()

    report_p = pd.Period(report_date, freq="M")
    months = [report_p - i for i in range(0, 6)]

    d["period"] = d["date"].dt.to_period("M")
    d = d[(d["is_billable"] == 1) & (d["period"].isin(months))].copy()

    covered = d.groupby("user_id")["period"].nunique().reset_index(name="months_covered")
    covered["flag_continuous_engagement_6mo"] = (covered["months_covered"] == 6).astype(int)
    return covered[["user_id", "flag_continuous_engagement_6mo"]]


def flag_two_weights_30_days_apart(df_weights: pd.DataFrame,
                                  base_users: pd.DataFrame,
                                  report_date: str,
                                  min_days_apart: int = 30) -> pd.DataFrame:
    """
    Require >=2 weight values on/after start_date with at least min_days_apart between earliest and latest.
    """
    out = base_users[["user_id", "start_date"]].copy()
    out["start_date"] = pd.to_datetime(out["start_date"], errors="coerce")
    out = out[out["start_date"].notna()].copy()

    if df_weights.empty:
        out["flag_2_weights_30d_apart_from_start"] = 0
        return out[["user_id", "flag_2_weights_30d_apart_from_start"]]

    w = df_weights.copy()
    w["effective_date"] = pd.to_datetime(w["effective_date"], errors="coerce")
    w = w[w["effective_date"].notna()].copy()

    w = w.merge(out[["user_id", "start_date"]], on="user_id", how="inner")
    report_dt = pd.to_datetime(report_date)

    w = w[(w["effective_date"] >= w["start_date"]) & (w["effective_date"] <= report_dt)].copy()
    if w.empty:
        out["flag_2_weights_30d_apart_from_start"] = 0
        return out[["user_id", "flag_2_weights_30d_apart_from_start"]]

    agg = w.groupby("user_id").agg(
        first_w=("effective_date", "min"),
        last_w=("effective_date", "max"),
        n_w=("effective_date", "nunique")
    ).reset_index()

    agg["days_apart"] = (agg["last_w"] - agg["first_w"]).dt.days
    agg["flag_2_weights_30d_apart_from_start"] = (
        (agg["n_w"] >= 2) & (agg["days_apart"] >= min_days_apart)
    ).astype(int)

    return agg[["user_id", "flag_2_weights_30d_apart_from_start"]]


# -----------------------
# GLP qualification
# -----------------------
def compute_glp1_qualified(df_glp1: pd.DataFrame,
                           report_date: str,
                           min_covered_days: int = 90,
                           max_gap_pct: float = 0.10,
                           recent_days: int = 90) -> pd.DataFrame:
    """
    Qualified GLP1 user if:
      - total_covered_days >= min_covered_days
      - coverage_gap_pct <= max_gap_pct over [first_rx_date, last_covered_day]
      - last_covered_day >= report_date - recent_days   (>=1 covered day within last 90 days)
    """
    cols = [
        "user_id",
        "flag_glp1_qualified",
        "first_rx_date",
        "last_covered_day",
        "total_covered_days",
        "period_days",
        "coverage_gap_pct",
    ]
    if df_glp1.empty:
        return pd.DataFrame(columns=cols)

    r = df_glp1.copy()
    r["prescribed_at"] = pd.to_datetime(r["prescribed_at"], errors="coerce")
    r = r[r["prescribed_at"].notna()].copy()

    r["total_refills"] = pd.to_numeric(r["total_refills"], errors="coerce").fillna(0)
    r["days_of_supply"] = pd.to_numeric(r["days_of_supply"], errors="coerce").fillna(0)

    r["covered_days"] = r["days_of_supply"] * (1 + r["total_refills"])
    r["rx_end_date"] = r["prescribed_at"] + pd.to_timedelta(r["covered_days"], unit="D")

    g = r.groupby("user_id").agg(
        first_rx_date=("prescribed_at", "min"),
        last_covered_day=("rx_end_date", "max"),
        total_covered_days=("covered_days", "sum"),
    ).reset_index()

    g["period_days"] = (g["last_covered_day"] - g["first_rx_date"]).dt.days
    g.loc[g["period_days"] <= 0, "period_days"] = 1

    g["coverage_gap_pct"] = 1 - (g["total_covered_days"] / g["period_days"])

    report_dt = pd.to_datetime(report_date)
    recent_cutoff = report_dt - pd.to_timedelta(recent_days, unit="D")

    g["flag_glp1_qualified"] = (
        (g["total_covered_days"] >= min_covered_days) &
        (g["coverage_gap_pct"] <= max_gap_pct) &
        (g["last_covered_day"] >= recent_cutoff)
    ).astype(int)

    return g[cols]


# -----------------------
# Summaries
# -----------------------
def summarize_weight_pct(df_in: pd.DataFrame, label: str) -> dict:
    d = df_in.copy()

    bw = pd.to_numeric(d["baseline_weight_lbs"], errors="coerce")
    cw = pd.to_numeric(d["current_weight_lbs"], errors="coerce")

    valid = (
        bw.notna() & cw.notna() &
        (bw > 0) &
        bw.between(50, 700) &
        cw.between(50, 700)
    )
    d = d[valid].copy()
    d["baseline_weight_lbs"] = bw[valid]
    d["current_weight_lbs"] = cw[valid]

    if d.empty:
        return {
            "Metric": label,
            "avg_weight_loss_pct": np.nan,
            "median_weight_loss_pct": np.nan,
            "avg_lbs_lost": np.nan,
            "sample_size": 0,
        }

    d["weight_loss_lbs"] = d["baseline_weight_lbs"] - d["current_weight_lbs"]
    d["weight_loss_pct"] = d["weight_loss_lbs"] / d["baseline_weight_lbs"]

    return {
        "Metric": label,
        "avg_weight_loss_pct": float(d["weight_loss_pct"].mean()),
        "median_weight_loss_pct": float(d["weight_loss_pct"].median()),
        "avg_lbs_lost": float(d["weight_loss_lbs"].mean()),
        "sample_size": int(d["user_id"].nunique()),
    }


def summarize_a1c(df_in: pd.DataFrame, label: str) -> dict:
    d = df_in.copy()
    d["baseline_a1c"] = pd.to_numeric(d["baseline_a1c"], errors="coerce")
    d["current_a1c"] = pd.to_numeric(d["current_a1c"], errors="coerce")

    d = d[d["baseline_a1c"].notna() & d["current_a1c"].notna() & (d["baseline_a1c"] > 0)].copy()
    if d.empty:
        return {
            "Metric": label,
            "avg_baseline_a1c": np.nan,
            "avg_current_a1c": np.nan,
            "avg_a1c_reduction_points": np.nan,
            "avg_a1c_reduction_pct": np.nan,
            "sample_size": 0,
        }

    d["a1c_reduction_points"] = d["baseline_a1c"] - d["current_a1c"]
    d["a1c_reduction_pct"] = d["a1c_reduction_points"] / d["baseline_a1c"]

    return {
        "Metric": label,
        "avg_baseline_a1c": float(d["baseline_a1c"].mean()),
        "avg_current_a1c": float(d["current_a1c"].mean()),
        "avg_a1c_reduction_points": float(d["a1c_reduction_points"].mean()),
        "avg_a1c_reduction_pct": float(d["a1c_reduction_pct"].mean()),
        "sample_size": int(d["user_id"].nunique()),
    }


# -----------------------
# Main
# -----------------------
def main(partner=PARTNER, report_date=REPORT_DATE):
    print(f"\n🚀 Apple PG Clinical — Partner={partner} As-Of={report_date}")

    # ------------------------------------------------------------
    # 1) Base cohort: ACTIVE as-of report_date + tenure >= 90
    #    Includes users.readable_id so exports are readable.
    # ------------------------------------------------------------
    base_users = get_data(f"""
        SELECT DISTINCT
            bus.user_id,
            u.readable_id,
            s.start_date,
            DATEDIFF('{report_date}', s.start_date) AS tenure_days
        FROM billable_user_statuses bus
        JOIN subscriptions s ON s.user_id = bus.user_id
        JOIN users u ON u.id = bus.user_id
        JOIN partner_employers pe ON pe.user_id = u.id
        WHERE pe.name = '{partner}'
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

    # IN (...) list for binary ids (kept internal for joins/pulls)
    raw_ids = base_users["user_id"].tolist()
    if raw_ids and isinstance(raw_ids[0], (bytes, bytearray)):
        formatted_ids = ", ".join([f"0x{uid.hex()}" for uid in raw_ids])
    else:
        formatted_ids = str(tuple(raw_ids))
        if len(raw_ids) == 1:
            formatted_ids = formatted_ids.replace(",", "")

    # ------------------------------------------------------------
    # 2) billable_user_statuses history (needed for continuous engagement gate)
    # ------------------------------------------------------------
    min_start = base_users["start_date"].min()
    df_bus = get_data(f"""
        SELECT user_id, date, is_billable, subscription_status, user_status
        FROM billable_user_statuses
        WHERE partner = '{partner}'
          AND date >= '{pd.to_datetime(min_start).date()}'
          AND date <= '{report_date}'
          AND user_id IN ({formatted_ids})
    """, "billable_user_statuses (cohort history)")

    # ------------------------------------------------------------
    # 3) Pull measures (cohort only)
    # ------------------------------------------------------------
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

    # Safe datetime coercion
    df_bus = coerce_dt(df_bus, "date")
    df_weights = coerce_dt(df_weights, "effective_date")
    df_bmi = coerce_dt(df_bmi, "effective_date")
    df_a1c = coerce_dt(df_a1c, "effective_date")
    df_glp1 = coerce_dt(df_glp1, "prescribed_at")

    # ------------------------------------------------------------
    # 4) Baselines + currents
    # ------------------------------------------------------------
    base_weight = baseline_within_window(
        df_weights, base_users,
        value_col="weight_lbs",
        meas_date_col="effective_date",
        window_days=BASELINE_WINDOW_DAYS,
        out_value_name="baseline_weight_lbs",
        out_date_name="baseline_weight_date",
    )
    cur_weight = latest_as_of(
        df_weights,
        value_col="weight_lbs",
        date_col="effective_date",
        out_value_name="current_weight_lbs",
        out_date_name="current_weight_date",
    )

    base_bmi = baseline_within_window(
        df_bmi, base_users,
        value_col="bmi",
        meas_date_col="effective_date",
        window_days=BASELINE_WINDOW_DAYS,
        out_value_name="baseline_bmi",
        out_date_name="baseline_bmi_date",
    )

    base_a1c = baseline_within_window(
        df_a1c, base_users,
        value_col="a1c",
        meas_date_col="effective_date",
        window_days=BASELINE_WINDOW_DAYS,
        out_value_name="baseline_a1c",
        out_date_name="baseline_a1c_date",
    )
    cur_a1c = latest_as_of(
        df_a1c,
        value_col="a1c",
        date_col="effective_date",
        out_value_name="current_a1c",
        out_date_name="current_a1c_date",
    )

    # ------------------------------------------------------------
    # 5) Engagement + GLP qualification gates
    # ------------------------------------------------------------
    weight_eng_monthly = weights_10_each_month_from_next_month(
        df_weights=df_weights,
        base_users=base_users,
        report_date=report_date,
        min_per_month=WEIGHTS_PER_MONTH_MIN,
        out_col="flag_10_weights_each_month_from_next_month",
    )

    eng_6mo = flag_continuous_engagement_6mo(df_bus, report_date)

    wt_2x = flag_two_weights_30_days_apart(df_weights, base_users, report_date)

    glp_qualified = compute_glp1_qualified(
        df_glp1,
        report_date=report_date,
        min_covered_days=90,
        max_gap_pct=0.10,
        recent_days=90,
    )

    # A1C labs in last 12 months
    report_dt = pd.to_datetime(report_date)
    start_12mo = report_dt - timedelta(days=365)
    if not df_a1c.empty:
        a1c_12mo = df_a1c[(df_a1c["effective_date"] >= start_12mo) & (df_a1c["effective_date"] <= report_dt)]
        a1c_counts = a1c_12mo.groupby("user_id")["effective_date"].nunique().reset_index(name="a1c_labs_12mo")
    else:
        a1c_counts = pd.DataFrame(columns=["user_id", "a1c_labs_12mo"])

    # ------------------------------------------------------------
    # 6) Build master (keep binary user_id for merges, but output readable_id)
    # ------------------------------------------------------------
    master = base_users[["user_id", "readable_id", "start_date", "tenure_days"]].copy()

    for t in [
        base_weight, cur_weight, base_bmi,
        base_a1c, cur_a1c,
        weight_eng_monthly, eng_6mo, wt_2x,
        glp_qualified, a1c_counts,
    ]:
        master = master.merge(t, on="user_id", how="left")

    master["flag_10_weights_each_month_from_next_month"] = master["flag_10_weights_each_month_from_next_month"].fillna(0).astype(int)
    master["flag_continuous_engagement_6mo"] = master["flag_continuous_engagement_6mo"].fillna(0).astype(int)
    master["flag_2_weights_30d_apart_from_start"] = master["flag_2_weights_30d_apart_from_start"].fillna(0).astype(int)
    master["flag_glp1_qualified"] = master["flag_glp1_qualified"].fillna(0).astype(int)
    master["a1c_labs_12mo"] = master["a1c_labs_12mo"].fillna(0).astype(int)

    # BMI flags (baseline BMI required for BMI cohorts)
    bmi_num = pd.to_numeric(master["baseline_bmi"], errors="coerce")
    master["flag_has_baseline_bmi"] = bmi_num.notna().astype(int)
    master["flag_baseline_bmi_ge_30"] = (bmi_num >= BMI_THRESHOLD).astype(int)
    master["flag_baseline_bmi_lt_30"] = (bmi_num < BMI_THRESHOLD).astype(int)

    # Weight loss % (guardrails prevent inf/-inf)
    bw = pd.to_numeric(master["baseline_weight_lbs"], errors="coerce")
    cw = pd.to_numeric(master["current_weight_lbs"], errors="coerce")
    valid_weight = (
        bw.notna() & cw.notna() &
        (bw > 0) &
        bw.between(50, 700) &
        cw.between(50, 700)
    )
    master["weight_loss_lbs"] = bw - cw
    master["weight_loss_pct"] = np.nan
    master.loc[valid_weight, "weight_loss_pct"] = (bw[valid_weight] - cw[valid_weight]) / bw[valid_weight]

    # A1C spacing (>=30 days baseline->current)
    master["baseline_a1c_date"] = pd.to_datetime(master["baseline_a1c_date"], errors="coerce")
    master["current_a1c_date"] = pd.to_datetime(master["current_a1c_date"], errors="coerce")
    master["days_between_a1c"] = (master["current_a1c_date"] - master["baseline_a1c_date"]).dt.days

    # Convert readable_id to string (in case it's binary)
    if "readable_id" in master.columns:
        master["readable_id"] = master["readable_id"].apply(
            lambda x: x.hex() if isinstance(x, (bytes, bytearray)) else str(x)
        )

    # ------------------------------------------------------------
    # 7) Cohorts
    # ------------------------------------------------------------
    master["cohort_wl_bmi30_not_on_glp"] = (
        (master["flag_has_baseline_bmi"] == 1) &
        (master["flag_baseline_bmi_ge_30"] == 1) &
        (master["flag_glp1_qualified"] == 0) &
        (master["flag_10_weights_each_month_from_next_month"] == 1)
    ).astype(int)

    master["cohort_wl_bmi30_on_glp"] = (
        (master["flag_has_baseline_bmi"] == 1) &
        (master["flag_baseline_bmi_ge_30"] == 1) &
        (master["flag_glp1_qualified"] == 1) &
        (master["flag_10_weights_each_month_from_next_month"] == 1)
    ).astype(int)

    master["cohort_wl_bmi_lt30"] = (
        (master["flag_has_baseline_bmi"] == 1) &
        (master["flag_baseline_bmi_lt_30"] == 1) &
        (master["flag_continuous_engagement_6mo"] == 1) &
        (master["flag_2_weights_30d_apart_from_start"] == 1)
    ).astype(int)

    master["cohort_a1c_baseline_ge_9"] = (
        (pd.to_numeric(master["baseline_a1c"], errors="coerce") >= A1C_BASELINE_THRESHOLD) &
        (master["a1c_labs_12mo"] >= A1C_MIN_LABS_IN_12MO) &
        master["current_a1c"].notna() &
        master["days_between_a1c"].notna() &
        (master["days_between_a1c"] >= 30)
    ).astype(int)

    # ------------------------------------------------------------
    # 8) Summary table
    # ------------------------------------------------------------
    summary_rows = [
        summarize_weight_pct(
            master[master["cohort_wl_bmi30_not_on_glp"] == 1],
            "Weight Loss – BMI>=30 NOT on GLP (10 weights/mo from month after start → Dec 2025)"
        ),
        summarize_weight_pct(
            master[master["cohort_wl_bmi30_on_glp"] == 1],
            "Weight Loss – BMI>=30 ON GLP (same 10 weights/mo rule as NOT-on-GLP)"
        ),
        summarize_weight_pct(
            master[master["cohort_wl_bmi_lt30"] == 1],
            "Weight Loss – BMI<30 (active Dec 2025; 6+ months engaged; 2 weights 30d apart; no monthly weights requirement)"
        ),
        summarize_a1c(
            master[master["cohort_a1c_baseline_ge_9"] == 1],
            "A1C Reduction – baseline >=9 + >=2 labs in 12 months"
        ),
    ]
    summary = pd.DataFrame(summary_rows)

    # ------------------------------------------------------------
    # 9) Debug cohort (BMI<30)
    # ------------------------------------------------------------
    bmi30_debug = master[master["cohort_wl_bmi_lt30"] == 1].copy()
    if not bmi30_debug.empty:
        bmi30_debug["baseline_weight_lbs_num"] = pd.to_numeric(bmi30_debug["baseline_weight_lbs"], errors="coerce")
        bmi30_debug["current_weight_lbs_num"] = pd.to_numeric(bmi30_debug["current_weight_lbs"], errors="coerce")
        bmi30_debug["bad_baseline_weight"] = (~bmi30_debug["baseline_weight_lbs_num"].between(50, 700)) | (bmi30_debug["baseline_weight_lbs_num"] <= 0)
        bmi30_debug["bad_current_weight"] = (~bmi30_debug["current_weight_lbs_num"].between(50, 700)) | (bmi30_debug["current_weight_lbs_num"] <= 0)
        bmi30_debug["missing_weight_pair"] = bmi30_debug["baseline_weight_lbs_num"].isna() | bmi30_debug["current_weight_lbs_num"].isna()
    else:
        bmi30_debug = pd.DataFrame()

    # ------------------------------------------------------------
    # 10) Export (readable_id first)
    # ------------------------------------------------------------
    front_cols = ["readable_id", "user_id", "start_date", "tenure_days"]
    front_cols = [c for c in front_cols if c in master.columns]
    master = master[front_cols + [c for c in master.columns if c not in front_cols]]

    user_out = f"apple_pg_user_level_{partner}_{report_date}.csv"
    summary_out = f"apple_pg_summary_{partner}_{report_date}.csv"
    debug_out = f"apple_pg_debug_bmi_lt30_{partner}_{report_date}.csv"

    master.to_csv(user_out, index=False)
    summary.to_csv(summary_out, index=False)
    if not bmi30_debug.empty:
        bmi30_debug.to_csv(debug_out, index=False)

    print(f"\n✅ Saved user-level: {user_out} ({len(master):,} users)")
    print(f"✅ Saved summary:   {summary_out} ({len(summary):,} rows)")
    if not bmi30_debug.empty:
        print(f"✅ Saved debug:     {debug_out} ({len(bmi30_debug):,} users)")

    print("\n🔎 Sizes")
    print("Base cohort:", len(master))
    print("Cohort BMI>=30 NOT on GLP:", int(master["cohort_wl_bmi30_not_on_glp"].sum()))
    print("Cohort BMI>=30 ON GLP:", int(master["cohort_wl_bmi30_on_glp"].sum()))
    print("Cohort BMI<30:", int(master["cohort_wl_bmi_lt30"].sum()))
    print("A1C cohort:", int(master["cohort_a1c_baseline_ge_9"].sum()))


if __name__ == "__main__":
    main()
