
# ---------------------------------------------------------------------------
# DWH-302 State of Georgia GLP-1 Continuation Adhoc
# ---------------------------------------------------------------------------

import mysql.connector
import pandas as pd
import numpy as np
import time
import sys
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

try:
    from config import get_db_config
except ImportError:
    print("❌ Error: Could not find 'config.py'.")
    sys.exit(1)

def connect_to_db():
    config = get_db_config()
    config['connect_timeout'] = 300
    return mysql.connector.connect(**config)

def get_data(conn, query, desc, chunk_size=50000):
    start = time.time()
    print(f"  📥 Fetching {desc}...")
    try:
        chunks = []
        for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
            chunks.append(chunk)
            sys.stdout.write(f"\r    ...loaded {sum(len(c) for c in chunks):,} rows")
            sys.stdout.flush()
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.DataFrame()
        duration = time.time() - start
        print(f"\n    ⏱️  Finished: {len(df):,} rows in {duration:.2f}s")
        return df
    except Exception as e:
        print(f"\n    ❌ Critical Error fetching {desc}: {e}")
        sys.exit(1)

def run_georgia_glp1_query(conn):
    print("\n  📥 Fetching Georgia GLP-1 members...")
    start = time.time()
    query = """
        WITH

        georgia_glp1_members AS (
            -- DISTINCT on user_id to prevent fan-out from:
            --   partner_employers (member may have >1 row)
            --   subscriptions     (guard against >1 active sub)
            --   questionnaire_records (guard against >1 "latest" answer)
            -- MIN(s.start_date) picks earliest active subscription if multiples exist
            SELECT
                u.id                                        AS member_id,
                u.readable_id,
                u.primary_condition_group,
                s.subscription_status,
                s.subscription_start_date,
                s.cancellation_date,
                DATEDIFF(CURDATE(), s.subscription_start_date) AS days_enrolled

            FROM users u

            -- Deduplicate partner_employers — just need to confirm SoG membership
            JOIN (
                SELECT DISTINCT user_id
                FROM partner_employers
                WHERE name = 'State of Georgia'
            ) pe ON pe.user_id = u.id

            -- Deduplicate subscriptions — take earliest active sub per member
            JOIN (
                SELECT
                    user_id,
                    'ACTIVE'                                AS subscription_status,
                    MIN(start_date)                         AS subscription_start_date,
                    NULL                                    AS cancellation_date
                FROM subscriptions
                WHERE status = 'ACTIVE'
                  AND cancellation_date IS NULL
                GROUP BY user_id
            ) s ON s.user_id = u.id

            -- Deduplicate questionnaire — confirm at least one yes answer exists
            WHERE EXISTS (
                SELECT 1
                FROM questionnaire_records qr_interest
                WHERE qr_interest.user_id = u.id
                  AND qr_interest.question_id = 'A8z9j98E0sxR'
                  AND qr_interest.answer_value = 1
                  AND qr_interest.is_latest_answer = 1
            )
        ),

        member_reported_med AS (
            SELECT
                user_id                                     AS member_id,
                answer_text                                 AS reported_medication_name
            FROM questionnaire_records
            WHERE question_id = 'knzp0ZppEBF4'
              AND is_latest_answer = 1
        ),

        member_continue_glp1 AS (
            SELECT
                user_id                                     AS member_id,
                answer_value                                AS wants_to_continue_glp1
            FROM questionnaire_records
            WHERE question_id = 'gV9Xu8RzF9hR'
              AND is_latest_answer = 1
        ),

        glp1_rx_coverage AS (
            SELECT
                p.patient_user_id                           AS member_id,
                BIN_TO_UUID(p.id)                           AS prescription_id,
                p.prescribed_ndc,
                m.name                                       AS prescribed_medication_name,
                m.therapy_type,
                p.prescribed_at,
                p.days_of_supply,
                p.total_refills,
                p.is_valid,
                mdc.drug_class_name,
                p.days_of_supply * (1 + COALESCE(p.total_refills, 0))
                                                            AS total_covered_days,
                DATE_ADD(
                    p.prescribed_at,
                    INTERVAL p.days_of_supply * (1 + COALESCE(p.total_refills, 0)) DAY
                )                                           AS coverage_end_date,
                CASE
                    WHEN DATE_ADD(
                        p.prescribed_at,
                        INTERVAL p.days_of_supply * (1 + COALESCE(p.total_refills, 0)) DAY
                    ) >= CURDATE()
                    THEN 1 ELSE 0
                END                                         AS rx_covers_today,
                ROW_NUMBER() OVER (
                    PARTITION BY p.patient_user_id
                    ORDER BY p.prescribed_at DESC
                )                                           AS rx_rank

            FROM prescriptions p
            JOIN medication_dosage_ndcs mdn
                ON mdn.ndc = p.prescribed_ndc
            JOIN medication_dosages md
                ON md.id = mdn.medication_dosage_id
            JOIN medications m
                ON m.id = md.medication_id
            JOIN medication_drug_classes mdc
                ON mdc.medication_id = m.id
            WHERE mdc.drug_class_name = 'GLP1'
              AND m.therapy_type IN ('WM', 'DM')
        ),

        latest_glp1_rx AS (
            SELECT *
            FROM glp1_rx_coverage
            WHERE rx_rank = 1
        ),

        baseline_weight AS (
            SELECT
                user_id                                     AS member_id,
                ROUND(value * 2.20462, 2)                   AS baseline_weight_lbs,
                effective_date                              AS baseline_weight_date
            FROM (
                SELECT
                    bwv.user_id,
                    bwv.value,
                    bwv.effective_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY bwv.user_id
                        ORDER BY bwv.effective_date ASC
                    )                                       AS rn
                FROM body_weight_values_cleaned bwv
                JOIN georgia_glp1_members g
                    ON g.member_id = bwv.user_id
                WHERE bwv.value IS NOT NULL
                  AND bwv.effective_date >= DATE_SUB(g.subscription_start_date, INTERVAL 30 DAY)
            ) ranked
            WHERE rn = 1
        ),

        final AS (
            SELECT
                g.member_id,
                g.readable_id,
                g.subscription_status,
                g.subscription_start_date,
                g.cancellation_date,
                g.days_enrolled,
                g.primary_condition_group,
                rm.reported_medication_name,
                cont.wants_to_continue_glp1,
                bw.baseline_weight_lbs,
                bw.baseline_weight_date,
                rx.prescription_id,
                rx.prescribed_at,
                rx.days_of_supply,
                rx.total_refills,
                rx.total_covered_days,
                rx.coverage_end_date,
                rx.rx_covers_today,
                rx.is_valid,
                rx.drug_class_name,
                rx.therapy_type,
                rx.prescribed_medication_name,
                CASE
                    WHEN cont.wants_to_continue_glp1 = 0
                        THEN 'Opted Out of GLP-1'

                    -- No Rx at all and enrolled < 30 days → genuinely too new
                    WHEN rx.prescription_id IS NULL
                     AND g.days_enrolled < 30
                        THEN 'New Enrollee - No Rx Yet'

                    -- No Rx at all and enrolled 30+ days → actionable gap
                    WHEN rx.prescription_id IS NULL
                        THEN 'Not Prescribed GLP-1'

                    -- Has Rx, written today before 9am → in flight
                    WHEN rx.prescription_id IS NOT NULL
                     AND DATE(rx.prescribed_at) = CURDATE()
                     AND TIME(NOW()) < '09:00:00'
                        THEN 'Rx Written - Not Yet Sent to Pharmacy'

                    -- Has Rx, enrolled < 30 days, and Rx is active → active, just new
                    WHEN rx.prescription_id IS NOT NULL
                     AND g.days_enrolled < 30
                     AND rx.rx_covers_today = 1
                        THEN 'Active GLP-1 Rx - Covered Through Today'

                    -- Has Rx, enrolled < 30 days, Rx lapsed within grace period → still counts
                    WHEN rx.prescription_id IS NOT NULL
                     AND g.days_enrolled < 30
                     AND rx.rx_covers_today = 0
                     AND DATEDIFF(CURDATE(), rx.coverage_end_date) <= 30
                        THEN 'Active GLP-1 Rx - Covered Through Today'

                    -- Has Rx, enrolled < 30 days, but Rx already lapsed beyond grace → flag
                    WHEN rx.prescription_id IS NOT NULL
                     AND g.days_enrolled < 30
                        THEN 'New Enrollee - Rx Lapsed'

                    -- Has Rx, active today → normal active
                    WHEN rx.prescription_id IS NOT NULL
                     AND rx.rx_covers_today = 1
                        THEN 'Active GLP-1 Rx - Covered Through Today'

                    -- Has Rx, lapsed within 30-day grace period → still counts as having had one
                    WHEN rx.prescription_id IS NOT NULL
                     AND rx.rx_covers_today = 0
                     AND DATEDIFF(CURDATE(), rx.coverage_end_date) <= 30
                        THEN 'Active GLP-1 Rx - Covered Through Today'

                    -- Has Rx, lapsed beyond grace period
                    WHEN rx.prescription_id IS NOT NULL
                     AND rx.rx_covers_today = 0
                        THEN 'GLP-1 Rx Lapsed - Coverage Expired'

                    ELSE 'Uncategorized'
                END                                         AS member_category

            FROM georgia_glp1_members g
            LEFT JOIN member_reported_med   rm   ON rm.member_id  = g.member_id
            LEFT JOIN member_continue_glp1  cont ON cont.member_id = g.member_id
            LEFT JOIN latest_glp1_rx        rx   ON rx.member_id  = g.member_id
            LEFT JOIN baseline_weight       bw   ON bw.member_id  = g.member_id
        )

        SELECT
            member_id,
            readable_id,
            subscription_status,
            subscription_start_date,
            cancellation_date,
            days_enrolled,
            primary_condition_group,
            reported_medication_name,
            wants_to_continue_glp1,
            baseline_weight_lbs,
            baseline_weight_date,
            prescription_id,
            prescribed_at,
            days_of_supply,
            total_refills,
            total_covered_days,
            coverage_end_date,
            rx_covers_today,
            is_valid,
            drug_class_name,
            therapy_type,
            prescribed_medication_name,
            member_category
        FROM final
        ORDER BY member_category, days_enrolled DESC
    """
    df = get_data(conn, query, "Georgia GLP-1 members")
    duration = time.time() - start
    print(f"  ⏱️  Total query time: {duration:.2f}s")
    print(f"  🔍 DEBUG columns returned: {df.columns.tolist()}")
    print(f"  🔍 prescribed_medication_name sample: {df['prescribed_medication_name'].value_counts(dropna=False).head(5).to_dict() if 'prescribed_medication_name' in df.columns else 'COLUMN MISSING'}")
    return df


# ---------------------------------------------------------------------------
# Task definitions
# All 6 slugs, with metadata about who they apply to.
#
#   required_for:
#     'all'          — every member should have this task
#     'non_diabetes' — only members whose primary_condition_group is NOT diabetes
#     'conditional'  — may or may not exist depending on member setup
# ---------------------------------------------------------------------------

TASKS = [
    {
        'slug'         : 'glp1-continuation-questionnaire',
        'col_prefix'   : 'task_glp1_questionnaire',
        'required_for' : 'all',
        'description'  : 'Continuation questionnaire',
    },
    {
        'slug'         : 'upload-prescription-label',
        'col_prefix'   : 'task_rx_label',
        'required_for' : 'all',
        'description'  : 'Upload prescription label image',
    },
    {
        'slug'         : 'pharmacy-insurance',
        'col_prefix'   : 'task_pharmacy_insurance',
        'required_for' : 'all',
        'description'  : 'Pharmacy insurance info',
    },
    {
        'slug'         : 'complete-initial-lab-order',
        'col_prefix'   : 'task_lab_order',
        'required_for' : 'all',
        'description'  : 'Complete initial lab order',
    },
    {
        'slug'         : 'upload-proof-of-weight',
        'col_prefix'   : 'task_weight_proof',
        'required_for' : 'non_diabetes',
        'description'  : 'Upload proof of weight documentation',
    },
    {
        'slug'         : 'preferred-pharmacy',
        'col_prefix'   : 'task_preferred_pharmacy',
        'required_for' : 'conditional',
        'description'  : 'Select preferred pharmacy (if applicable)',
    },
]

# Derived column names for convenience
for t in TASKS:
    t['status_col']       = f"{t['col_prefix']}_status"
    t['started_col']      = f"{t['col_prefix']}_started_at"
    t['completed_col']    = f"{t['col_prefix']}_completed_at"

TASK_SLUGS      = [t['slug']       for t in TASKS]
ALL_TASK_COLS   = [c for t in TASKS for c in [t['status_col'], t['started_col'], t['completed_col']]]


def run_task_analysis(conn, member_ids):
    """
    Fetches status/timestamps for all 6 task slugs for the given member_ids.
    Returns a wide DataFrame (one row per member).
    NULL in a task column = no task record exists for that member + slug.
    """
    if not member_ids:
        print("  ⚠️  No member IDs passed to task analysis — skipping.")
        return pd.DataFrame()

    print(f"\n  📥 Fetching task progress for {len(member_ids):,} unprescribed members...")
    start = time.time()

    placeholders = ", ".join(["%s"] * len(member_ids))
    slug_list     = ", ".join([f"'{s}'" for s in TASK_SLUGS])

    # Build CASE blocks dynamically from TASKS list
    case_blocks = []
    for t in TASKS:
        p = t['col_prefix']
        s = t['slug']
        case_blocks.append(f"""
            MAX(CASE WHEN t.slug = '{s}' THEN t.status       END) AS {p}_status,
            MAX(CASE WHEN t.slug = '{s}' THEN t.started_at   END) AS {p}_started_at,
            MAX(CASE WHEN t.slug = '{s}' THEN t.completed_at END) AS {p}_completed_at""")

    cases = ",".join(case_blocks)

    query = f"""
        SELECT
            t.user_id AS member_id,
            {cases}
        FROM tasks t
        WHERE t.user_id IN ({placeholders})
          AND t.slug IN ({slug_list})
        GROUP BY t.user_id
    """

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, member_ids)
        rows = cursor.fetchall()
        cursor.close()
        task_df = pd.DataFrame(rows) if rows else pd.DataFrame()
        duration = time.time() - start
        print(f"    ⏱️  Task query finished: {len(task_df):,} members with task records in {duration:.2f}s")
        return task_df
    except Exception as e:
        print(f"\n    ❌ Error fetching task data: {e}")
        return pd.DataFrame()


def merge_tasks_into_cohort(cohort_df, task_df):
    """Left-joins task columns onto the cohort. Missing members get NULLs."""
    if task_df.empty:
        for col in ALL_TASK_COLS:
            cohort_df[col] = None
        return cohort_df
    return cohort_df.merge(task_df, on='member_id', how='left')


def add_task_summary_columns(df):
    """
    Adds computed summary columns to the cohort DataFrame:

      - tasks_required_for_member   : how many of the 6 tasks are expected for this member
                                      (all 4 'all' tasks + proof-of-weight if non-diabetes
                                       + preferred-pharmacy excluded as conditional)
      - tasks_completed_count       : how many of the required tasks are COMPLETED
      - tasks_incomplete            : comma-separated list of required tasks not yet COMPLETED
      - all_required_tasks_done     : 1 if tasks_completed_count >= tasks_required_for_member
    """
    df = df.copy()

    # Determine whether each member is diabetes or not
    # Adjust the condition string below if your DB uses different values
    DIABETES_CONDITIONS = {'type 2 diabetes', 'diabetes', 'dm', 't2d'}
    df['_is_diabetes'] = df['primary_condition_group'].str.lower().str.strip().isin(DIABETES_CONDITIONS)

    def _summarise(row):
        required = []
        for t in TASKS:
            if t['required_for'] == 'all':
                required.append(t)
            elif t['required_for'] == 'non_diabetes' and not row['_is_diabetes']:
                required.append(t)
            # 'conditional' tasks are not counted in required totals

        total_required  = len(required)
        completed_tasks = [t for t in required if str(row.get(t['status_col'], '')).upper() == 'COMPLETED']
        incomplete      = [t['slug'] for t in required if str(row.get(t['status_col'], '')).upper() != 'COMPLETED']

        return pd.Series({
            'tasks_required_for_member' : total_required,
            'tasks_completed_count'     : len(completed_tasks),
            'tasks_incomplete'          : ', '.join(incomplete) if incomplete else 'none',
            'all_required_tasks_done'   : 1 if len(completed_tasks) >= total_required else 0,
        })

    summary_cols = df.apply(_summarise, axis=1)
    df = pd.concat([df, summary_cols], axis=1)
    df.drop(columns=['_is_diabetes'], inplace=True)
    return df


def build_task_status_summary(cohort_df):
    """
    Tidy summary: for each task slug, count of members in each status.
    Includes required_for and description metadata columns.
    """
    rows = []
    task_meta = {t['slug']: t for t in TASKS}
    for t in TASKS:
        col = t['status_col']
        if col not in cohort_df.columns:
            continue
        counts = (
            cohort_df[col]
            .fillna('NO TASK RECORD')
            .value_counts()
            .reset_index()
        )
        counts.columns = ['status', 'member_count']
        counts.insert(0, 'required_for',  t['required_for'])
        counts.insert(0, 'description',   t['description'])
        counts.insert(0, 'task_slug',     t['slug'])
        rows.append(counts)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def build_incomplete_task_summary(cohort_df):
    """
    For each individual incomplete task, how many members still need to do it,
    broken out by their current status on that task.
    Only counts tasks that are 'required' for that member (skips proof-of-weight
    for diabetes members, skips conditional tasks).
    """
    DIABETES_CONDITIONS = {'type 2 diabetes', 'diabetes', 'dm', 't2d'}
    rows = []

    for t in TASKS:
        if t['required_for'] == 'conditional':
            continue  # excluded from required counts

        col = t['status_col']
        if col not in cohort_df.columns:
            continue

        if t['required_for'] == 'non_diabetes':
            subset = cohort_df[
                ~cohort_df['primary_condition_group'].str.lower().str.strip().isin(DIABETES_CONDITIONS)
            ]
        else:
            subset = cohort_df

        not_done = subset[subset[col].fillna('').str.upper() != 'COMPLETED']
        if not_done.empty:
            continue

        counts = (
            not_done[col]
            .fillna('NO TASK RECORD')
            .value_counts()
            .reset_index()
        )
        counts.columns = ['status', 'member_count']
        counts.insert(0, 'required_for', t['required_for'])
        counts.insert(0, 'description',  t['description'])
        counts.insert(0, 'task_slug',    t['slug'])
        rows.append(counts)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_to_excel(df, not_prescribed_df=None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"georgia_glp1_members_{timestamp}.xlsx"
    print(f"\n  📤 Exporting to {filename}...")

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:

        # All members
        df.to_excel(writer, sheet_name="All Members", index=False)
        print(f"  ✅ All Members: {len(df):,} rows")

        # Per-category sheets
        for category in sorted(df["member_category"].unique()):
            sheet_df = df[df["member_category"] == category]
            sheet_name = category[:31]
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  ✅ {category}: {len(sheet_df):,} members")

        if not_prescribed_df is not None and not not_prescribed_df.empty:

            # Full detail with all task columns + summary columns
            not_prescribed_df.to_excel(
                writer, sheet_name="Not Prescribed - Task Detail", index=False
            )
            print(f"  ✅ Not Prescribed - Task Detail: {len(not_prescribed_df):,} members")

            # Per-task status counts (all 6 tasks)
            status_summary = build_task_status_summary(not_prescribed_df)
            if not status_summary.empty:
                status_summary.to_excel(
                    writer, sheet_name="Not Prescribed - Task Summary", index=False
                )
                print(f"  ✅ Not Prescribed - Task Summary: written")

            # Incomplete required tasks only — what's actually blocking people
            incomplete_summary = build_incomplete_task_summary(not_prescribed_df)
            if not incomplete_summary.empty:
                incomplete_summary.to_excel(
                    writer, sheet_name="Not Prescribed - Blockers", index=False
                )
                print(f"  ✅ Not Prescribed - Blockers: written")

    print(f"\n✅ Exported to {filename}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("🚀 Starting Georgia GLP-1 Member Analysis")
    print(f"   Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        conn = connect_to_db()
        print("  ✅ Connected to DB")
    except Exception as e:
        print(f"  ❌ Failed to connect to DB: {e}")
        sys.exit(1)

    # Step 1: Main member query
    df = run_georgia_glp1_query(conn)

    if df.empty:
        print("⚠️  No results returned. Check question IDs and partner employer name.")
        conn.close()
        sys.exit(0)

    print(f"\n📊 Member category breakdown:")
    print(df["member_category"].value_counts().to_string())

    # Step 2: Task analysis — all members with no 9amhealth GLP-1 Rx
    # Includes "Not Prescribed GLP-1" (30+ days, no Rx) and
    # "New Enrollee - No Rx Yet" (< 30 days, no Rx)
    NO_RX_CATEGORIES = {"Not Prescribed GLP-1", "New Enrollee - No Rx Yet"}
    not_prescribed_df = df[df["member_category"].isin(NO_RX_CATEGORIES)].copy()
    print(f"\n  → {len(not_prescribed_df):,} members with no 9amhealth GLP-1 Rx")

    if not not_prescribed_df.empty:
        member_ids        = not_prescribed_df["member_id"].tolist()
        task_df           = run_task_analysis(conn, member_ids)
        not_prescribed_df = merge_tasks_into_cohort(not_prescribed_df, task_df)
        not_prescribed_df = add_task_summary_columns(not_prescribed_df)

        # Console summary
        print(f"\n📊 Required task completion breakdown:")
        print(not_prescribed_df["tasks_completed_count"].value_counts().sort_index().to_string())

        print(f"\n  All required tasks done (awaiting Rx): "
              f"{not_prescribed_df['all_required_tasks_done'].sum():,}")

        print(f"\n📊 Task status breakdown (per slug):")
        for t in TASKS:
            col = t['status_col']
            if col in not_prescribed_df.columns:
                print(f"\n  {t['slug']} ({t['required_for']}):")
                print(
                    not_prescribed_df[col]
                    .fillna('NO TASK RECORD')
                    .value_counts()
                    .to_string()
                )
    else:
        print("  ⚠️  No 'Not Prescribed GLP-1' members found — task sheets will be skipped.")
        not_prescribed_df = None

    conn.close()

    # Step 3: Export
    export_to_excel(df, not_prescribed_df)


if __name__ == "__main__":
    main()
