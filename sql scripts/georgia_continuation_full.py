# ---------------------------------------------------------------------------
# Georgia GLP-1 Continuation Members — Full Detail Pull
# Cohort: State of Georgia, active, answered yes (1) to A8z9j98E0sxR
# ---------------------------------------------------------------------------

import mysql.connector
import pandas as pd
import numpy as np
import time
import sys
import warnings
from datetime import datetime, date

warnings.filterwarnings('ignore')

try:
    from config import get_db_config
except ImportError:
    print("❌ Error: Could not find 'config.py'.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# TASK DEFINITIONS
# ---------------------------------------------------------------------------
TASKS = [
    {'slug': 'glp1-continuation-questionnaire', 'col_prefix': 'task_glp1_questionnaire', 'description': 'Continuation questionnaire'},
    {'slug': 'upload-prescription-label',        'col_prefix': 'task_rx_label',           'description': 'Upload prescription label'},
    {'slug': 'pharmacy-insurance',               'col_prefix': 'task_pharmacy_insurance', 'description': 'Pharmacy insurance info'},
    {'slug': 'complete-initial-lab-order',       'col_prefix': 'task_lab_order',          'description': 'Complete initial lab order'},
    {'slug': 'upload-proof-of-weight',           'col_prefix': 'task_weight_proof',       'description': 'Upload proof of weight'},
    {'slug': 'preferred-pharmacy',               'col_prefix': 'task_preferred_pharmacy', 'description': 'Select preferred pharmacy'},
]
for t in TASKS:
    t['status_col']    = f"{t['col_prefix']}_status"
    t['started_col']   = f"{t['col_prefix']}_started_at"
    t['completed_col'] = f"{t['col_prefix']}_completed_at"

TASK_SLUGS = [t['slug'] for t in TASKS]

# ---------------------------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------------------------
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
            sys.stdout.write(f"\r    ...{sum(len(c) for c in chunks):,} rows")
            sys.stdout.flush()
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        print(f"\n    ✅ {len(df):,} rows in {time.time()-start:.1f}s")
        return df
    except Exception as e:
        print(f"\n    ❌ Error fetching {desc}: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# MAIN MEMBER QUERY
# ---------------------------------------------------------------------------
def run_main_query(conn):
    query = """
        WITH

        -- ── Cohort: all active SoG members ──────────────────────────────────
        georgia_active AS (
            SELECT
                u.id                                            AS member_id,
                u.readable_id,
                u.primary_condition_group,
                s.subscription_start_date,
                s.cancellation_date,
                DATEDIFF(CURDATE(), s.subscription_start_date)  AS days_enrolled
            FROM users u
            JOIN (
                SELECT DISTINCT user_id
                FROM partner_employers
                WHERE name = 'State of Georgia'
            ) pe ON pe.user_id = u.id
            JOIN (
                SELECT user_id,
                       MIN(start_date)        AS subscription_start_date,
                       MAX(cancellation_date) AS cancellation_date
                FROM subscriptions
                WHERE status = 'ACTIVE' AND cancellation_date IS NULL
                GROUP BY user_id
            ) s ON s.user_id = u.id
        ),

        -- ── PII ──────────────────────────────────────────────────────────────
        member_names AS (
            SELECT
                user_id                                         AS member_id,
                UPPER(first_name)                               AS first_name,
                UPPER(last_name)                                AS last_name,
                UPPER(CONCAT(first_name, ' ', last_name))       AS full_name_upper,
                date_of_birth,
                UPPER(default_shipping_address_street)          AS shipping_street,
                UPPER(default_shipping_address_city)            AS shipping_city,
                UPPER(default_shipping_address_state)           AS shipping_state,
                default_shipping_address_zip                    AS shipping_zip
            FROM nineamdwh_restricted.user_details
        ),

        -- ── Survey: which GLP-1 drug ─────────────────────────────────────────
        reported_med AS (
            SELECT member_id, reported_medication_name FROM (
                SELECT user_id AS member_id, answer_text AS reported_medication_name,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY answered_at DESC) AS rn
                FROM questionnaire_records
                WHERE question_id = 'knzp0ZppEBF4' AND is_latest_answer = 1
            ) r WHERE rn = 1
        ),

        -- ── Survey: past 3-month GLP-1 use (A8z9j98E0sxR) ─────────────────
        glp1_past_use AS (
            SELECT member_id, past_glp1_use, past_glp1_use_answered_at FROM (
                SELECT
                    user_id      AS member_id,
                    answer_value AS past_glp1_use,
                    answered_at  AS past_glp1_use_answered_at,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY answered_at DESC) AS rn
                FROM questionnaire_records
                WHERE question_id = 'A8z9j98E0sxR' AND is_latest_answer = 1
            ) r WHERE rn = 1
        ),

        -- ── Survey: wants to continue GLP-1 ─────────────────────────────────
        continuation_answer AS (
            SELECT member_id, wants_to_continue_glp1, continuation_answered_at
            FROM (
                SELECT
                    user_id      AS member_id,
                    answer_value AS wants_to_continue_glp1,
                    answered_at  AS continuation_answered_at,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY answered_at DESC) AS rn
                FROM questionnaire_records
                WHERE question_id = 'gV9Xu8RzF9hR' AND is_latest_answer = 1
            ) ranked
            WHERE rn = 1
        ),

        -- ── Survey: most recent dose date ────────────────────────────────────
        last_dose_date AS (
            SELECT member_id, most_recent_dose_date_raw FROM (
                SELECT user_id AS member_id, answer_text AS most_recent_dose_date_raw,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY answered_at DESC) AS rn
                FROM questionnaire_records
                WHERE question_id = 'Pm2WEZgGJ6hT' AND is_latest_answer = 1
            ) r WHERE rn = 1
        ),

        -- ── Survey: doses remaining ──────────────────────────────────────────
        doses_survey AS (
            SELECT member_id, doses_remaining_at_survey, doses_question_answered_at FROM (
                SELECT user_id AS member_id,
                       answer_value AS doses_remaining_at_survey,
                       answered_at  AS doses_question_answered_at,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY answered_at DESC) AS rn
                FROM questionnaire_records
                WHERE question_id = 'UUeznpkuACcR' AND is_latest_answer = 1
            ) r WHERE rn = 1
        ),

        -- ── All GLP-1 Rx (WM + DM) — for latest AND first-ever ──────────────
        glp1_rx_all AS (
            SELECT
                p.patient_user_id                               AS member_id,
                BIN_TO_UUID(p.id)                               AS prescription_id,
                m.name                                          AS prescribed_medication_name,
                m.therapy_type,
                mdc.drug_class_name,
                p.prescribed_at,
                DATE(p.prescribed_at)                           AS rx_date,
                p.days_of_supply,
                p.total_refills,
                p.is_valid,
                p.days_of_supply * (1 + COALESCE(p.total_refills, 0)) AS total_covered_days,
                DATE_ADD(p.prescribed_at,
                    INTERVAL p.days_of_supply * (1 + COALESCE(p.total_refills, 0)) DAY
                )                                               AS coverage_end_date,
                CASE WHEN DATE_ADD(p.prescribed_at,
                    INTERVAL p.days_of_supply * (1 + COALESCE(p.total_refills, 0)) DAY
                ) >= CURDATE() THEN 1 ELSE 0 END                AS rx_covers_today,
                ROW_NUMBER() OVER (
                    PARTITION BY p.patient_user_id ORDER BY p.prescribed_at DESC
                )                                               AS rn_latest,
                ROW_NUMBER() OVER (
                    PARTITION BY p.patient_user_id ORDER BY p.prescribed_at ASC
                )                                               AS rn_first
            FROM prescriptions p
            JOIN medication_dosage_ndcs mdn ON mdn.ndc = p.prescribed_ndc
            JOIN medication_dosages md      ON md.id = mdn.medication_dosage_id
            JOIN medications m              ON m.id = md.medication_id
            JOIN medication_drug_classes mdc ON mdc.medication_id = m.id
            WHERE mdc.drug_class_name = 'GLP1'
              AND m.therapy_type IN ('WM', 'DM')
        ),

        latest_glp1_rx AS (
            SELECT * FROM glp1_rx_all WHERE rn_latest = 1
        ),

        first_glp1_rx AS (
            SELECT
                member_id,
                prescribed_at  AS first_glp1_rx_date,
                prescribed_medication_name AS first_glp1_rx_drug_name,
                therapy_type   AS first_glp1_rx_therapy_type
            FROM glp1_rx_all WHERE rn_first = 1
        ),

        -- ── WM Rx since 2026-01-01 ───────────────────────────────────────────
        wm_rx_2026_all AS (
            SELECT
                p.patient_user_id                               AS member_id,
                m.name                                          AS wm_rx_drug_name,
                m.therapy_type                                  AS wm_rx_therapy_type,
                DATE(p.prescribed_at)                           AS wm_rx_date,
                p.prescribed_at                                 AS wm_prescribed_at,
                ROW_NUMBER() OVER (
                    PARTITION BY p.patient_user_id ORDER BY p.prescribed_at DESC
                )                                               AS rn_latest,
                ROW_NUMBER() OVER (
                    PARTITION BY p.patient_user_id ORDER BY p.prescribed_at ASC
                )                                               AS rn_first
            FROM prescriptions p
            JOIN medication_dosage_ndcs mdn ON mdn.ndc = p.prescribed_ndc
            JOIN medication_dosages md      ON md.id = mdn.medication_dosage_id
            JOIN medications m              ON m.id = md.medication_id
            WHERE m.therapy_type = 'WM'
              AND DATE(p.prescribed_at) >= '2026-01-01'
        ),

        wm_rx_2026 AS (
            SELECT
                member_id,
                1                                               AS has_wm_rx_since_2026,
                MAX(CASE WHEN rn_first  = 1 THEN wm_rx_date END) AS first_wm_rx_2026_date,
                MAX(CASE WHEN rn_latest = 1 THEN wm_rx_date END) AS latest_wm_rx_2026_date,
                MAX(CASE WHEN rn_latest = 1 THEN wm_rx_drug_name END) AS latest_wm_rx_drug_name,
                MAX(CASE WHEN rn_latest = 1 THEN wm_rx_therapy_type END) AS latest_wm_rx_therapy_type,
                MAX(CASE WHEN rn_latest = 1 THEN wm_prescribed_at END) AS latest_wm_prescribed_at
            FROM wm_rx_2026_all
            GROUP BY member_id
        ),

        -- ── Other non-GLP1 Rx (all time) ────────────────────────────────────
        other_rx_all AS (
            SELECT
                p.patient_user_id                               AS member_id,
                m.name                                          AS rx_drug_name,
                m.therapy_type                                  AS rx_therapy_type,
                mdc.drug_class_name                             AS rx_drug_class,
                DATE(p.prescribed_at)                           AS rx_date,
                ROW_NUMBER() OVER (
                    PARTITION BY p.patient_user_id ORDER BY p.prescribed_at DESC
                )                                               AS rn
            FROM prescriptions p
            JOIN medication_dosage_ndcs mdn ON mdn.ndc = p.prescribed_ndc
            JOIN medication_dosages md      ON md.id = mdn.medication_dosage_id
            JOIN medications m              ON m.id = md.medication_id
            JOIN medication_drug_classes mdc ON mdc.medication_id = m.id
            WHERE mdc.drug_class_name != 'GLP1'
        ),

        other_rx_summary AS (
            SELECT
                member_id,
                1                                               AS has_other_non_glp1_rx,
                COUNT(*)                                        AS other_rx_total_count,
                MAX(CASE WHEN rn = 1 THEN rx_drug_name    END) AS other_rx_latest_drug_name,
                MAX(CASE WHEN rn = 1 THEN rx_drug_class   END) AS other_rx_latest_drug_class,
                MAX(CASE WHEN rn = 1 THEN rx_therapy_type END) AS other_rx_latest_therapy_type,
                MAX(CASE WHEN rn = 1 THEN rx_date         END) AS other_rx_latest_date
            FROM other_rx_all
            GROUP BY member_id
        ),

        -- ── Baseline weight ──────────────────────────────────────────────────
        baseline_weight AS (
            SELECT member_id, baseline_weight_lbs, baseline_weight_date
            FROM (
                SELECT
                    bwv.user_id                                 AS member_id,
                    ROUND(bwv.value * 2.20462, 2)               AS baseline_weight_lbs,
                    bwv.effective_date                          AS baseline_weight_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY bwv.user_id ORDER BY bwv.effective_date ASC
                    )                                           AS rn
                FROM body_weight_values_cleaned bwv
                JOIN georgia_active g ON g.member_id = bwv.user_id
                WHERE bwv.value IS NOT NULL
                  AND bwv.effective_date >= DATE_SUB(g.subscription_start_date, INTERVAL 30 DAY)
            ) ranked
            WHERE rn = 1
        ),

        -- ── Height + weight for BMI ──────────────────────────────────────────
        member_height AS (
            SELECT member_id, height_cm FROM (
                SELECT user_id AS member_id, answer_value AS height_cm,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY answered_at DESC) AS rn
                FROM questionnaire_records
                WHERE question_id = 'o6Fn8WhK92TQ' AND is_latest_answer = 1
            ) r WHERE rn = 1
        ),

        member_weight AS (
            SELECT member_id, weight_kg FROM (
                SELECT user_id AS member_id, answer_value AS weight_kg,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY answered_at DESC) AS rn
                FROM questionnaire_records
                WHERE question_id = '4Zi8ggSNSqvL' AND is_latest_answer = 1
            ) r WHERE rn = 1
        )

        SELECT
            -- Identifiers
            g.member_id,
            g.readable_id,
            nm.first_name,
            nm.last_name,
            nm.full_name_upper,
            nm.date_of_birth,
            nm.shipping_street,
            nm.shipping_city,
            nm.shipping_state,
            nm.shipping_zip,

            -- Enrollment
            g.primary_condition_group,
            g.subscription_start_date,
            g.cancellation_date,
            g.days_enrolled,

            -- Past GLP-1 use flag
            pu.past_glp1_use,
            CASE WHEN pu.past_glp1_use = 1 THEN 'Yes' WHEN pu.past_glp1_use = 0 THEN 'No' ELSE 'Not Answered' END AS is_glp1_continuation_member,
            pu.past_glp1_use_answered_at,

            -- Continuation survey
            cont.wants_to_continue_glp1,
            cont.continuation_answered_at,

            -- Reported drug
            rm.reported_medication_name,

            -- Dose survey
            d.doses_remaining_at_survey,
            d.doses_question_answered_at,
            ldd.most_recent_dose_date_raw,

            -- Baseline weight
            bw.baseline_weight_lbs,
            bw.baseline_weight_date,

            -- BMI
            mh.height_cm,
            mw.weight_kg,
            CASE
                WHEN mh.height_cm > 0 AND mw.weight_kg > 0
                THEN ROUND(mw.weight_kg / POW(mh.height_cm / 100, 2), 1)
                ELSE NULL
            END                                                 AS calculated_bmi,

            -- Latest GLP-1 Rx
            rx.prescription_id,
            rx.prescribed_medication_name,
            rx.therapy_type,
            rx.drug_class_name,
            rx.is_valid                                         AS rx_is_valid,
            rx.rx_date                                          AS glp1_rx_date,
            rx.days_of_supply,
            rx.total_refills,
            rx.total_covered_days,
            rx.coverage_end_date,
            rx.rx_covers_today,

            -- First-ever GLP-1 Rx from 9am
            frx.first_glp1_rx_date,
            frx.first_glp1_rx_drug_name,
            frx.first_glp1_rx_therapy_type,

            -- WM Rx since 2026
            COALESCE(wm.has_wm_rx_since_2026, 0)               AS has_wm_rx_since_2026,
            wm.first_wm_rx_2026_date,
            wm.latest_wm_rx_2026_date,
            wm.latest_wm_rx_drug_name,
            wm.latest_wm_rx_therapy_type,
            wm.latest_wm_prescribed_at,

            -- Other non-GLP1 Rx
            COALESCE(orx.has_other_non_glp1_rx, 0)             AS has_other_non_glp1_rx,
            orx.other_rx_total_count,
            orx.other_rx_latest_drug_name,
            orx.other_rx_latest_drug_class,
            orx.other_rx_latest_therapy_type,
            orx.other_rx_latest_date

        FROM georgia_active g
        LEFT JOIN member_names        nm   ON nm.member_id   = g.member_id
        LEFT JOIN reported_med        rm   ON rm.member_id   = g.member_id
        LEFT JOIN glp1_past_use       pu   ON pu.member_id   = g.member_id
        LEFT JOIN continuation_answer cont ON cont.member_id = g.member_id
        LEFT JOIN doses_survey        d    ON d.member_id    = g.member_id
        LEFT JOIN last_dose_date      ldd  ON ldd.member_id  = g.member_id
        LEFT JOIN baseline_weight     bw   ON bw.member_id   = g.member_id
        LEFT JOIN member_height       mh   ON mh.member_id   = g.member_id
        LEFT JOIN member_weight       mw   ON mw.member_id   = g.member_id
        LEFT JOIN latest_glp1_rx      rx   ON rx.member_id   = g.member_id
        LEFT JOIN first_glp1_rx       frx  ON frx.member_id  = g.member_id
        LEFT JOIN wm_rx_2026          wm   ON wm.member_id   = g.member_id
        LEFT JOIN other_rx_summary    orx  ON orx.member_id  = g.member_id

        ORDER BY nm.last_name, nm.first_name
    """
    return get_data(conn, query, "Georgia continuation members (full detail)")


# ---------------------------------------------------------------------------
# TASK PULL — all members
# ---------------------------------------------------------------------------
def run_task_analysis(conn, member_ids):
    if not member_ids:
        return pd.DataFrame()
    print(f"\n  📥 Fetching tasks for {len(member_ids):,} members...")
    start = time.time()
    placeholders = ", ".join(["%s"] * len(member_ids))
    slug_list     = ", ".join([f"'{s}'" for s in TASK_SLUGS])
    case_blocks = []
    for t in TASKS:
        p, s = t['col_prefix'], t['slug']
        case_blocks.append(f"""
            MAX(CASE WHEN t.slug = '{s}' THEN t.status       END) AS {p}_status,
            MAX(CASE WHEN t.slug = '{s}' THEN t.started_at   END) AS {p}_started_at,
            MAX(CASE WHEN t.slug = '{s}' THEN t.completed_at END) AS {p}_completed_at""")
    query = f"""
        SELECT t.user_id AS member_id, {','.join(case_blocks)}
        FROM tasks t
        WHERE t.user_id IN ({placeholders})
          AND t.slug IN ({slug_list})
        GROUP BY t.user_id
    """
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, member_ids)
        df = pd.DataFrame(cursor.fetchall())
        print(f"    ✅ {len(df):,} members with task records in {time.time()-start:.1f}s")
        return df
    except Exception as e:
        print(f"    ❌ Error fetching tasks: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# MEDICAL CONDITIONS
# ---------------------------------------------------------------------------
def get_medical_conditions(conn, member_ids):
    if not member_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(member_ids))
    query = f"""
        SELECT mc.user_id AS member_id, mc.name AS condition_name,
               mc.icd10, mc.source, mc.recorded_at
        FROM medical_conditions mc
        WHERE mc.user_id IN ({placeholders})
        ORDER BY mc.user_id, mc.recorded_at
    """
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, member_ids)
        df = pd.DataFrame(cursor.fetchall())
        print(f"    ✅ {len(df):,} condition records")
        return df
    except Exception as e:
        print(f"    ❌ Error fetching conditions: {e}")
        return pd.DataFrame()

# Comorbidity definitions: (flag_col, [name keywords], [icd10 prefixes])
COMORBIDITIES = [
    ('comorb_type2_diabetes',   ['type 2 diabetes', 'type ii diabetes', 't2d', 'diabetes mellitus'],
                                 ['E11']),
    ('comorb_prediabetes',      ['prediabetes', 'pre-diabetes', 'impaired fasting glucose', 'igt'],
                                 ['R73', 'E10']),
    ('comorb_hypertension',     ['hypertension', 'high blood pressure', 'htn'],
                                 ['I10', 'I11', 'I12', 'I13']),
    ('comorb_dyslipidemia',     ['dyslipidemia', 'hyperlipidemia', 'high cholesterol',
                                  'hypercholesterolemia', 'mixed hyperlipidemia'],
                                 ['E78']),
    ('comorb_sleep_apnea',      ['sleep apnea', 'obstructive sleep apnea', 'osa'],
                                 ['G47.3', 'G47.33']),
    ('comorb_cardiovascular',   ['cardiovascular', 'heart attack', 'myocardial infarction',
                                  'stroke', 'coronary artery disease', 'cad', 'heart failure',
                                  'atrial fibrillation', 'peripheral artery disease'],
                                 ['I21', 'I22', 'I25', 'I50', 'I63', 'I64', 'I48', 'I73']),
    ('comorb_mash_nafld',       ['mash', 'nafld', 'nash', 'nonalcoholic',
                                  'metabolic dysfunction', 'steatohepatitis', 'fatty liver'],
                                 ['K75.81', 'K76.0', 'K74']),
]

def _matches_comorbidity(name_str, icd10_str, name_keywords, icd10_prefixes):
    name_lower = (name_str or '').lower()
    icd_str    = (icd10_str or '').upper()
    if any(kw in name_lower for kw in name_keywords):
        return True
    if any(icd_str.startswith(pfx.upper()) for pfx in icd10_prefixes):
        return True
    return False

def summarize_conditions(cond_df):
    if cond_df.empty:
        cols = ['member_id', 'all_conditions', 'all_icd10s'] + [c[0] for c in COMORBIDITIES]
        return pd.DataFrame(columns=cols)
    rows = []
    for mid, grp in cond_df.groupby('member_id'):
        row = {
            'member_id':      mid,
            'all_conditions': ', '.join(grp['condition_name'].dropna().unique().tolist()),
            'all_icd10s':     ', '.join(grp['icd10'].dropna().unique().tolist()),
        }
        # Check each condition row against each comorbidity definition
        for flag_col, name_kws, icd_pfxs in COMORBIDITIES:
            flag = 0
            for _, crow in grp.iterrows():
                if _matches_comorbidity(crow.get('condition_name'), crow.get('icd10'),
                                        name_kws, icd_pfxs):
                    flag = 1
                    break
            row[flag_col] = flag
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TASK + DERIVED COLUMNS
# ---------------------------------------------------------------------------
def merge_tasks(df, task_df):
    if task_df.empty:
        for t in TASKS:
            for col in [t['status_col'], t['started_col'], t['completed_col']]:
                df[col] = None
        return df
    return pd.merge(df, task_df, on='member_id', how='left')

def add_task_summary_cols(df):
    lab_col   = 'task_lab_order_status'
    quest_col = 'task_glp1_questionnaire_status'

    df['lab_done']           = (df[lab_col].fillna('').str.upper()  == 'COMPLETED').astype(int)
    df['questionnaire_done'] = (df[quest_col].fillna('').str.upper() == 'COMPLETED').astype(int)
    df['both_tasks_done']    = ((df['lab_done'] == 1) & (df['questionnaire_done'] == 1)).astype(int)
    df['neither_task_done']  = ((df['lab_done'] == 0) & (df['questionnaire_done'] == 0)).astype(int)

    status_cols = [t['status_col'] for t in TASKS]
    df['tasks_completed_count'] = sum(
        df[c].fillna('').str.upper().eq('COMPLETED').astype(int)
        for c in status_cols if c in df.columns
    )

    # Time from both tasks done → WM Rx (days)
    lab_comp   = 'task_lab_order_completed_at'
    quest_comp = 'task_glp1_questionnaire_completed_at'
    if lab_comp in df.columns and quest_comp in df.columns:
        lab_dt   = pd.to_datetime(df[lab_comp],   errors='coerce')
        quest_dt = pd.to_datetime(df[quest_comp], errors='coerce')
        df['both_tasks_completed_at'] = pd.DataFrame({'l': lab_dt, 'q': quest_dt}).max(axis=1)
        wm_rx_dt = pd.to_datetime(df['latest_wm_prescribed_at'], errors='coerce')
        df['days_both_tasks_to_wm_rx'] = (wm_rx_dt - df['both_tasks_completed_at']).dt.days
        df.loc[df['days_both_tasks_to_wm_rx'] < 0, 'days_both_tasks_to_wm_rx'] = np.nan

    return df


def assign_member_category(row):
    """
    Priority order:
    1. Not a continuation member (past_glp1_use = 0) → 'Not GLP-1 Continuation Member'
    2. Has a WM Rx from 9am (since 2026)             → 'Has WM Rx from 9amHealth'
    3. No WM Rx — then check tasks:
       a. Both lab + questionnaire done  → 'No WM Rx — Both Tasks Complete'
       b. Lab done only                  → 'No WM Rx — Lab Complete Only'
       c. Questionnaire done only        → 'No WM Rx — Questionnaire Complete Only'
       d. Neither done                   → 'No WM Rx — No Tasks Complete'
    """
    if row.get('past_glp1_use', None) != 1:
        return 'Not GLP-1 Continuation Member'
    if row.get('has_wm_rx_since_2026', 0) == 1:
        return 'Has WM Rx from 9amHealth'
    lab   = row.get('lab_done', 0)
    quest = row.get('questionnaire_done', 0)
    if lab == 1 and quest == 1:
        return 'No WM Rx — Both Tasks Complete'
    if lab == 1 and quest == 0:
        return 'No WM Rx — Lab Complete Only'
    if lab == 0 and quest == 1:
        return 'No WM Rx — Questionnaire Complete Only'
    return 'No WM Rx — No Tasks Complete'


# ---------------------------------------------------------------------------
# POST-PROCESSING: dose calcs
# ---------------------------------------------------------------------------
def add_derived_columns(df):
    today = pd.Timestamp(date.today())

    df['most_recent_dose_date'] = pd.to_datetime(
        df['most_recent_dose_date_raw'], errors='coerce'
    )
    survey_dt = pd.to_datetime(df['doses_question_answered_at'], errors='coerce')

    # Days/weeks since the survey was answered (this is our baseline for decay)
    df['days_since_survey_answered']  = (today - survey_dt).dt.days
    df['weeks_since_survey_answered'] = (df['days_since_survey_answered'] / 7).apply(
        lambda x: int(x) if pd.notna(x) else None
    )

    # Estimated doses remaining today:
    # doses_remaining_at_survey minus full weeks elapsed since they answered
    # (weekly injection assumed — one dose consumed per week)
    df['estimated_doses_remaining_today'] = (
        df['doses_remaining_at_survey'] - df['weeks_since_survey_answered']
    ).clip(lower=0)

    # Days since last dose (informational — from most_recent_dose_date)
    df['days_since_last_dose'] = (today - df['most_recent_dose_date']).dt.days

    # Likely missed dose: estimated doses have run out
    df['likely_missed_dose'] = (
        df['estimated_doses_remaining_today'].fillna(-1) <= 0
    ).astype(int)
    # Only flag if we actually have dose survey data
    no_data = df['doses_remaining_at_survey'].isna()
    df.loc[no_data, 'likely_missed_dose'] = np.nan

    # Doses at time of WM Rx: how many doses did they have when they got prescribed?
    # = doses_remaining_at_survey minus weeks between survey answer and Rx date
    wm_rx_dt = pd.to_datetime(df['latest_wm_prescribed_at'], errors='coerce')
    weeks_survey_to_rx = ((wm_rx_dt - survey_dt).dt.days / 7).apply(
        lambda x: max(int(x), 0) if pd.notna(x) else None
    )
    df['estimated_doses_at_rx_time'] = (
        df['doses_remaining_at_survey'] - weeks_survey_to_rx
    ).clip(lower=0)
    # Not meaningful if Rx came before the survey was answered
    df.loc[wm_rx_dt < survey_dt, 'estimated_doses_at_rx_time'] = np.nan

    return df


# ---------------------------------------------------------------------------
# HELPER: any weight-related comorbidity
# ---------------------------------------------------------------------------
WEIGHT_COMORB_COLS = [
    'comorb_type2_diabetes', 'comorb_prediabetes', 'comorb_hypertension',
    'comorb_dyslipidemia', 'comorb_sleep_apnea', 'comorb_cardiovascular',
    'comorb_mash_nafld',
]

def has_weight_comorb(row):
    return int(any(row.get(c, 0) == 1 for c in WEIGHT_COMORB_COLS))

# ---------------------------------------------------------------------------
# DRUG SWITCHING TABLE
# ---------------------------------------------------------------------------
def build_drug_switching_table(df):
    """
    For members with a WM Rx: cross-tab of reported drug (came in on)
    vs. prescribed drug (what 9am wrote).
    Shows continuation on same med vs. switching trends.
    """
    rx_df = df[df['has_wm_rx_since_2026'] == 1].copy()
    if rx_df.empty:
        return pd.DataFrame()

    rx_df['reported_drug_clean']   = rx_df['reported_medication_name'].fillna('Not Reported').str.strip().str.title()
    rx_df['prescribed_drug_clean'] = rx_df['latest_wm_rx_drug_name'].fillna('Unknown').str.strip().str.title()

    # Normalize to brand name for comparison — extract first word (brand name)
    # e.g. "Wegovy (Semaglutide)" → "wegovy"
    #      "Wegovy Subcutaneous Solution Auto-Injector" → "wegovy"
    #      "Zepbound (Tirzepatide)" → "zepbound"
    def extract_brand(s):
        if pd.isna(s) or str(s).strip() == '':
            return ''
        # Take first word, lowercase, strip punctuation
        import re
        return re.sub(r'[^a-z]', '', str(s).lower().split()[0])

    rx_df['_reported_brand']  = rx_df['reported_drug_clean'].apply(extract_brand)
    rx_df['_prescribed_brand'] = rx_df['prescribed_drug_clean'].apply(extract_brand)

    rx_df['same_med'] = np.where(
        (rx_df['_reported_brand'] == '') | (rx_df['_prescribed_brand'] == ''),
        'Unknown',
        np.where(rx_df['_reported_brand'] == rx_df['_prescribed_brand'], 'Same Med', 'Switched')
    )

    # Cross-tab: reported (rows) × prescribed (cols)
    pivot = pd.crosstab(
        rx_df['reported_drug_clean'],
        rx_df['prescribed_drug_clean'],
        margins=True,
        margins_name='TOTAL'
    ).reset_index()
    pivot.insert(0, 'Reported Drug (came in on)', pivot.pop('reported_drug_clean'))

    # Summary rows
    same  = int((rx_df['same_med'] == 'Same Med').sum())
    switched = int((rx_df['same_med'] == 'Switched').sum())
    total = len(rx_df)

    summary = pd.DataFrame([
        {'Metric': 'Total members with WM Rx', 'Count': total, 'Pct': ''},
        {'Metric': 'Stayed on same med',        'Count': same,    'Pct': f'{same/total*100:.1f}%' if total else ''},
        {'Metric': 'Switched to different med', 'Count': switched,'Pct': f'{switched/total*100:.1f}%' if total else ''},
    ])

    return pivot, summary, rx_df[['readable_id','first_name','last_name',
                                   'reported_drug_clean','prescribed_drug_clean',
                                   'same_med','latest_wm_rx_2026_date',
                                   'days_both_tasks_to_wm_rx'
                                   ]].rename(columns={
                                       'reported_drug_clean':   'reported_drug',
                                       'prescribed_drug_clean': 'prescribed_drug',
                                   })


# ---------------------------------------------------------------------------
# SUMMARY SHEET
# ---------------------------------------------------------------------------
def build_summary(df):
    today = pd.Timestamp(date.today())
    n     = len(df)
    cont  = df[df['past_glp1_use'] == 1]
    nc    = len(cont) if len(cont) > 0 else 1

    rows = []
    def row(metric, value='', note=''):
        rows.append({'Metric': metric, 'Value': value, 'Note': note})

    # ── Section 1: Total Enrollment Snapshot ─────────────────────────────────
    row('━━ SECTION 1: TOTAL ENROLLMENT SNAPSHOT ━━')
    row('Run date', today.strftime('%Y-%m-%d'))
    row('Total active SoG members (answered A8z9j98E0sxR)', n)
    continuation     = int((df['past_glp1_use'] == 1).sum())
    non_continuation = int((df['past_glp1_use'] != 1).sum())
    row('  On WM med upon signup (continuation members)', continuation,
        f'{continuation/n*100:.1f}%')
    row('  NOT on WM med upon signup', non_continuation,
        f'{non_continuation/n*100:.1f}%')

    # Enrollment trend by month
    row('', '')
    row('  Enrollment by month (signup date)')
    if 'subscription_start_date' in df.columns:
        df['enroll_month'] = pd.to_datetime(
            df['subscription_start_date'], errors='coerce'
        ).dt.to_period('M').astype(str)
        for mo, cnt in df['enroll_month'].value_counts().sort_index().items():
            row(f'    {mo}', int(cnt))

    # ── Section 2: Continuation Members ──────────────────────────────────────
    row('', '')
    row('━━ SECTION 2: CONTINUATION MEMBERS (on WM med at signup) ━━')
    row('Total continuation members', nc)

    # Dropped off: no Rx + enrolled 30+ days + no task activity at all
    dropped = cont[
        (cont['has_wm_rx_since_2026'] == 0) &
        (cont['days_enrolled'] >= 30) &
        (cont['tasks_completed_count'] == 0)
    ]
    pending = cont[
        (cont['has_wm_rx_since_2026'] == 0) &
        ~(
            (cont['days_enrolled'] >= 30) &
            (cont['tasks_completed_count'] == 0)
        )
    ]
    prescribed = cont[cont['has_wm_rx_since_2026'] == 1]

    row('  Prescribed WM Rx by 9amHealth',  len(prescribed), f'{len(prescribed)/nc*100:.1f}%')
    row('  Pending (no Rx yet, showing activity)', len(pending), f'{len(pending)/nc*100:.1f}%')
    row('  Dropped off (no Rx + 30+ days + no tasks)', len(dropped), f'{len(dropped)/nc*100:.1f}%')

    # Enrolled → Prescribed timing
    row('', '')
    row('  ── Enrolled → Prescribed Timing ──')
    if 'first_glp1_rx_date' in cont.columns and 'subscription_start_date' in cont.columns:
        rx_timing = cont[cont['has_wm_rx_since_2026'] == 1].copy()
        rx_timing['days_enroll_to_rx'] = (
            pd.to_datetime(rx_timing['first_glp1_rx_date'], errors='coerce') -
            pd.to_datetime(rx_timing['subscription_start_date'], errors='coerce')
        ).dt.days
        valid_timing = rx_timing['days_enroll_to_rx'].dropna()
        if len(valid_timing):
            row('  Avg days enrollment → first Rx',    round(valid_timing.mean(), 1))
            row('  Median days enrollment → first Rx', round(valid_timing.median(), 1))

    # Task funnel (continuation members only)
    row('', '')
    row('  ── Task Funnel (continuation members) ──')
    row('  Completed lab order',      int(cont['lab_done'].sum()),
        f"{cont['lab_done'].mean()*100:.1f}%")
    row('  Completed questionnaire',  int(cont['questionnaire_done'].sum()),
        f"{cont['questionnaire_done'].mean()*100:.1f}%")
    row('  Completed BOTH tasks',     int(cont['both_tasks_done'].sum()),
        f"{cont['both_tasks_done'].mean()*100:.1f}%")
    row('  Completed NEITHER task',   int(cont['neither_task_done'].sum()),
        f"{cont['neither_task_done'].mean()*100:.1f}%")

    if 'days_both_tasks_to_wm_rx' in cont.columns:
        valid = cont['days_both_tasks_to_wm_rx'].dropna()
        if len(valid):
            row('  Avg days: both tasks done → WM Rx',    round(valid.mean(), 1))
            row('  Median days: both tasks done → WM Rx', round(valid.median(), 1))

    # BMI eligibility for both-tasks-done, no Rx yet
    row('', '')
    row('  ── BMI Eligibility: Both Tasks Done, No WM Rx Yet ──')
    pending_both = cont[
        (cont['both_tasks_done'] == 1) &
        (cont['has_wm_rx_since_2026'] == 0)
    ]
    row('  Members in this group', len(pending_both))
    if len(pending_both):
        bmi_gt30     = pending_both['calculated_bmi'] > 30
        bmi_27_30    = (pending_both['calculated_bmi'] > 27) & (pending_both['calculated_bmi'] <= 30)
        bmi_le27     = pending_both['calculated_bmi'] <= 27
        bmi_unknown  = pending_both['calculated_bmi'].isna()

        # Weight-related comorbidity flag
        if all(c in pending_both.columns for c in WEIGHT_COMORB_COLS):
            has_comorb = pending_both.apply(has_weight_comorb, axis=1).astype(bool)
        else:
            has_comorb = pd.Series(False, index=pending_both.index)

        row('  BMI > 30 (eligible, no clinical barrier)',
            int(bmi_gt30.sum()), f'{bmi_gt30.mean()*100:.1f}% of group')
        row('  BMI 27–30 with qualifying comorbidity',
            int((bmi_27_30 & has_comorb).sum()))
        row('  BMI 27–30 with NO comorbidity on file (needs review)',
            int((bmi_27_30 & ~has_comorb).sum()))
        row('  BMI ≤ 27 with 1+ weight-related comorbidity',
            int((bmi_le27 & has_comorb).sum()))
        row('  BMI ≤ 27 no comorbidity',
            int((bmi_le27 & ~has_comorb).sum()))
        row('  BMI unknown', int(bmi_unknown.sum()))

    # ── Section 4: New-to-Med Members ────────────────────────────────────────
    row('', '')
    row('━━ SECTION 4: NEW-TO-MED MEMBERS (not on WM med at signup) ━━')
    new_mem = df[df['past_glp1_use'] != 1]
    nn = len(new_mem) if len(new_mem) > 0 else 1
    row('Total new-to-med members', len(new_mem))
    new_rx = new_mem[new_mem['has_wm_rx_since_2026'] == 1]
    row('  Received new WM Rx', len(new_rx), f'{len(new_rx)/nn*100:.1f}%')
    row('  No Rx yet',          len(new_mem) - len(new_rx),
        f'{(len(new_mem)-len(new_rx))/nn*100:.1f}%')
    if 'therapy_type' in new_rx.columns:
        wm_rx  = int((new_rx['therapy_type'] == 'WM').sum())
        dm_rx  = int((new_rx['therapy_type'] == 'DM').sum())
        row('  WM (GLP-1 weight mgmt) Rx', wm_rx)
        row('  DM (diabetes) Rx', dm_rx)
    if 'first_glp1_rx_date' in new_rx.columns and 'subscription_start_date' in new_rx.columns:
        new_timing = (
            pd.to_datetime(new_rx['first_glp1_rx_date'], errors='coerce') -
            pd.to_datetime(new_rx['subscription_start_date'], errors='coerce')
        ).dt.days.dropna()
        if len(new_timing):
            row('  Avg days enrollment → first Rx', round(new_timing.mean(), 1))
            row('  Median days enrollment → first Rx', round(new_timing.median(), 1))

    # ── Full BMI breakdown with Rx split ─────────────────────────────────────
    row('', '')
    row('━━ BMI BREAKDOWN (all members, with/without WM Rx) ━━')
    bmi_avail = df['calculated_bmi'].notna().sum()
    row('Members with BMI calculable', int(bmi_avail))
    if bmi_avail > 0:
        row('Avg BMI', round(df['calculated_bmi'].mean(), 1))
        for label, mask in [
            ('BMI > 30',  df['calculated_bmi'] > 30),
            ('BMI 27–30', (df['calculated_bmi'] > 27) & (df['calculated_bmi'] <= 30)),
            ('BMI ≤ 27',  df['calculated_bmi'] <= 27),
        ]:
            grp     = df[mask]
            with_rx = int((grp['has_wm_rx_since_2026'] == 1).sum())
            no_rx   = int((grp['has_wm_rx_since_2026'] == 0).sum())
            row(f'  {label}', int(mask.sum()))
            row(f'    → Has WM Rx',  with_rx)
            row(f'    → No WM Rx',   no_rx)

        # BMI ≤ 27 with 1+ weight-related comorbidity
        if all(c in df.columns for c in WEIGHT_COMORB_COLS):
            le27 = df['calculated_bmi'] <= 27
            has_c = df.apply(has_weight_comorb, axis=1).astype(bool)
            row('  BMI ≤ 27 with 1+ weight-related comorbidity',
                int((le27 & has_c).sum()))
        row('  BMI unknown', int(df['calculated_bmi'].isna().sum()))

    # ── Tasks (all members) ───────────────────────────────────────────────────
    row('', '')
    row('━━ TASKS (all members) ━━')
    row('Completed lab order',     int(df['lab_done'].sum()),
        f"{df['lab_done'].mean()*100:.1f}%")
    row('Completed questionnaire', int(df['questionnaire_done'].sum()),
        f"{df['questionnaire_done'].mean()*100:.1f}%")
    row('Completed BOTH tasks',    int(df['both_tasks_done'].sum()),
        f"{df['both_tasks_done'].mean()*100:.1f}%")
    row('Completed NEITHER task',  int(df['neither_task_done'].sum()),
        f"{df['neither_task_done'].mean()*100:.1f}%")

    # ── WM Rx ─────────────────────────────────────────────────────────────────
    row('', '')
    row('━━ WM Rx (9amHealth, since 2026) ━━')
    row('Members with WM Rx since 2026', int(df['has_wm_rx_since_2026'].sum()),
        f"{df['has_wm_rx_since_2026'].mean()*100:.1f}%")
    row('Members with active GLP-1 Rx today',
        int(df['rx_covers_today'].fillna(0).sum()))
    row('Members with no GLP-1 Rx ever', int(df['prescription_id'].isna().sum()))
    row('Members with other non-GLP1 Rx', int(df['has_other_non_glp1_rx'].sum()))

    if 'days_both_tasks_to_wm_rx' in df.columns:
        valid = df['days_both_tasks_to_wm_rx'].dropna()
        if len(valid):
            row('Avg days: both tasks done → WM Rx',    round(valid.mean(), 1))
            row('Median days: both tasks done → WM Rx', round(valid.median(), 1))

    # ── Doses ─────────────────────────────────────────────────────────────────
    row('', '')
    row('━━ DOSES ━━')
    answered = df['doses_remaining_at_survey'].notna().sum()
    row('Members who answered dose survey', int(answered))
    if answered > 0:
        missed = int(df['likely_missed_dose'].eq(1).sum())
        row('Likely missed a dose (est. ≤0 remaining)', missed,
            f"{missed/answered*100:.1f}% of those who answered")
        row('Avg estimated doses remaining today',
            round(df['estimated_doses_remaining_today'].dropna().mean(), 1))

    # ── Comorbidities ─────────────────────────────────────────────────────────
    row('', '')
    row('━━ COMORBIDITIES (continuation members only) ━━')
    comorb_labels = {
        'comorb_type2_diabetes':  'Type 2 Diabetes',
        'comorb_prediabetes':     'Prediabetes',
        'comorb_hypertension':    'Hypertension / High Blood Pressure',
        'comorb_dyslipidemia':    'Dyslipidemia / High Cholesterol',
        'comorb_sleep_apnea':     'Obstructive Sleep Apnea (OSA)',
        'comorb_cardiovascular':  'Cardiovascular Disease',
        'comorb_mash_nafld':      'MASH / NAFLD (Fatty Liver)',
    }
    for col, label in comorb_labels.items():
        if col in cont.columns:
            cnt = int(cont[col].sum())
            row(f'  {label}', cnt, f'{cnt/nc*100:.1f}% of continuation members')

    # ── Reported drug ─────────────────────────────────────────────────────────
    row('', '')
    row('━━ REPORTED DRUG (what members said they came in on) ━━')
    drug_counts = df['reported_medication_name'].value_counts(dropna=True)
    for drug, cnt in drug_counts.items():
        row(f'  {drug}', int(cnt))
    row('  Not reported', int(df['reported_medication_name'].isna().sum()))

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------
def export_to_excel(df, summary_df):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"georgia_continuation_full_{timestamp}.xlsx"
    print(f"\n  📤 Exporting to {filename}...")

    drop_cols = ['most_recent_dose_date_raw']
    df_out = df.drop(columns=[c for c in drop_cols if c in df.columns])

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:

        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        print(f"  ✅ Summary")

        df_out.to_excel(writer, sheet_name="All Members", index=False)
        print(f"  ✅ All Members: {len(df_out):,} rows")

        # One sheet per member category
        for cat in df_out['member_category'].value_counts().index:
            sheet_df   = df_out[df_out['member_category'] == cat]
            sheet_name = cat[:31]
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  ✅ {cat}: {len(sheet_df):,} rows")

        # Drug switching tab (members with WM Rx only)
        result = build_drug_switching_table(df_out)
        if result:
            pivot, switch_summary, switch_detail = result
            switch_summary.to_excel(writer, sheet_name="Drug Switching — Summary",
                                    index=False)
            pivot.to_excel(writer, sheet_name="Drug Switching — Crosstab",
                           index=False)
            switch_detail.to_excel(writer, sheet_name="Drug Switching — Detail",
                                   index=False)
            print(f"  ✅ Drug Switching sheets written")

        # Dropped off members
        dropped = df_out[
            (df_out['past_glp1_use'] == 1) &
            (df_out['has_wm_rx_since_2026'] == 0) &
            (df_out['days_enrolled'] >= 30) &
            (df_out['tasks_completed_count'] == 0)
        ]
        if len(dropped):
            dropped.to_excel(writer, sheet_name="Dropped Off", index=False)
            print(f"  ✅ Dropped Off: {len(dropped):,} rows")

        # Both tasks done, no Rx — BMI eligibility detail
        pending_both = df_out[
            (df_out['past_glp1_use'] == 1) &
            (df_out['both_tasks_done'] == 1) &
            (df_out['has_wm_rx_since_2026'] == 0)
        ]
        if len(pending_both):
            pending_both.to_excel(writer, sheet_name="Pending — Both Tasks Done",
                                  index=False)
            print(f"  ✅ Pending — Both Tasks Done: {len(pending_both):,} rows")

    print(f"\n✅ Saved: {filename}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("🚀 Georgia GLP-1 Continuation — Full Detail Pull")
    print(f"   Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        conn = connect_to_db()
        print("  ✅ Connected to DB\n")
    except Exception as e:
        print(f"  ❌ Failed to connect: {e}")
        sys.exit(1)

    try:
        # 1. Main member pull
        df = run_main_query(conn)
        if df.empty:
            print("⚠️  No members returned.")
            sys.exit(0)

        print(f"\n  👥 Total members: {len(df):,}")
        member_ids = df['member_id'].tolist()

        # 2. Tasks
        task_df = run_task_analysis(conn, member_ids)
        df = merge_tasks(df, task_df)
        df = add_task_summary_cols(df)

        # 3. Medical conditions
        print(f"\n  📥 Fetching medical conditions...")
        cond_df      = get_medical_conditions(conn, member_ids)
        cond_summary = summarize_conditions(cond_df)
        df = pd.merge(df, cond_summary, on='member_id', how='left')

        # 4. Derived dose + timing columns
        df = add_derived_columns(df)

        # 5. Member category
        df['member_category'] = df.apply(assign_member_category, axis=1)

        # 6. Console summary
        print(f"\n📊 Member Categories:")
        print(df['member_category'].value_counts().to_string())
        print(f"\n📊 BMI available:      {df['calculated_bmi'].notna().sum():,} of {len(df):,}")
        print(f"📊 Has WM Rx (2026):   {df['has_wm_rx_since_2026'].sum():,}")
        print(f"📊 Likely missed dose: {df['likely_missed_dose'].eq(1).sum():,}")
        if 'days_both_tasks_to_wm_rx' in df.columns:
            valid = df['days_both_tasks_to_wm_rx'].dropna()
            if len(valid):
                print(f"📊 Avg days tasks→Rx:  {valid.mean():.1f}")

        # 7. Build summary + export
        summary_df = build_summary(df)
        export_to_excel(df, summary_df)

    except Exception as e:
        print(f"\n💥 Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
