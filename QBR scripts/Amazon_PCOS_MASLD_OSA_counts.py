import pandas as pd
from config import get_db_config
import mysql.connector
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

#Note: The answer texts include leading spaces as per the database records for just PCOS
CONDITIONS = {
    "PCOS": [
        " Polycystic Ovarian Syndrome (PCOS or PCO)",
        " Síndrome de Ovario Poliquístico (SOP)"
    ],
    "MASLD": [
        "Fatty liver disease (NAFLD, NASH, or MASLD)",
        "Enfermedad de hígado graso (NAFLD, NASH o MASLD)"
    ],
    "OSA": [
        "Obstructive Sleep Apnea (OSA)",
        "Apnea obstructiva del sueño (AOS)"
    ]
}

ICD10S_MAP = {
    "PCOS": "'R73.03','I10','E78.5','G47.33','K76.0','I50.0','I25.1','I73.9','I63.9'",
    "MASLD": "'R73.03','I10','E78.5','G47.33','E28.2','I50.0','I25.1','I73.9','I63.9'",
    "OSA": "'R73.03','I10','E78.5','E28.2','K76.0','I50.0','I25.1','I73.9','I63.9'"
}

METRICS = [
    {"bmi_op": "<", "bmi_val": 35, "cond_count": 1},
    {"bmi_op": "<", "bmi_val": 35, "cond_count": 2},
    {"bmi_op": ">=", "bmi_val": 35, "cond_count": 0},
    {"bmi_op": ">=", "bmi_val": 35, "cond_count": 1},
    {"bmi_op": ">=", "bmi_val": 35, "cond_count": 2},
    {"bmi_op": ">=", "bmi_val": 40, "cond_count": 0},
    {"bmi_op": ">=", "bmi_val": 40, "cond_count": 1},
    {"bmi_op": ">=", "bmi_val": 40, "cond_count": 2},
]

def create_temp_tables(cursor):
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_amazon_users_base")
    cursor.execute("""
        CREATE TEMPORARY TABLE tmp_amazon_users_base AS
        SELECT DISTINCT s.user_id, s.start_date
        FROM subscriptions s
        JOIN partner_employers bus ON bus.user_id = s.user_id
        WHERE bus.name = 'Amazon'
        AND s.start_date <= '2026-01-01'
    """)
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_baseline_bmi")
    cursor.execute("""
        CREATE TEMPORARY TABLE tmp_baseline_bmi AS
        SELECT 
            bv.user_id,
            bv.value,
            bv.effective_date
        FROM bmi_values bv
        JOIN tmp_amazon_users_base au ON bv.user_id = au.user_id
        WHERE bv.effective_date = (
            SELECT MIN(bv2.effective_date)
            FROM bmi_values bv2
            WHERE bv2.user_id = bv.user_id
        )
    """)

def run_query(cursor, answer_texts, bmi_op, bmi_val, cond_count, icd10s, cond_name, metric_idx):
    logging.info(f"Running query for {cond_name} | Metric {metric_idx+1}/{len(METRICS)} | BMI {bmi_op} {bmi_val} | Condition Count {cond_count}")
    start = time.time()
    answer_in = ", ".join([f'"{txt}"' for txt in answer_texts])

    if cond_count == 0:
        # Users with none of the ICD10s for this condition
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_single_condition_users")
        cursor.execute(f"""
            CREATE TEMPORARY TABLE tmp_single_condition_users AS
            SELECT au.user_id
            FROM tmp_amazon_users_base au
            LEFT JOIN (
                SELECT user_id
                FROM medical_conditions
                WHERE icd10 IN ({icd10s})
                GROUP BY user_id
            ) mc ON mc.user_id = au.user_id
            WHERE mc.user_id IS NULL
        """)
    else:
        having_clause = f"HAVING COUNT(DISTINCT mc.icd10) = {cond_count}"
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_single_condition_users")
        cursor.execute(f"""
            CREATE TEMPORARY TABLE tmp_single_condition_users AS
            SELECT au.user_id
            FROM tmp_amazon_users_base au
            JOIN medical_conditions mc ON mc.user_id = au.user_id
            WHERE mc.icd10 IN ({icd10s})
            GROUP BY au.user_id
            {having_clause}
        """)

    # Main query
    query = f"""
        WITH user_eligibility as (
            select al.user_id, alf.value as eligibility_status
            from audit_logs al
            join audit_log_payload_fields alf on al.id = alf.audit_log_id and alf.`key` = 'eligibility'
            where al.event_name = 'program.generic.medical_eligibility_determined'
        )
        SELECT 
            COUNT(DISTINCT qr.user_id) as user_count,
            ue.eligibility_status
        FROM tmp_single_condition_users scu
        JOIN questionnaire_records qr ON qr.user_id = scu.user_id
        JOIN tmp_baseline_bmi bv ON bv.user_id = scu.user_id
        LEFT JOIN user_eligibility ue ON ue.user_id = scu.user_id
        WHERE qr.question_title LIKE 'Are you currently living with any of these conditions%'
          AND qr.answer_text IN ({answer_in})
          AND bv.value {bmi_op} {bmi_val}
        GROUP BY ue.eligibility_status
    """
    cursor.execute(query)
    elapsed = time.time() - start
    logging.info(f"Finished query for {cond_name} | Metric {metric_idx+1}/{len(METRICS)} in {elapsed:.2f}s")
    return cursor.fetchall()

def main():
    logging.info("Starting Amazon Condition Eligibility Export")
    db = mysql.connector.connect(**get_db_config())
    cursor = db.cursor(dictionary=True)
    writer = pd.ExcelWriter("Amazon_PCOS_MASLD_OSA.xlsx", engine="openpyxl")
    create_temp_tables(cursor)  # Only once!
    for cond_idx, (cond, answer_texts) in enumerate(CONDITIONS.items()):
        logging.info(f"Processing condition: {cond} ({cond_idx+1}/{len(CONDITIONS)})")
        results = []
        icd10s = ICD10S_MAP[cond]
        for metric_idx, metric in enumerate(METRICS):
            rows = run_query(
                cursor,
                answer_texts,
                metric["bmi_op"],
                metric["bmi_val"],
                metric["cond_count"],
                icd10s,
                cond,
                metric_idx
            )
            for row in rows:
                results.append({
                    "BMI_Operator": metric["bmi_op"],
                    "BMI_Value": metric["bmi_val"],
                    "Condition_Count": metric["cond_count"],
                    "Eligibility_Status": row["eligibility_status"],
                    "User_Count": row["user_count"]
                })
        # Filter out null eligibility_status
        results = [row for row in results if row["Eligibility_Status"] is not None]

        # Pivot so each eligibility status is its own column
        if results:
            df = pd.DataFrame(results)
            pivot_df = df.pivot_table(
                index=["BMI_Operator", "BMI_Value", "Condition_Count"],
                columns="Eligibility_Status",
                values="User_Count",
                fill_value=0
            ).reset_index()
            pivot_df.columns.name = None  # Remove pandas index name
            pivot_df.to_excel(writer, sheet_name=cond, index=False)
        else:
            # Write empty sheet if no results
            pd.DataFrame().to_excel(writer, sheet_name=cond, index=False)
        logging.info(f"Finished writing sheet for {cond}")
    writer.close()
    cursor.close()
    db.close()
    logging.info("✅ Exported to Amazon_PCOS_MASLD_OSA.xlsx")

if __name__ == "__main__":
    main()