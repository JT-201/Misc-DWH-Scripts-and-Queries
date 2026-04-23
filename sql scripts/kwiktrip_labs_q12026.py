# DWH-437
# Kwik Trip Q1 2026 Lab Orders Report
# Pulls completed lab orders for Kwik Trip members (Jan-Mar 2026),
# joins with member name/DOB from preferences, and exports to Excel.
# Developer: Megan Riddle
# Last updated: 2026-04-15
# Note: Kwiktrip ID is shared within a family. Use preferences 
# table for first/last name and DOB to differentiate members.

import os
os.environ['DB_ENV'] = 'be-prod'

import sys
import pandas as pd
import mysql.connector

sys.path.insert(0, os.path.expanduser("~/Documents/9am_dev_queries"))
from config import get_db_config

config = get_db_config()
config['connect_timeout'] = 300
conn = mysql.connector.connect(**config)

query = """
WITH kwiktrip_mapping AS (
    SELECT DISTINCT
        pec.user_id,
        pesud.value AS kwiktrip_id
    FROM partner_eligibility_checks pec
    JOIN partner_eligibility_specific_user_data pesud
        ON pesud.eligibility_check_id = pec.id
        AND pesud.`key` = 'id'
    WHERE pec.payer_id = UUID_TO_BIN('287fcc30-03df-45f0-a00f-7f4b2814da0d')
),
lab_orders AS (
    SELECT
        lo.user_id,
        m.kwiktrip_id,
        lo.status,
        lo.trigger_at AS lab_date
    FROM labs_laborders lo
    INNER JOIN kwiktrip_mapping m ON m.user_id = lo.user_id
    WHERE lo.status IN ('COMPLETED', 'RESULTS_AVAILABLE')
      AND lo.trigger_at >= '2026-01-01'
      AND lo.trigger_at <  '2026-04-01'
),
prefs AS (
    SELECT
        p.user_id,
        p.`key`,
        p.value
    FROM preferences_preferences p
    INNER JOIN (SELECT DISTINCT user_id FROM lab_orders) lo
        ON p.user_id = lo.user_id
    WHERE p.`key` IN (
        'auth.user.firstname',
        'auth.user.lastname',
        'user.date-of-birth'
    )
)
SELECT
    MAX(CASE WHEN p.`key` = 'auth.user.firstname'  THEN p.value END) AS first_name,
    MAX(CASE WHEN p.`key` = 'auth.user.lastname'   THEN p.value END) AS last_name,
    MAX(CASE WHEN p.`key` = 'user.date-of-birth'   THEN p.value END) AS dob,
    lo.kwiktrip_id,
    BIN_TO_UUID(lo.user_id) AS user_id,
    lo.status,
    lo.lab_date
FROM lab_orders lo
LEFT JOIN prefs p ON p.user_id = lo.user_id
GROUP BY
    lo.user_id,
    lo.kwiktrip_id,
    lo.status,
    lo.lab_date
ORDER BY lo.lab_date, last_name, first_name
"""

cursor = conn.cursor(dictionary=True)
cursor.execute(query)
rows = cursor.fetchall()
cursor.close()
conn.close()

df = pd.DataFrame(rows)
print(f"Rows returned: {len(df)}")
print(f"Missing first_name: {df['first_name'].isna().sum()}")
print(f"Missing last_name:  {df['last_name'].isna().sum()}")
print(f"Missing dob:        {df['dob'].isna().sum()}")

for col in ['first_name', 'last_name', 'dob']:
    df[col] = df[col].str.strip('"')

output_path = "kwiktrip_labs_q1_2026.xlsx"
df.to_excel(output_path, index=False, sheet_name="Lab Orders")
print(f"Saved: {output_path}")
