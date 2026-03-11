import os
os.environ['DB_ENV'] = 'be-prod'

import sys
import pandas as pd
import mysql.connector

sys.path.insert(0, os.path.expanduser("~/Documents/9am_dev_queries"))
from config import get_db_config

# ── Connection ───────────────────────────────────────────────────────────────
# Make sure your SSH tunnel is running first:
# ssh -N -L 3310:backendproduseast-databas-databaseserverlesscluste-7t7ceazm12ty.cluster-cdjmnv40d5ja.us-east-1.rds.amazonaws.com:3306 9am-bastion-prod

config = get_db_config()
config['connect_timeout'] = 300
conn = mysql.connector.connect(**config)

# ── Query ─────────────────────────────────────────────────────────────────────
query = """
SELECT DISTINCT
    km.first_name,
    km.last_name,
    km.dob,
    km.kwiktrip_id,
    BIN_TO_UUID(lo.user_id) AS user_id,
    lo.status,
    lo.trigger_at AS lab_date
FROM labs_laborders lo
JOIN (
    SELECT DISTINCT pec.user_id, pesud.value AS kwiktrip_id
    FROM partner_eligibility_checks pec
    JOIN partner_eligibility_specific_user_data pesud
        ON pesud.eligibility_check_id = pec.id
        AND pesud.`key` = 'id'
    WHERE pec.payer_id = UUID_TO_BIN('287fcc30-03df-45f0-a00f-7f4b2814da0d')
) mapping ON mapping.user_id = lo.user_id
JOIN (
    SELECT kwiktrip_id, MIN(first_name) AS first_name, MIN(last_name) AS last_name, MIN(dob) AS dob
    FROM kwiktrip_members
    GROUP BY kwiktrip_id
) km ON km.kwiktrip_id = mapping.kwiktrip_id
WHERE lo.trigger_at >= '2026-01-01'
  AND lo.trigger_at <  '2026-03-01'
ORDER BY lo.trigger_at, km.last_name, km.first_name
"""

cursor = conn.cursor(dictionary=True)
cursor.execute(query)
rows = cursor.fetchall()
cursor.close()
conn.close()

df = pd.DataFrame(rows)
print(f"Rows returned: {len(df)}")

output_path = "kwiktrip_labs_jan_feb_2026.xlsx"
df.to_excel(output_path, index=False, sheet_name="Lab Orders")
print(f"Saved: {output_path}")