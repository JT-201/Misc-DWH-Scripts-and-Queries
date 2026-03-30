import pandas as pd
from config import get_db_config
import mysql.connector

def get_connection():
    cfg = get_db_config()
    return mysql.connector.connect(**cfg)

query = """
WITH bony_members AS (
    SELECT
        BIN_TO_UUID(user_id) AS user_id
    FROM partner_employments
    WHERE partner_id = UUID_TO_BIN('dd06daf1-7c80-4482-836b-634709e80fac')
),
lab_orders AS (
    SELECT
        BIN_TO_UUID(lo.id)      AS lab_order_id,
        BIN_TO_UUID(lo.user_id) AS user_id,
        lo.status,
        lo.completed_at         AS date_of_service
    FROM labs_laborders lo
    INNER JOIN bony_members bm ON BIN_TO_UUID(lo.user_id) = bm.user_id
    WHERE lo.provider IN ('LABCORP', 'QUEST')
      AND lo.status IN ('COMPLETED', 'RESULTS_AVAILABLE')
),
prefs AS (
    SELECT
        BIN_TO_UUID(user_id) AS user_id,
        `key`,
        value
    FROM preferences_preferences
    WHERE BIN_TO_UUID(user_id) IN (SELECT user_id FROM lab_orders)
      AND `key` IN (
          'auth.user.firstname',
          'auth.user.lastname',
          'user.date-of-birth',
          'subscription.address.shipment.default.state'
      )
)
SELECT
    lo.user_id,
    lo.status,
    lo.date_of_service,
    MAX(CASE WHEN p.`key` = 'auth.user.firstname'                         THEN p.value END) AS first_name,
    MAX(CASE WHEN p.`key` = 'auth.user.lastname'                          THEN p.value END) AS last_name,
    MAX(CASE WHEN p.`key` = 'user.date-of-birth'                          THEN p.value END) AS date_of_birth,
    MAX(CASE WHEN p.`key` = 'subscription.address.shipment.default.state' THEN p.value END) AS state
FROM lab_orders lo
LEFT JOIN prefs p ON lo.user_id = p.user_id
GROUP BY
    lo.user_id,
    lo.status,
    lo.date_of_service
ORDER BY last_name, first_name
"""
conn = get_connection()
df = pd.read_sql(query, conn)
conn.close()

print(f"Total records: {len(df)}")
print(f"Missing DOB:   {df['date_of_birth'].isna().sum()}")
print(f"Missing state: {df['state'].isna().sum()}")
print(df.head())

output_path = "bony_lab_members.xlsx"
df.to_excel(output_path, index=False)
print(f"\nSaved to {output_path}")