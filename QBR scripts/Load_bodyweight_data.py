import mysql.connector
from config import get_db_config  # Use your encrypted config

conn = mysql.connector.connect(
    **get_db_config(),
    allow_local_infile=True  # Enable local infile for CSV loading
)

cursor = conn.cursor()

csv_path = '/Users/joshuatolentino/Downloads/filtered_20251222_110911.csv'

query = f"""
LOAD DATA LOCAL INFILE '{csv_path}'
INTO TABLE body_weight_values_cleaned
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\\n'
IGNORE 1 LINES
(@id_UUID, @user_id_UUID, intake,effective, source, value)
SET user_id = UUID_TO_BIN(@user_id_UUID),
    id = UUID_TO_BIN(@id_UUID);
"""

cursor.execute(query)
conn.commit()
cursor.close()
conn.close()