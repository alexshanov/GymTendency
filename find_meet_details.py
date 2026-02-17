import sqlite3
import pandas as pd

conn = sqlite3.connect('gym_data.db')
query = "SELECT meet_db_id, source, source_meet_id, name FROM Meets WHERE name LIKE '%Copeland%' AND comp_year = 2025"
df = pd.read_sql_query(query, conn)
print(df)
conn.close()
