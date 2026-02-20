
import sqlite3

db_file = "gym_data.db"
meet_ids = [192, 298, 45, 39, 271, 176]
file_patterns = [
    'C5432FCE37715FF3C29F88080A34FDD6',
    'DED0EA6FC0639F27504AC7F32A72F26E',
    'altadore_ev25',
    'C134634F5870232007708E3DDE052043',
    'E59F531DB80CE78C226742F6A762490F',
    'F84F37265D8E2574088E8DCF785E3E5A'
]

conn = sqlite3.connect(db_file)
cursor = conn.cursor()

for m_id in meet_ids:
    print(f"Purging records for meet_db_id: {m_id}")
    cursor.execute("DELETE FROM Results WHERE meet_db_id = ?", (m_id,))

for pattern in file_patterns:
    print(f"Purging file signatures matching: {pattern}")
    cursor.execute("DELETE FROM ProcessedFiles WHERE file_path LIKE ?", (f"%{pattern}%",))

conn.commit()
conn.close()
print("Purge complete.")
