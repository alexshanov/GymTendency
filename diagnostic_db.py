import sqlite3
import os

db_path = "gym_data.db"
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

try:
    conn = sqlite3.connect(db_path, timeout=5)
    cursor = conn.cursor()
    
    print("Checking for levels starting with 'B'...")
    cursor.execute("SELECT DISTINCT level FROM Results WHERE level LIKE 'B%'")
    b_levels = cursor.fetchall()
    print(f"Found B levels: {b_levels}")
    
    print("\nChecking for Daxton Hull records in 2023...")
    cursor.execute("SELECT athlete_name, level, meet_name, aa_score, aa_d FROM Gold_Results_MAG WHERE athlete_name LIKE 'Daxton Hull%' AND year = 2023")
    daxton_records = cursor.fetchall()
    for r in daxton_records:
        print(r)
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
