import sqlite3
import json
import os

DB_PATH = 'gym_data.db'
ROSTER_PATH = 'internal_roster.json'

def generate_modified_gold():
    if not os.path.exists(ROSTER_PATH):
        print(f"Error: {ROSTER_PATH} not found.")
        return

    with open(ROSTER_PATH, 'r') as f:
        roster = json.load(f)

    # Get the list of full names from the roster
    # Some names might be duplicates (like Mykhailo Yatsiv) so use a set
    target_names = list(set([a['full_name'] for a in roster if a['full_name'] != '-']))

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA busy_timeout = 60000") # Wait up to 60 seconds if locked
    cursor = conn.cursor()

    # Placeholders for the query
    placeholders = ', '.join(['?'] * len(target_names))

    print(f"Targeting {len(target_names)} unique full names from roster.")

    # Check coverage in the database
    cursor.execute(f"SELECT DISTINCT athlete_name FROM Gold_Results_MAG WHERE athlete_name IN ({placeholders})", target_names)
    matched = set([row[0] for row in cursor.fetchall()])
    matched_count = len(matched)
    
    print(f"Found {matched_count} out of {len(target_names)} athletes in Gold_Results_MAG.")
    
    if matched_count < len(target_names):
        missing = [name for name in target_names if name not in matched]
        print("\nUNMATCHED ATHLETES FROM ROSTER:")
        for name in sorted(missing):
            print(f"  - {name}")
        print("")

    # Level 1: Only the matched athletes
    print("Creating Gold_Results_MAG_Filtered_L1...")
    cursor.execute("DROP TABLE IF EXISTS Gold_Results_MAG_Filtered_L1")
    cursor.execute(f"""
        CREATE TABLE Gold_Results_MAG_Filtered_L1 AS
        SELECT * FROM Gold_Results_MAG
        WHERE athlete_name IN ({placeholders})
    """, target_names)
    
    cursor.execute("SELECT COUNT(*) FROM Gold_Results_MAG_Filtered_L1")
    l1_rows = cursor.fetchone()[0]
    print(f"L1 created with {l1_rows} rows.")

    # Level 2: Matched athletes + their competitors in same meets and groups
    print("Creating Gold_Results_MAG_Filtered_L2...")
    cursor.execute("DROP TABLE IF EXISTS Gold_Results_MAG_Filtered_L2")
    
    # We define a "group" as (meet_name, level, age)
    # Since age can be NULL, we use COALESCE to ensure matching works
    cursor.execute(f"""
        CREATE TABLE Gold_Results_MAG_Filtered_L2 AS
        SELECT * FROM Gold_Results_MAG
        WHERE (meet_name, level, COALESCE(age, '')) IN (
            SELECT DISTINCT meet_name, level, COALESCE(age, '')
            FROM Gold_Results_MAG
            WHERE athlete_name IN ({placeholders})
        )
    """, target_names)

    cursor.execute("SELECT COUNT(*) FROM Gold_Results_MAG_Filtered_L2")
    l2_rows = cursor.fetchone()[0]
    print(f"L2 created with {l2_rows} rows.")

    conn.commit()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    generate_modified_gold()
