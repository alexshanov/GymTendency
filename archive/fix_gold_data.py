import sqlite3
import re
import etl_functions

DB_PATH = "gym_data.db"
SQL_DUMP_PATH = "Gold_Results_MAG_Filtered_L1_L1.sql"

def normalize_all_gold_tables():
    print("--- Normalizing Dates in Gold Tables ---")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    gold_tables = [
        "Gold_Results_MAG",
        "Gold_Results_MAG_Filtered_L1",
        "Gold_Results_MAG_Filtered_L2"
    ]
    
    for table in gold_tables:
        print(f"Processing {table}...")
        cursor.execute(f"SELECT rowid, date FROM {table}")
        rows = cursor.fetchall()
        
        updates = []
        for rowid, date_str in rows:
            if not date_str:
                continue
            
            new_date = etl_functions.parse_date_to_iso(date_str)
            if new_date != date_str:
                updates.append((new_date, rowid))
        
        if updates:
            print(f"  Updating {len(updates)} records in {table}...")
            cursor.executemany(f"UPDATE {table} SET date = ? WHERE rowid = ?", updates)
            
    conn.commit()
    conn.close()

def restore_anton_aa_ranks():
    print("\n--- Restoring Anton Shanov AA Ranks ---")
    
    # 1. Extract truth from SQL dump
    anton_truth = {}
    with open(SQL_DUMP_PATH, 'r') as f:
        for line in f:
            if "Anton Shanov" in line:
                # Naive extract meet and aa_rank
                # Format: (... VALUES ('Anton Shanov', 2025, '2025-02-28', 'Copeland Classic...', ..., 'aa_rank')
                # We'll use regex for robustness
                match = re.search(r"VALUES\s*\('(.*?)',\s*(\d+),\s*'(.*?)',\s*'(.*?)',.*?'(.*?)'\)\s*ON CONFLICT", line)
                if match:
                    name, year, date, meet, aa_rank = match.groups()
                    key = (name, year, meet)
                    if aa_rank:
                        anton_truth[key] = aa_rank
    
    print(f"Extracted {len(anton_truth)} Truth records for Anton.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 2. Cleanup Duplicates for Anton in L1
    # Check for duplicates (name, year, meet)
    cursor.execute("""
        DELETE FROM Gold_Results_MAG_Filtered_L1 
        WHERE rowid NOT IN (
            SELECT MIN(rowid) 
            FROM Gold_Results_MAG_Filtered_L1 
            WHERE athlete_name LIKE 'Anton Shanov%'
            GROUP BY athlete_name, year, meet_name
        ) 
        AND athlete_name LIKE 'Anton Shanov%'
    """)
    print(f"Deduplicated Anton records in L1: {cursor.rowcount} removed.")
    
    # 3. Backfill Ranks
    for (name, year, meet), rank in anton_truth.items():
        cursor.execute("""
            UPDATE Gold_Results_MAG_Filtered_L1 
            SET aa_rank = ? 
            WHERE athlete_name = ? AND year = ? AND meet_name = ? AND (aa_rank IS NULL OR aa_rank = '')
        """, (rank, name, int(year), meet))
        if cursor.rowcount > 0:
            print(f"  Restored AA Rank '{rank}' for {meet} ({year})")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    normalize_all_gold_tables()
    restore_anton_aa_ranks()
