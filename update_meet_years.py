import sqlite3
import pandas as pd
import os
import re

DB_FILE = "gym_data.db"
KSCORE_MANIFEST = "discovered_meet_ids_kscore.csv"

def update_years():
    if not os.path.exists(DB_FILE):
        print("Database not found.")
        return
    
    if not os.path.exists(KSCORE_MANIFEST):
        print("K-Score manifest not found.")
        return

    print("--- Updating Meet Years from K-Score Manifest ---")
    manifest = pd.read_csv(KSCORE_MANIFEST)
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    updates = 0
    for _, row in manifest.iterrows():
        source_meet_id = str(row['MeetID']).replace('kscore_', '')
        start_date = row['start_date_iso']
        comp_year = row['Year']
        
        cursor.execute("""
            UPDATE Meets 
            SET start_date_iso = ?, comp_year = ? 
            WHERE source = 'kscore' AND source_meet_id = ?
        """, (start_date, int(comp_year), source_meet_id))
        
        updates += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"Successfully updated {updates} K-Score meet records.")

if __name__ == "__main__":
    update_years()
