import pandas as pd
import glob
import os
import sqlite3
import json
from etl_functions import load_column_aliases, sanitize_column_name

# --- CONFIGURATION ---
FOLDERS = [
    "CSVs_kscore_final",
    "CSVs_Livemeet_final",
    "CSVs_mso_final"
]
DB_FILE = "gym_data.db"

def get_db_columns():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(Results)")
        return {row[1].lower() for row in cursor.fetchall()}

def get_csv_columns():
    all_cols = set()
    file_map = {} # col -> list of example files
    
    print("Scanning CSV headers...")
    for folder in FOLDERS:
        if not os.path.exists(folder):
            print(f"Skipping missing folder: {folder}")
            continue
            
        files = glob.glob(os.path.join(folder, "*.csv"))
        for f in files:
            try:
                # Read just header
                df = pd.read_csv(f, nrows=0)
                for col in df.columns:
                    col_clean = col.strip()
                    all_cols.add(col_clean)
                    if col_clean not in file_map: file_map[col_clean] = []
                    if len(file_map[col_clean]) < 3: file_map[col_clean].append(os.path.basename(f))
            except Exception as e:
                pass
    return all_cols, file_map

def main():
    db_cols = get_db_columns()
    csv_cols, file_map = get_csv_columns()
    aliases = load_column_aliases()
    
    # Normalize aliases map for checking
    alias_map_normalized = {k.lower(): v.lower() for k, v in aliases.items()}
    
    # Known "Consumed" columns (Identity / Apparatus) that don't become DB columns directly
    consumed_cols = {
        'gymnast', 'athlete', 'name', 
        'club', 'team', 
        'meet', # Usually maps to meet_id
        'gender', # implicit
        '#' # Often empty or just an index
    }
    
    missed_cols = []
    
    print("\n--- AUDIT REPORT: CSV Columns vs Database ---")
    
    for raw_col in sorted(csv_cols):
        raw_lower = raw_col.lower()
        
        # 1. Is it a standard DB column?
        if raw_lower in db_cols:
            continue
            
        # 2. Is it in the alias list?
        if raw_lower in alias_map_normalized:
            target = alias_map_normalized[raw_lower]
            if target in db_cols:
                continue
        
        # 3. Is it a Consumed Identity column?
        if raw_lower in consumed_cols:
            continue
            
        # 4. Is it an Apparatus Result column? (Result_Event_Score/D/Rnk) or Raw Apparatus (VT, UB...)
        if raw_lower.startswith('result_'):
            continue
            
        # MSO Apparatus abbreviations (Explicitly mapped in mso_load_data.py to Apparatus rows)
        mso_apps = ['vt', 'ub', 'bb', 'fx', 'aa', 'ph', 'sr', 'pb', 'hb', 'hibar', 'pbars', 'pomml', 'bars', 'beam', 'floor', 'vault', 'high bar', 'parallel bars', 'uneven bars', 'rings', 'pommel horse', 'all around']
        if raw_lower in mso_apps:
             continue
             
        # 5. Check actual sanitization
        sanitized = sanitize_column_name(raw_col)
        if sanitized in db_cols:
             continue
        
        missed_cols.append((raw_col, file_map[raw_col]))

    if missed_cols:
        print(f"\n⚠️  POTENTIALLY MISSED COLUMNS ({len(missed_cols)}):")
        print("   (These exist in CSVs but verify if they are in the DB under a different name or truly ignored)")
        for col, files in missed_cols:
            print(f"   [?] '{col}' (found in {files})")
    else:
        print("\n🎉 NO MISSED COLUMNS DETECTED!")
        print("   All CSV columns are either in the DB, aliased, consumed as identity, or pivoted as apparatus data.")

if __name__ == "__main__":
    main()
