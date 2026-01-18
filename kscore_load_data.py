# kscore_load_data.py

import sqlite3
import pandas as pd
import os
import glob
import traceback
import json
import re

# --- Import shared functions from our new ETL library ---
from etl_functions import (
    setup_database,
    load_club_aliases,
    standardize_club_name,
    standardize_athlete_name,
    detect_discipline,
    get_or_create_person,
    get_or_create_club,
    get_or_create_athlete_link,
    get_or_create_meet
)

# --- CONFIGURATION (Specific to Kscore) ---
DB_FILE = "gym_data.db"
KSCORE_CSVS_DIR = "CSVs_kscore_final" 
KSCORE_MEET_MANIFEST_FILE = "discovered_meet_ids_kscore.csv"

# ==============================================================================
#  DATA LOADING FUNCTIONS (Specific to Kscore)
# ==============================================================================

def load_meet_manifest(manifest_file):
    """
    Reads the Kscore meet manifest CSV into a dictionary for easy lookups.
    """
    print(f"--- Loading Kscore meet manifest from '{manifest_file}' ---")
    try:
        manifest_df = pd.read_csv(manifest_file)
        manifest = {
            row['MeetID']: {
                'name': row['MeetName'],
                'start_date_iso': row['start_date_iso'],
                'location': row['Location'],
                'year': row['Year']
            }
            for _, row in manifest_df.iterrows()
        }
        print(f"Successfully loaded details for {len(manifest)} Kscore meets into memory.")
        return manifest
    except (FileNotFoundError, KeyError) as e:
        print(f"Warning: Could not load Kscore manifest. Meet details will be incomplete. Error: {e}")
        return {} 

def process_kscore_files(meet_manifest, club_alias_map):
    """
    Main function to find and process all Kscore result CSV files.
    """
    print("\n--- Starting to process Kscore result files ---")
    
    search_pattern = os.path.join(KSCORE_CSVS_DIR, "*_FINAL_*.csv")
    csv_files = glob.glob(search_pattern)

    if not csv_files:
        print(f"Warning: No result files found in '{KSCORE_CSVS_DIR}'.")
        return
        
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # --- Caches now map to the new schema ---
            person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
            club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
            apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
            meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}

            for filepath in csv_files:
                print(f"\nProcessing file: {os.path.basename(filepath)}")
                # Pass all the new caches to the parsing function
                parse_kscore_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map)

    except Exception as e:
        print(f"A critical error occurred during file processing: {e}")
        traceback.print_exc()

def parse_kscore_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map):
    """
    Parses a single Kscore CSV and loads its data into the database using the new schema.
    """
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty:
            print("File is empty. Skipping.")
            return
    except Exception as e:
        print(f"Warning: Could not read CSV file '{filepath}'. Error: {e}")
        return

    full_source_id = os.path.basename(filepath).split('_FINAL_')[0]
    source_meet_id = full_source_id.replace('kscore_', '', 1)
    meet_details = meet_manifest.get(full_source_id, {})
    if not meet_details.get('name'):
        meet_details['name'] = df['Meet'].iloc[0] if 'Meet' in df.columns and not df.empty else f"Kscore {source_meet_id}"
    meet_db_id = get_or_create_meet(conn, 'kscore', source_meet_id, meet_details, meet_cache)

    discipline_id, discipline_name, gender_heuristic = detect_discipline(df)
    print(f"  Detected Discipline: {discipline_name}")

    # --- KEY MAPPING FOR IDS ---
    # We still need to know which columns hold the Identity info
    KEY_MAP = {
        'Gymnast': 'Name', 'Athlete': 'Name', 'Name': 'Name',
        'Club': 'Club', 'Team': 'Club',
        'Level': 'Level',
        'Age': 'Age',
        'Prov': 'Prov',
        'Meets': 'Meet' # Fallback
    }

    # Find the critical columns in this specific file
    col_map = {col: KEY_MAP.get(col, col) for col in df.columns}
    name_col = next((c for c, v in col_map.items() if v == 'Name'), None)
    
    if not name_col:
        print(f"File matches no known Name column (checked Athlete, Gymnast, Name). Skipping.")
        return

    from etl_functions import sanitize_column_name, ensure_column_exists
    from etl_functions import check_duplicate_result, validate_score, standardize_score_status

    cursor = conn.cursor()
    athletes_processed = 0
    results_inserted = 0

    # Pre-calculate which columns are "Dynamic Results" vs "Apparatus Results"
    # K-Score structure: Result_Event_D, Result_Event_Score
    # We will treat "Result_" columns as apparatus data to be normalized into rows?
    # WAIT. The user wants "columns as is". 
    # BUT we are inserting into a relational 'Results' table which has `apparatus_id`.
    # So we MUST pivot the apparatus columns (D/Score/Rank) into rows.
    # The *Dynamic* columns are things like "Start Value" or "Execution" that are NOT part of the standard triplet.
    
    # Identify apparatus triplets
    result_columns = [col for col in df.columns if col.startswith('Result_')]
    event_bases = {}
    for col in result_columns:
        match = re.search(r'Result_(.*)_(Score|D|Rnk)$', col)
        if match:
            raw_event_name = match.group(1)
            event_bases[raw_event_name] = raw_event_name

    # Identify other dynamic columns (Metadata that isn't Name/Club/Apparatus)
    # These will be added as columns to the Results table
    ignore_cols = list(event_bases.keys()) + result_columns + [name_col]
    if 'Club' in df.columns: ignore_cols.append('Club')
    
    dynamic_cols_to_add = []
    for col in df.columns:
        if col not in ignore_cols and not col.startswith('Result_'):
            # This is a candidate for a new column (e.g. "USAG #", "Session")
            sanitized = sanitize_column_name(col)
            ensure_column_exists(cursor, 'Results', sanitized, 'TEXT')
            dynamic_cols_to_add.append((col, sanitized))

    # Ensure apparatus-specific bonus columns exist
    ensure_column_exists(cursor, 'Results', 'bonus', 'REAL')
    ensure_column_exists(cursor, 'Results', 'execution_bonus', 'REAL')

    for index, row in df.iterrows():
        # 1. Identity
        raw_name = row.get(name_col)
        person_name = standardize_athlete_name(raw_name)
        if not person_name: continue
        athletes_processed += 1
        
        person_id = get_or_create_person(conn, person_name, gender_heuristic, person_cache)
        
        raw_club = row.get('Club', '')
        club_name = standardize_club_name(raw_club, club_alias_map)
        club_id = get_or_create_club(conn, club_name, club_cache)
        athlete_id = get_or_create_athlete_link(conn, person_id, club_id, athlete_cache)
        
        # 2. Extract Dynamic Values for this row
        dynamic_values = {}
        for raw_col, safe_col in dynamic_cols_to_add:
            val = row.get(raw_col)
            if val: dynamic_values[safe_col] = str(val)

        # 3. Process Apparatus (Pivot)
        for raw_event, _ in event_bases.items():
            # Clean up event name for lookup (remove underscores for matching?)
            # K-Score typical: "Balance_Beam"
            # Apparatus Cache typical: "Balance Beam" or "Beam"
            clean_name = raw_event.replace('_', ' ')
            
            # Legacy mapping support if needed, but we try to match "As Is" first
            if clean_name == "Balance Beam": clean_name = "Beam" # Small normalization for matching ID
            
            app_key = (clean_name, discipline_id)
            if app_key not in apparatus_cache:
                 # Try strict match on raw event if simple replace didn't work
                 app_key = (raw_event, discipline_id)
            if app_key not in apparatus_cache: app_key = (clean_name, 99) 
            
            if app_key not in apparatus_cache: 
                # print(f"Warning: Unknown apparatus {clean_name}")
                continue
            
            apparatus_id = apparatus_cache[app_key]
            
            # Extract Triplet + Bonuses
            d_val = row.get(f'Result_{raw_event}_D')
            score_val = row.get(f'Result_{raw_event}_Score')
            rank_val = row.get(f'Result_{raw_event}_Rnk')
            bonus_val = row.get(f'Result_{raw_event}_Bonus')
            exec_bonus_val = row.get(f'Result_{raw_event}_Exec_Bonus') or row.get(f'Result_{raw_event}_Execution_Bonus')
            
            if not score_val and not d_val: continue
            
            # Numeric conversion
            score_numeric = pd.to_numeric(score_val, errors='coerce')
            d_numeric = pd.to_numeric(d_val, errors='coerce')
            rank_numeric = pd.to_numeric(rank_val, errors='coerce')
            bonus_numeric = pd.to_numeric(bonus_val, errors='coerce')
            exec_bonus_numeric = pd.to_numeric(exec_bonus_val, errors='coerce')
            
            if check_duplicate_result(conn, meet_db_id, athlete_id, apparatus_id): continue
            
            # Dynamic INSERT Construction
            cols = ['meet_db_id', 'athlete_id', 'apparatus_id', 'gender', 'score_final', 'score_d', 'rank_numeric']
            vals = [meet_db_id, athlete_id, apparatus_id, gender_heuristic, score_numeric, d_numeric, rank_numeric]
            
            if bonus_numeric is not None:
                cols.append('bonus')
                vals.append(bonus_numeric)
            if exec_bonus_numeric is not None:
                cols.append('execution_bonus')
                vals.append(exec_bonus_numeric)
            
            # Add dynamic extra columns (Global metadata)
            for col_name, col_val in dynamic_values.items():
                cols.append(col_name)
                vals.append(col_val)
                
            placeholders = ', '.join(['?'] * len(cols))
            quoted_cols = [f'"{c}"' for c in cols]
            col_str = ', '.join(quoted_cols)
            
            sql = f"INSERT INTO Results ({col_str}) VALUES ({placeholders})"
            cursor.execute(sql, vals)
            results_inserted += 1

    conn.commit()
    print(f"  Processed {athletes_processed} athletes, inserting {results_inserted} records (Dynamic Schema).")


# ==============================================================================
#  MAIN EXECUTION
# ==============================================================================
def main():
    """
    Main execution block for the Kscore data loader.
    """
    # ensure database exists (but don't wipe it!)
    if not os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} not found. Please run create_db.py first.")
        return

    club_aliases = load_club_aliases()
    meet_manifest = load_meet_manifest(KSCORE_MEET_MANIFEST_FILE)
    
    process_kscore_files(meet_manifest, club_aliases)
    
    print("\n--- Kscore data loading script finished ---")

if __name__ == "__main__":
    main()