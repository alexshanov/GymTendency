# livemeet_load_data.py

import sqlite3
import pandas as pd
import os
import glob
import traceback
import json
import re
import argparse

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
    get_or_create_meet,
    calculate_file_hash,
    is_file_processed,
    mark_file_processed
)

# --- CONFIGURATION (Specific to Livemeet) ---
DB_FILE = "gym_data.db"
LIVEMEET_CSVS_DIR = "CSVs_Livemeet_final" 
MEET_MANIFEST_FILE = "discovered_meet_ids_livemeet.csv" 

# ==============================================================================
#  DATA LOADING FUNCTIONS (Specific to Livemeet)
# ==============================================================================

def load_meet_manifest(manifest_file):
    """
    Reads the Livemeet manifest CSV into a dictionary for easy lookups.
    """
    print(f"--- Loading Livemeet manifest from '{manifest_file}' ---")
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
        print(f"Successfully loaded details for {len(manifest)} Livemeet meets into memory.")
        return manifest
    except (FileNotFoundError, KeyError) as e:
        print(f"Warning: Could not load Livemeet manifest. Meet details will be incomplete. Error: {e}")
        return {} 

def process_livemeet_files(meet_manifest, club_alias_map, sample_rate=1):
    """
    Main function to find and process all Livemeet result CSV files.
    """
    print(f"\n--- Starting to process Livemeet result files (Sample Rate: {sample_rate}) ---")
    
    search_pattern = os.path.join(LIVEMEET_CSVS_DIR, "*_FINAL_*.csv")
    csv_files = glob.glob(search_pattern)
    csv_files.sort()
    
    if sample_rate > 1:
        csv_files = csv_files[::sample_rate]

    if not csv_files:
        print(f"Warning: No result files found in '{LIVEMEET_CSVS_DIR}'.")
        return
        
    try:
        with sqlite3.connect(DB_FILE, timeout=60) as conn:
            # --- Caches now map to the new schema ---
            person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
            club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
            apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
            meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}

            for filepath in csv_files:
                filename = os.path.basename(filepath)
                file_hash = calculate_file_hash(filepath)
                
                if is_file_processed(conn, filepath, file_hash):
                    print(f"  Skipping: {filename} (Already processed and unchanged)")
                    continue

                print(f"\nProcessing file: {filename}")
                # Pass all the new caches to the parsing function
                success = parse_livemeet_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map)
                
                if success:
                    mark_file_processed(conn, filepath, file_hash)

    except Exception as e:
        print(f"A critical error occurred during file processing: {e}")
        traceback.print_exc()

def parse_livemeet_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map):
    """
    Parses a single Livemeet CSV and loads its data into the database using the new schema.
    """
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty or 'Name' not in df.columns:
            print("File is empty or missing 'Name' column. Skipping.")
            return True
    except Exception as e:
        print(f"Warning: Could not read CSV file '{filepath}'. Error: {e}")
        return False

    source_meet_id = os.path.basename(filepath).split('_FINAL_')[0]
    meet_details = meet_manifest.get(source_meet_id, {})
    if not meet_details.get('name'):
        meet_details['name'] = df['Meet'].iloc[0] if 'Meet' in df.columns and not df.empty else f"Livemeet {source_meet_id}"
    meet_db_id = get_or_create_meet(conn, 'livemeet', source_meet_id, meet_details, meet_cache)

    discipline_id, discipline_name, gender_heuristic = detect_discipline(df)
    print(f"  Detected Discipline: {discipline_name}")

    # --- KEY MAPPING FOR IDS ---
    KEY_MAP = {
        'Name': 'Name', 'Athlete': 'Name',
        'Club': 'Club', 'Team': 'Club',
        'Level': 'Level',
        'Age': 'Age',
        'Prov': 'Prov',
        'Meet': 'Meet',
        'Group': 'Group',
        'Age_Group': 'Age_Group'
    }

    # --- 1. Detect and Normalize Raw Headers (Sportzsoft/LiveMeet messy format) ---
    # Some files have repeated apparatus headers: Vault, Vault, Vault ...
    # These are triplets of (D, Score, Rank), often repeated 3 times.
    raw_apps = ['Vault', 'Uneven_Bars', 'Beam', 'Floor', 'Pommel_Horse', 'Rings', 'Parallel_Bars', 'High_Bar', 'AllAround']
    new_headers = []
    seen_counts = {}
    for col in df.columns:
        base = col.split('.')[0]
        if base in raw_apps:
            count = seen_counts.get(base, 0)
            seen_counts[base] = count + 1
            # triplet_pos: 0=D, 1=Score, 2=Rnk
            # triplet_num: 0, 1, 2 (Sportzsoft often exports 3 identical triplets)
            triplet_pos = count % 3
            triplet_num = count // 3
            suffix = ['D', 'Score', 'Rnk'][triplet_pos]
            if triplet_num == 0:
                proposed_name = f"Result_{base}_{suffix}"
            else:
                proposed_name = f"EXTRA_{base}_{triplet_num}_{suffix}"
            
            # CRITICAL FIX: Avoid creating duplicate columns if the CSV already has explicit Result_... columns.
            # E.g. If 'Result_AllAround_D' exists, don't rename 'AllAround' to 'Result_AllAround_D'.
            if proposed_name in df.columns:
                new_headers.append(col) # Keep original name (e.g. 'AllAround')
            else:
                new_headers.append(proposed_name)
        else:
            new_headers.append(col)
    df.columns = new_headers

    col_map = {col: KEY_MAP.get(col, col) for col in df.columns}
    name_col = next((c for c, v in col_map.items() if v == 'Name'), None)
    
    if not name_col:
        print(f"Skipping file: No Name column identified (checked Athlete, Gymnast, Name).")
        return

    from etl_functions import sanitize_column_name, ensure_column_exists, parse_rank
    from etl_functions import check_duplicate_result, validate_score, standardize_score_status

    cursor = conn.cursor()
    athletes_processed = 0
    results_inserted = 0

    # Identify apparatus triplets
    result_columns = [col for col in df.columns if col.startswith('Result_')]
    event_bases = {}
    for col in result_columns:
        match = re.search(r'Result_(.*)_(Score|D|E|Rnk|Total)$', col)
        if match:
            raw_event_name = match.group(1)
            event_bases[raw_event_name] = raw_event_name

    # Identify Dynamic Columns
    ignore_cols = list(event_bases.keys()) + result_columns + [name_col]
    if 'Club' in df.columns: ignore_cols.append('Club')
    
    dynamic_cols_to_add = []
    for col in df.columns:
        if col not in ignore_cols and not col.startswith('Result_'):
            sanitized = sanitize_column_name(col)
            if ensure_column_exists(cursor, 'Results', sanitized, 'TEXT'):
                dynamic_cols_to_add.append((col, sanitized))
            else:
                # If column was rejected (whitelist), we skip loading it to avoid SQL errors
                pass
            
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
            if val: 
                dynamic_values[safe_col] = str(val)
                # Unify Group into Session for database consistency
                if safe_col == 'group' and 'session' not in dynamic_values:
                    dynamic_values['session'] = str(val)

        # 3. Process Apparatus (Pivot)
        for raw_event, _ in event_bases.items():
            clean_name = raw_event.replace('_', ' ')
            if clean_name == "Balance Beam": clean_name = "Beam" # Small normalization for matching ID
            if clean_name == "AllAround": clean_name = "All Around"
            
            app_key = (clean_name, discipline_id)
            if app_key not in apparatus_cache:
                 app_key = (raw_event, discipline_id)
            if app_key not in apparatus_cache: app_key = (clean_name, 99) 
            
            if app_key not in apparatus_cache: 
                continue
            
            apparatus_id = apparatus_cache[app_key]
            
            # Extract Detailed Columns
            d_val = row.get(f'Result_{raw_event}_D')
            sv_val = row.get(f'Result_{raw_event}_SV')
            score_val = row.get(f'Result_{raw_event}_Score')
            e_val = row.get(f'Result_{raw_event}_E')
            bonus_val = row.get(f'Result_{raw_event}_Bonus')
            penalty_val = row.get(f'Result_{raw_event}_Penalty')
            rank_val = row.get(f'Result_{raw_event}_Rnk')
            
            # Legacy/Fallback execution bonus
            exec_bonus_val = row.get(f'Result_{raw_event}_Exec_Bonus') or row.get(f'Result_{raw_event}_Execution_Bonus')
            
            if not score_val and not d_val and not sv_val: continue
            
            # Numeric conversion
            def to_float(val):
                n = pd.to_numeric(val, errors='coerce')
                return float(n) if not pd.isna(n) else None

            score_numeric = to_float(score_val)
            d_numeric = to_float(d_val)
            sv_numeric = to_float(sv_val)
            e_numeric = to_float(e_val)
            bonus_numeric = to_float(bonus_val)
            penalty_numeric = to_float(penalty_val)
            exec_bonus_numeric = to_float(exec_bonus_val)
            rank_numeric = parse_rank(str(rank_val)) if rank_val else None
            
            # Check Session-Aware Uniqueness
            current_session = dynamic_values.get('session') or dynamic_values.get('group')
            current_level = row.get('Level')
            
            if check_duplicate_result(conn, meet_db_id, athlete_id, apparatus_id, session=current_session, level=current_level): 
                continue
            
            # Dynamic INSERT Construction
            cols = ['meet_db_id', 'athlete_id', 'apparatus_id', 'gender', 'score_final', 'score_d', 'score_sv', 'score_e', 'penalty', 'rank_numeric', 'score_text', 'rank_text']
            vals = [int(meet_db_id), int(athlete_id), int(apparatus_id), gender_heuristic, score_numeric, d_numeric, sv_numeric, e_numeric, penalty_numeric, rank_numeric, str(score_val) if score_val else None, str(rank_val) if rank_val else None]
            
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
            # Quote all column names
            quoted_cols = [f'"{c}"' for c in cols]
            col_str = ', '.join(quoted_cols)
            
            sql = f"INSERT INTO Results ({col_str}) VALUES ({placeholders})"
            cursor.execute(sql, vals)
            results_inserted += 1
            
    conn.commit()
    print(f"  Processed {athletes_processed} athletes, inserting {results_inserted} records (Dynamic Schema).")
    return True


# ==============================================================================
#  MAIN EXECUTION
# ==============================================================================
def main():
    """
    Main execution block for the Livemeet data loader.
    """
    if not os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} not found. Please run create_db.py first.")
        return

    parser = argparse.ArgumentParser(description="Load Livemeet data into the database.")
    parser.add_argument("--sample", type=int, default=1, help="Process every Nth file (e.g. 10)")
    parser.add_argument("--file", type=str, help="Process a single specific file")
    args = parser.parse_args()

    club_aliases = load_club_aliases()
    meet_manifest = load_meet_manifest(MEET_MANIFEST_FILE)
    
    if args.file:
        with sqlite3.connect(DB_FILE) as conn:
            person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
            club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
            apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
            meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}
            parse_livemeet_file(args.file, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_aliases)
    else:
        process_livemeet_files(meet_manifest, club_aliases, sample_rate=args.sample)
    
    print("\n--- Livemeet data loading script finished ---")

if __name__ == "__main__":
    main()