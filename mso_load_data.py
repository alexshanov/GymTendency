import sqlite3
import csv
import os
import glob
import traceback
import json
import re
import argparse
import gc  # Explicit garbage collection

# --- Import shared functions ---
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
    mark_file_processed,
    sanitize_column_name,
    ensure_column_exists,
    check_duplicate_result,
    validate_score,
    standardize_score_status,
    parse_rank
)

# --- CONFIGURATION (Specific to MSO) ---
DB_FILE = "gym_data.db"
MSO_CSVS_DIR = "CSVs_mso_final" 
MSO_MANIFEST_FILE = "discovered_meet_ids_mso.csv"

# --- MSO Column Mappings ---
COLUMN_MAP = {
    'Gymnast': 'Name',
    'Team': 'Club',
    'Sess': 'Session',
    'Lvl': 'Level',
    'Div': 'Age_Group',
    'VT': 'Vault', 'VAULT': 'Vault',
    'UB': 'Uneven Bars', 'BARS': 'Uneven Bars', 'UNEVEN BARS': 'Uneven Bars',
    'BB': 'Beam', 'BEAM': 'Beam', 'Balance Beam': 'Beam',
    'FX': 'Floor', 'FLR': 'Floor', 'FLOOR': 'Floor',
    'AA': 'All Around', 'ALL AROUND': 'All Around',
    'PH': 'Pommel Horse', 'POMMEL HORSE': 'Pommel Horse', 'POMML': 'Pommel Horse',
    'SR': 'Rings', 'RINGS': 'Rings',
    'PB': 'Parallel Bars', 'PBARS': 'Parallel Bars', 'PARALLEL BARS': 'Parallel Bars',
    'HB': 'High Bar', 'HIBAR': 'High Bar', 'HIGH BAR': 'High Bar'
}

def load_meet_manifest(manifest_file):
    print(f"--- Loading MSO meet manifest from '{manifest_file}' ---")
    try:
        with open(manifest_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            manifest = {
                str(row['MeetID']): {
                    'name': row['MeetName'],
                    'start_date_iso': row['Date'], 
                    'location': row['State'],
                    'year': None
                }
                for row in reader
            }
        return manifest
    except (FileNotFoundError, KeyError) as e:
        print(f"Warning: Could not load MSO manifest. Error: {e}")
        return {} 

def parse_cell_value(cell_str):
    if not isinstance(cell_str, str) or not cell_str.strip():
        return None, None, None, None, None
        
    parts = cell_str.split()
    if not parts:
        return None, None, None, None, None
    
    score_final = None
    d_score = None
    rank_numeric = None
    rank_text = None
    
    def is_float(s):
        try:
            float(s)
            return True
        except ValueError:
            return False
            
    def _parse_rank_local(s):
        clean = re.sub(r'\D', '', s)
        return int(clean) if clean else None
        
    # --- LOGIC ---
    if len(parts) == 1:
        if is_float(parts[0]):
            return float(parts[0]), None, None, None, None
            
    if len(parts) == 2:
        p0, p1 = parts[0], parts[1]
        if is_float(p0) and is_float(p1):
            score_final = float(p1) + (float(p0) / 1000.0)
            return score_final, None, None, None, None
            
    if len(parts) == 3:
        rank_part = parts[0]
        frac_part = parts[1]
        int_part = parts[2]
        if is_float(frac_part) and is_float(int_part):
            score_final = float(int_part) + (float(frac_part) / 1000.0)
            rank_numeric = _parse_rank_local(rank_part)
            rank_text = rank_part
            return score_final, None, rank_numeric, rank_text, None

    if len(parts) == 4:
        rank_part = parts[0]
        frac_part = parts[1]
        int_part = parts[2]
        extra_part = parts[3]
        if is_float(frac_part) and is_float(int_part):
            score_final = float(int_part) + (float(frac_part) / 1000.0)
            rank_numeric = _parse_rank_local(rank_part)
            rank_text = rank_part
            bonus = float(extra_part) if is_float(extra_part) else None
            return score_final, None, rank_numeric, rank_text, bonus

    if is_float(parts[-1]):
        score_final = float(parts[-1])
        remaining = parts[:-1]
        if len(remaining) == 1:
             rank_text = remaining[0]
             rank_numeric = _parse_rank_local(remaining[0])
    
    return score_final, None, rank_numeric, rank_text, None

def process_mso_files(meet_manifest, club_alias_map, sample_rate=1):
    print(f"\n--- Starting to process MSO result files (Sample Rate: {sample_rate}) ---")
    search_pattern = os.path.join(MSO_CSVS_DIR, "*_mso.csv")
    csv_files = glob.glob(search_pattern)
    csv_files.sort()
    
    if sample_rate > 1:
        csv_files = csv_files[::sample_rate]
    if not csv_files:
        print(f"Warning: No result files found in '{MSO_CSVS_DIR}'.")
        return
        
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # Initial Caches
            person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
            club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
            apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
            meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}

            for i, filepath in enumerate(csv_files):
                filename = os.path.basename(filepath)
                file_hash = calculate_file_hash(filepath)
                
                if is_file_processed(conn, filepath, file_hash):
                    if i % 100 == 0: print(f"  Skipping: {filename} (Already processed)")
                    continue

                print(f"[{i}/{len(csv_files)}] Processing file: {filename}")
                success = parse_mso_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map)
                
                if success:
                    mark_file_processed(conn, filepath, file_hash)
                
                # Periodically clear caches if they get too large (every 500 files)
                if i > 0 and i % 500 == 0:
                    print("  -> Periodic cache refresh to manage memory...")
                    person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
                    club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
                    athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
                    gc.collect()

    except Exception as e:
        print(f"Critical error: {e}")
        traceback.print_exc()

def parse_mso_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map):
    filename = os.path.basename(filepath)
    source_meet_id = filename.split('_mso.csv')[0]
    
    rows = []
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return False

    if not rows: return True

    # Determine Meet Metadata
    meet_details = meet_manifest.get(source_meet_id, {})
    if not meet_details.get('name') and 'Meet' in rows[0]:
        meet_details['name'] = rows[0]['Meet']
        
    meet_db_id = get_or_create_meet(conn, 'mso', source_meet_id, meet_details, meet_cache)
    
    headers = list(rows[0].keys())
    
    def find_col_by_fuzzy(candidates):
        for c in headers:
            if c.upper().strip() in [cand.upper() for cand in candidates]:
                return c
        return None

    name_col = find_col_by_fuzzy(['Gymnast', 'Name'])
    club_col = find_col_by_fuzzy(['Team', 'Club'])
    
    if not name_col:
        print(f"  Skipping {filename}: No Name column identified.")
        return False

    apparatus_cols = []
    dynamic_metadata_cols = []
    known_apparatus_keys = ['Vault', 'Uneven Bars', 'Beam', 'Floor', 'All Around', 'Pommel Horse', 'Rings', 'Parallel Bars', 'High Bar']
    
    for col in headers:
        norm_key = COLUMN_MAP.get(col.strip(), col)
        if norm_key in known_apparatus_keys:
            apparatus_cols.append(col)
        elif col not in [name_col, club_col]:
             dynamic_metadata_cols.append(col)

    detected_apparatus_names = [COLUMN_MAP.get(c.strip(), c) for c in apparatus_cols]
    if any(x in ['Pommel Horse', 'Rings', 'Parallel Bars', 'High Bar'] for x in detected_apparatus_names):
        discipline_id = 2 # MAG
        gender_heuristic = 'M'
    else:
        discipline_id = 1 # WAG
        gender_heuristic = 'F'

    cursor = conn.cursor()
    dynamic_mapping = []
    for raw_col in dynamic_metadata_cols:
        safe_name = sanitize_column_name(raw_col)
        ensure_column_exists(cursor, 'Results', safe_name, 'TEXT')
        dynamic_mapping.append((raw_col, safe_name))

    ensure_column_exists(cursor, 'Results', 'bonus', 'REAL')
    ensure_column_exists(cursor, 'Results', 'execution_bonus', 'REAL')

    results_inserted = 0
    for row in rows:
        raw_name = row.get(name_col)
        person_name = standardize_athlete_name(raw_name)
        if not person_name: continue
        
        person_id = get_or_create_person(conn, person_name, gender_heuristic, person_cache)
        raw_club = row.get(club_col) if club_col else ""
        club_name = standardize_club_name(raw_club, club_alias_map)
        club_id = get_or_create_club(conn, club_name, club_cache)
        athlete_id = get_or_create_athlete_link(conn, person_id, club_id, athlete_cache)
        
        dynamic_vals = {}
        for r_col, s_col in dynamic_mapping:
            val = row.get(r_col)
            if val: dynamic_vals[s_col] = str(val)
            
        for raw_app_col in apparatus_cols:
            cell_value = row.get(raw_app_col)
            if not cell_value: continue
            
            clean_app_name = COLUMN_MAP.get(raw_app_col.strip(), raw_app_col)
            app_key = (clean_app_name, discipline_id)
            if app_key not in apparatus_cache: app_key = (clean_app_name, 99)
            if app_key not in apparatus_cache: continue
            
            apparatus_id = apparatus_cache[app_key]
            mso_vals = parse_cell_value(cell_value)
            score_final, d_score, rank_numeric_mso, rank_text, bonus = mso_vals
            
            rank_numeric = rank_numeric_mso if rank_numeric_mso is not None else parse_rank(rank_text)
            if score_final is None and d_score is None: continue
            if check_duplicate_result(conn, meet_db_id, athlete_id, apparatus_id): continue
            
            cols = ['meet_db_id', 'athlete_id', 'apparatus_id', 'gender', 'score_final', 'score_d', 'rank_numeric', 'rank_text', 'score_text']
            vals = [int(meet_db_id), int(athlete_id), int(apparatus_id), gender_heuristic, score_final, d_score, rank_numeric, rank_text, str(cell_value)]
            
            if bonus is not None:
                cols.append('bonus')
                vals.append(bonus)
            for col_name, col_val in dynamic_vals.items():
                cols.append(col_name)
                vals.append(col_val)
                
            placeholders = ', '.join(['?'] * len(cols))
            col_str = ', '.join([f'"{c}"' for c in cols])
            sql = f"INSERT INTO Results ({col_str}) VALUES ({placeholders})"
            cursor.execute(sql, vals)
            results_inserted += 1
            
    conn.commit()
    # Explicitly clear the row list to free memory sooner
    rows.clear()
    return True

def main():
    if not os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} not found.")
        return

    parser = argparse.ArgumentParser(description="Load MSO data into the database.")
    parser.add_argument("--sample", type=int, default=1, help="Process every Nth file (e.g. 10)")
    parser.add_argument("--file", type=str, help="Process a single specific file")
    args = parser.parse_args()

    club_aliases = load_club_aliases()
    meet_manifest = load_meet_manifest(MSO_MANIFEST_FILE)
    
    if args.file:
        with sqlite3.connect(DB_FILE) as conn:
            person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
            club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
            apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
            meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}
            parse_mso_file(args.file, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_aliases)
    else:
        process_mso_files(meet_manifest, club_aliases, sample_rate=args.sample)
        
    print("\n--- MSO data loading finished ---")

if __name__ == "__main__":
    main()
