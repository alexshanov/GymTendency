
import sqlite3
import pandas as pd
import os
import glob
import traceback
import json
import re

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
    get_or_create_meet
)

# --- CONFIGURATION (Specific to MSO) ---
DB_FILE = "gym_data.db"
MSO_CSVS_DIR = "CSVs_mso_final" 
MSO_MANIFEST_FILE = "discovered_meet_ids_mso.csv"

# --- MSO Column Mappings ---
# Maps raw CSV headers to internal/standard names
COLUMN_MAP = {
    # Service Columns
    'Gymnast': 'Name',
    'Team': 'Club',
    'Sess': 'Session',
    'Lvl': 'Level',
    'Div': 'Age_Group',
    
    # Apparatus - WAG
    'VT': 'Vault', 'VAULT': 'Vault',
    'UB': 'Uneven Bars', 'BARS': 'Uneven Bars', 'UNEVEN BARS': 'Uneven Bars',
    'BB': 'Beam', 'BEAM': 'Beam', 'Balance Beam': 'Beam',
    'FX': 'Floor', 'FLR': 'Floor', 'FLOOR': 'Floor',
    'AA': 'AllAround', 'ALL AROUND': 'AllAround',
    
    # Apparatus - MAG
    'PH': 'Pommel Horse', 'POMMEL HORSE': 'Pommel Horse', 'POMML': 'Pommel Horse',
    'SR': 'Rings', 'RINGS': 'Rings',
    'PB': 'Parallel Bars', 'PBARS': 'Parallel Bars', 'PARALLEL BARS': 'Parallel Bars',
    'HB': 'High Bar', 'HIBAR': 'High Bar', 'HIGH BAR': 'High Bar'
}

def load_meet_manifest(manifest_file):
    print(f"--- Loading MSO meet manifest from '{manifest_file}' ---")
    try:
        manifest_df = pd.read_csv(manifest_file)
        manifest = {
            str(row['MeetID']): {
                'name': row['MeetName'],
                'start_date_iso': row['Date'], 
                'location': row['State'],
                'year': None
            }
            for _, row in manifest_df.iterrows()
        }
        return manifest
    except (FileNotFoundError, KeyError) as e:
        print(f"Warning: Could not load MSO manifest. Error: {e}")
        return {} 

def parse_cell_value(cell_str):
    """
    Parses a raw cell string from MSO into (score, d_score, rank).
    Typical inputs:
    - "9.500"           -> Score: 9.5
    - "1 9.500"         -> Rank: 1, Score: 9.5
    - "T2 9.500"        -> Rank: 2, Score: 9.5 (Rank Text: T2)
    - "1 3.5 9.500"     -> Rank: 1, D: 3.5, Score: 9.5
    
    Heuristics:
    - Last token is almost always the FINAL SCORE (float).
    - First token (if integer-like) is RANK.
    - If 3 tokens, Middle is D-SCORE.
    """
    if not isinstance(cell_str, str) or not cell_str.strip():
        return None, None, None, None
        
    parts = cell_str.split()
    if not parts:
        return None, None, None, None
    
    score_final = None
    d_score = None
    rank_numeric = None
    rank_text = None
    
    # Helper to check if string is float
    def is_float(s):
        try:
            float(s)
            return True
        except ValueError:
            return False
            
    # Helper to extract integer rank
    def parse_rank(s):
        clean = re.sub(r'\D', '', s)
        return int(clean) if clean else None

    # Logic based on token count
    # Default assumption: Last token is Score
    if is_float(parts[-1]):
        score_final = float(parts[-1])
        remaining = parts[:-1]
    else:
        # Sometimes score might be missing or weird text
        return None, None, None, None # Skip weird cells for now
        
    if not remaining:
        # Case: "9.500"
        pass
    
    elif len(remaining) == 1:
        # Case: "1 9.500" or "10.0 9.500" (?)
        # Is the first part Rank or D-score?
        # Rank is typically an integer or "T2". D-score is float.
        token = remaining[0]
        if is_float(token) and '.' in token:
            d_score = float(token) # Likely D-score if it has decimal
        else:
            rank_text = token
            rank_numeric = parse_rank(token)
            
    elif len(remaining) == 2:
        # Case: "1 4.0 9.500" -> Rank, D, Score
        rank_token = remaining[0]
        d_token = remaining[1]
        
        rank_text = rank_token
        rank_numeric = parse_rank(rank_token)
        
        if is_float(d_token):
            d_score = float(d_token)
            
    return score_final, d_score, rank_numeric, rank_text

def process_mso_files(meet_manifest, club_alias_map):
    print("\n--- Starting to process MSO result files ---")
    csv_files = glob.glob(os.path.join(MSO_CSVS_DIR, "*_mso.csv"))
    if not csv_files:
        print(f"Warning: No result files found in '{MSO_CSVS_DIR}'.")
        return
        
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # Caches
            person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
            club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
            apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
            meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}

            for filepath in csv_files:
                parse_mso_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map)

    except Exception as e:
        print(f"Critical error: {e}")
        traceback.print_exc()

def parse_mso_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map):
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty: return
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return

    filename = os.path.basename(filepath)
    source_meet_id = filename.split('_mso.csv')[0]
    
    # Determine Meet Metadata
    meet_details = meet_manifest.get(source_meet_id, {})
    if not meet_details.get('name') and 'Meet' in df.columns:
        meet_details['name'] = df['Meet'].iloc[0]
        
    meet_db_id = get_or_create_meet(conn, 'mso', source_meet_id, meet_details, meet_cache)
    
    # Identify Columns
    # Map raw headers to internal keys
    # Keys found in this CSV
    found_map = {} # raw_col -> internal_key
    apparatus_cols = []
    
    for col in df.columns:
        norm_col = col.strip()
        if norm_col in COLUMN_MAP:
            internal = COLUMN_MAP[norm_col]
            found_map[col] = internal
            # Identify apparatus columns (those in map that are strictly apparatus)
            if internal in ['Vault', 'Uneven Bars', 'Beam', 'Floor', 'AllAround', 'Pommel Horse', 'Rings', 'Parallel Bars', 'High Bar']:
                apparatus_cols.append(col)
        # Check fallback for Name/Club if exact match failed
        elif 'gymnast' in norm_col.lower() or 'name' in norm_col.lower():
            found_map[col] = 'Name'
        elif 'team' in norm_col.lower() or 'club' in norm_col.lower():
            found_map[col] = 'Club'
    
    # Determine Discipline
    # If we found MAG-specific apparatus, it's MAG.
    detected_apparatus_names = [COLUMN_MAP.get(c.strip(), c) for c in apparatus_cols]
    if any(x in ['Pommel Horse', 'Rings', 'Parallel Bars', 'High Bar'] for x in detected_apparatus_names):
        discipline_id = 2 # MAG
        gender_heuristic = 'M'
        print(f"  {filename}: Detected MAG")
    else:
        discipline_id = 1 # WAG
        gender_heuristic = 'F'
        print(f"  {filename}: Detected WAG")

    # Find core columns in this specific file
    name_col = next((k for k, v in found_map.items() if v == 'Name'), None)
    club_col = next((k for k, v in found_map.items() if v == 'Club'), None)
    level_col = next((k for k, v in found_map.items() if v == 'Level'), None)
    
    if not name_col:
        print(f"  Skipping {filename}: No Name column identified.")
        return

    cursor = conn.cursor()
    results_inserted = 0
    
    for index, row in df.iterrows():
        # 1. Athlete & Club
        raw_name = row.get(name_col)
        person_name = standardize_athlete_name(raw_name)
        if not person_name: continue
        
        person_id = get_or_create_person(conn, person_name, gender_heuristic, person_cache)
        
        raw_club = row.get(club_col) if club_col else ""
        club_name = standardize_club_name(raw_club, club_alias_map)
        club_id = get_or_create_club(conn, club_name, club_cache)
        
        athlete_id = get_or_create_athlete_link(conn, person_id, club_id, athlete_cache)
        
        # 2. Metadata
        level_val = row.get(level_col) if level_col else None
        
        # Build Details JSON from other known columns
        details_dict = {}
        for col, internal_key in found_map.items():
            if internal_key in ['Session', 'Age_Group'] and row.get(col):
                details_dict[internal_key] = row.get(col)
        
        # Also grab any unmapped columns that look useful? (Skip for now to keep clean)
        details_json = json.dumps(details_dict)
        
        # 3. Process Apparatus Results
        for raw_app_col in apparatus_cols:
            cell_value = row.get(raw_app_col)
            if not cell_value: continue
            
            clean_app_name = COLUMN_MAP.get(raw_app_col.strip())
            
            # Get apparatus ID
            app_key = (clean_app_name, discipline_id)
            if app_key not in apparatus_cache:
                app_key = (clean_app_name, 99) # Fallback / Shared
            
            if app_key not in apparatus_cache:
                continue
                
            apparatus_id = apparatus_cache[app_key]
            
            # Parse Score
            score, d_score, rank_num, rank_txt = parse_cell_value(cell_value)
            
            if score is None and rank_num is None: # Empty or invalid
                continue
                
            # Deduplicate
            from etl_functions import check_duplicate_result, validate_score, standardize_score_status
            if check_duplicate_result(conn, meet_db_id, athlete_id, apparatus_id):
                continue
                
            # Validate
            is_valid, warning = validate_score(score, d_score, clean_app_name)
            if warning: print(f"    Warning ({person_name} - {clean_app_name}): {warning}")
            
            cursor.execute("""
                INSERT INTO Results (
                    meet_db_id, athlete_id, apparatus_id, gender,
                    level, score_d, score_final,
                    rank_numeric, rank_text, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                meet_db_id, athlete_id, apparatus_id, gender_heuristic,
                level_val, d_score, score,
                rank_num, rank_txt, details_json
            ))
            results_inserted += 1
            
    conn.commit()
    print(f"  Inserted {results_inserted} results from {filename}")

def main():
    if not os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} not found.")
        return
    club_aliases = load_club_aliases()
    meet_manifest = load_meet_manifest(MSO_MANIFEST_FILE)
    process_mso_files(meet_manifest, club_aliases)
    print("\n--- MSO data loading finished ---")

if __name__ == "__main__":
    main()
