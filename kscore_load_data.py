import sqlite3
import pandas as pd
import os
import glob
import traceback
import json
import re

# --- CONFIGURATION ---
DB_FILE = "gym_data.db"
# This script will ONLY process CSVs from the kscore scraper's output directory
KSCORE_CSVS_DIR = "CSVs_final_kscore" 
KSCORE_MEET_MANIFEST_FILE = "discovered_meet_ids_kscore.csv"

# ==============================================================================
#  1. DATABASE SETUP
# ==============================================================================
def setup_database():
    """
    Ensures all necessary tables exist. This function is idempotent and safe to run
    even if the database has already been created by the livemeet loader.
    """
    print("--- Verifying database schema and definitions ---")
    
    # The schema is identical to the livemeet loader, ensuring compatibility.
    schema_queries = [
        "CREATE TABLE IF NOT EXISTS Disciplines (discipline_id INTEGER PRIMARY KEY, discipline_name TEXT NOT NULL UNIQUE);",
        "CREATE TABLE IF NOT EXISTS Apparatus (apparatus_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, discipline_id INTEGER NOT NULL, sort_order INTEGER, FOREIGN KEY (discipline_id) REFERENCES Disciplines (discipline_id), UNIQUE(name, discipline_id));",
        "CREATE TABLE IF NOT EXISTS Athletes (athlete_id INTEGER PRIMARY KEY AUTOINCREMENT, full_name TEXT NOT NULL, club TEXT, gender TEXT, UNIQUE(full_name, club));",
        "CREATE TABLE IF NOT EXISTS Meets (meet_db_id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, source_meet_id TEXT NOT NULL, name TEXT, start_date_iso TEXT, location TEXT, year INTEGER, UNIQUE(source, source_meet_id));",
        "CREATE TABLE IF NOT EXISTS Results (result_id INTEGER PRIMARY KEY AUTOINCREMENT, meet_db_id INTEGER NOT NULL, athlete_id INTEGER NOT NULL, apparatus_id INTEGER NOT NULL, score_d REAL, score_final REAL, score_text TEXT, rank_numeric INTEGER, rank_text TEXT, details_json TEXT, FOREIGN KEY (meet_db_id) REFERENCES Meets (meet_db_id), FOREIGN KEY (athlete_id) REFERENCES Athletes (athlete_id), FOREIGN KEY (apparatus_id) REFERENCES Apparatus (apparatus_id));"
    ]

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            print("Ensuring tables exist...")
            for query in schema_queries:
                cursor.execute(query)
            
            # --- Populate Definition Tables (INSERT OR IGNORE is idempotent) ---
            disciplines = [(1, 'WAG'), (2, 'MAG'), (99, 'Other')]
            cursor.executemany("INSERT OR IGNORE INTO Disciplines (discipline_id, discipline_name) VALUES (?, ?)", disciplines)

            WAG_EVENTS = {'Vault': 1, 'Uneven Bars': 2, 'Beam': 3, 'Floor': 4}
            MAG_EVENTS = {'Floor': 1, 'Pommel Horse': 2, 'Rings': 3, 'Vault': 4, 'Parallel Bars': 5, 'High Bar': 6}
            OTHER_EVENTS = {'AllAround': 99, 'All-Around': 99, 'Physical Preparation': 100}

            all_apparatus = []
            for name, order in WAG_EVENTS.items(): all_apparatus.append((name, 1, order))
            for name, order in MAG_EVENTS.items(): all_apparatus.append((name, 2, order))
            for name, order in OTHER_EVENTS.items(): all_apparatus.append((name, 99, order))
            
            cursor.executemany("INSERT OR IGNORE INTO Apparatus (name, discipline_id, sort_order) VALUES (?, ?, ?)", all_apparatus)
            
            conn.commit()
        print("Database schema verified.")
        return True
    except Exception as e:
        print(f"Error during database setup: {e}")
        traceback.print_exc()
        return False

# ==============================================================================
#  2. DATA LOADING FUNCTIONS (Reused and Adapted for Kscore)
# ==============================================================================

def load_meet_manifest(manifest_file):
    """
    Reads the meet manifest CSV into a dictionary for easy lookups.
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
        print(f"Warning: Could not load Kscore manifest file. Meet details will be incomplete. Error: {e}")
        return {} 

def get_or_create_meet(conn, source, source_meet_id, meet_details, cache):
    meet_key = (source, source_meet_id)
    if meet_key in cache:
        return cache[meet_key]
    cursor = conn.cursor()
    cursor.execute("SELECT meet_db_id FROM Meets WHERE source = ? AND source_meet_id = ?", meet_key)
    result = cursor.fetchone()
    if result:
        meet_db_id = result[0]
    else:
        cursor.execute("INSERT INTO Meets (source, source_meet_id, name, start_date_iso, location, year) VALUES (?, ?, ?, ?, ?, ?)",
            (source, source_meet_id, meet_details.get('name'), meet_details.get('start_date_iso'), meet_details.get('location'), meet_details.get('year')))
        meet_db_id = cursor.lastrowid
        print(f"  -> New meet added to DB: '{meet_details.get('name')}' (Source: {source}, ID: {meet_db_id})")
    cache[meet_key] = meet_db_id
    return meet_db_id

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
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, full_name, club FROM Athletes").fetchall()}
            apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
            meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}

            for filepath in csv_files:
                print(f"\nProcessing file: {os.path.basename(filepath)}")
                # Pass the club_alias_map to the parsing function
                parse_kscore_file(filepath, conn, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map)

    except Exception as e:
        print(f"A critical error occurred during file processing: {e}")
        traceback.print_exc()

def parse_kscore_file(filepath, conn, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map):
    """
    Parses a single Kscore CSV and loads its data into the database.
    """
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty or 'Name' not in df.columns:
            print("File is empty or missing 'Name' column. Skipping.")
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

    core_column = 'Name'
    result_columns = [col for col in df.columns if col.startswith('Result_')]
    dynamic_columns = [col for col in df.columns if col != core_column and not col.startswith('Result_')]
    
    event_bases = {}
    for col in result_columns:
        match = re.search(r'Result_(.*)_(Score|D)$', col)
        if match:
            raw_event_name = match.group(1).replace('_', ' ')
            if raw_event_name == "All Around": raw_event_name = "AllAround"
            if raw_event_name not in event_bases: event_bases[raw_event_name] = raw_event_name.replace(' ', '_')

    athletes_processed = 0
    results_inserted = 0
    cursor = conn.cursor()
    for index, row in df.iterrows():
        if not row.get(core_column): continue
        athletes_processed += 1
        
        # Pass the club_alias_map to the get_or_create_athlete function
        athlete_id = get_or_create_athlete(conn, row, gender_heuristic, athlete_cache, club_alias_map)
        if athlete_id is None:
            print(f"Warning: Skipping row {index+2} due to invalid or empty athlete name.")
            continue

        details_dict = {col: val for col, val in row[dynamic_columns].items() if val}
        details_json = json.dumps(details_dict)
        for clean_name, raw_name in event_bases.items():
            apparatus_key = (clean_name, discipline_id)
            if apparatus_key not in apparatus_cache: apparatus_key = (clean_name, 99) 
            if apparatus_key not in apparatus_cache: continue
            apparatus_id = apparatus_cache[apparatus_key]
            d_col, score_col, rank_col = f'Result_{raw_name}_D', f'Result_{raw_name}_Score', f'Result_{raw_name}_Rnk'
            d_val, score_val, rank_val = row.get(d_col), row.get(score_col), row.get(rank_col)
            score_numeric = pd.to_numeric(score_val, errors='coerce')
            score_text = None if pd.notna(score_numeric) else (str(score_val) if score_val else None)
            d_numeric = pd.to_numeric(d_val, errors='coerce')
            rank_numeric, rank_text = None, None
            if rank_val:
                temp_rank_num = pd.to_numeric(rank_val, errors='coerce')
                if pd.notna(temp_rank_num): rank_numeric = int(temp_rank_num)
                else:
                    rank_text = str(rank_val)
                    cleaned_rank_str = re.sub(r'\D', '', str(rank_val))
                    if cleaned_rank_str: rank_numeric = int(cleaned_rank_str)
            cursor.execute("INSERT INTO Results (meet_db_id, athlete_id, apparatus_id, score_d, score_final, score_text, rank_numeric, rank_text, details_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (meet_db_id, athlete_id, apparatus_id, d_numeric, score_numeric, score_text, rank_numeric, rank_text, details_json))
            results_inserted += 1
    conn.commit()
    print(f"  Processed {athletes_processed} athletes, inserting {results_inserted} result records.")
    
# --- Helper functions (Unchanged) ---
def detect_discipline(df):
    column_names = set(df.columns); MAG_INDICATORS = {'Pommel_Horse', 'Rings', 'Parallel_Bars', 'High_Bar'}; WAG_INDICATORS = {'Uneven_Bars', 'Beam'}
    for col in column_names:
        if any(indicator in col for indicator in MAG_INDICATORS): return 2, 'MAG', 'M'
        if any(indicator in col for indicator in WAG_INDICATORS): return 1, 'WAG', 'F'
    return 99, 'Other', 'Unknown'

def get_or_create_athlete(conn, row, gender_heuristic, athlete_cache, club_alias_map):
    """
    Finds an athlete in the cache/DB or creates a new one, ensuring both
    the name and club are standardized before any action.
    """
    cursor = conn.cursor()
    
    # --- Name Standardization (already in place) ---
    raw_name = row.get('Name')
    name = standardize_athlete_name(raw_name)
    if not name:
        return None

    # --- NEW: Club Standardization ---
    raw_club = row.get('Club')
    # Call our new function to apply aliases
    club = standardize_club_name(raw_club, club_alias_map)
    # --- END OF NEW PART ---

    # The rest of the function now uses the fully standardized 'name' and 'club'
    athlete_key = (name, club)

    if athlete_key in athlete_cache: 
        return athlete_cache[athlete_key]
    
    if club is None: 
        cursor.execute("SELECT athlete_id FROM Athletes WHERE full_name = ? AND club IS NULL", (name,))
    else: 
        cursor.execute("SELECT athlete_id FROM Athletes WHERE full_name = ? AND club = ?", (name, club))
    
    result = cursor.fetchone()
    if result: 
        athlete_id = result[0]
    else: 
        cursor.execute("INSERT INTO Athletes (full_name, club, gender) VALUES (?, ?, ?)", (name, club, gender_heuristic))
        athlete_id = cursor.lastrowid
        
    athlete_cache[athlete_key] = athlete_id
    return athlete_id

def standardize_athlete_name(name_str):
    """
    Takes a raw athlete name string and standardizes it to 'First Last' format.
    This version is more careful with capitalization to preserve existing formats.
    It primarily targets 'LAST, First' and 'LAST First' patterns.
    """
    if not isinstance(name_str, str) or not name_str.strip():
        return None

    name_str = name_str.strip()

    # Case 1: Handle "LAST, First" format
    if ',' in name_str:
        parts = [p.strip() for p in name_str.split(',', 1)]
        if len(parts) == 2:
            # Reorder to "First Last" and apply title case to each part
            first_name = parts[1].title()
            last_name = parts[0].title()
            return f"{first_name} {last_name}"

    # Case 2: Handle "LAST First" (all caps last name, followed by mixed case first name)
    words = name_str.split()
    if len(words) > 1 and words[0].isupper() and all(c.isupper() for c in words[0]) and any(c.islower() for c in ' '.join(words[1:])):
        last_name = words[0].title()
        first_names = " ".join(words[1:])
        return f"{first_names} {last_name}"

    # Default Case: Trust the existing format but clean up whitespace.
    # We avoid .title() here to preserve names like 'McKinley' or names with multiple caps.
    return ' '.join(word.capitalize() for word in words)

def load_club_aliases(filepath="club_aliases.json"):
    """
    Loads the club alias mapping from a JSON file into a dictionary.
    """

    try:
        with open(filepath, 'r') as f:
            aliases = json.load(f)
            # Standardize the keys to Title Case for consistent lookups
            return {key.title(): value for key, value in aliases.items()}
    except FileNotFoundError:
        print(f"Warning: '{filepath}' not found. No club aliases will be applied.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Could not parse '{filepath}'. Please check if it's valid JSON.")
        return {}

def standardize_club_name(club_str, alias_map):
    """
    Cleans a club name and applies an alias from the mapping if one exists.
    """
    if not club_str or not isinstance(club_str, str):
        return None
    
    # Standardize to Title Case for lookup
    cleaned_club = club_str.strip().title()
    
    # .get(key, default) is perfect here. If the alias exists, use it.
    # Otherwise, use the cleaned_club name as the default.
    return alias_map.get(cleaned_club, cleaned_club)

# ==============================================================================
#  3. MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":
    if setup_database():
        # Load the club aliases into memory
        club_aliases = load_club_aliases()
        
        # Load the Kscore meet manifest into memory
        meet_manifest = load_meet_manifest(KSCORE_MEET_MANIFEST_FILE)
        
        # Pass the manifest and the aliases to the processing function
        process_kscore_files(meet_manifest, club_aliases)
        
        print("\n--- Kscore data loading script finished ---")