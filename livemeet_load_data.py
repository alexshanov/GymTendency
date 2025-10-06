# livemeet_load_data.py

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

# --- CONFIGURATION (Specific to Livemeet) ---
DB_FILE = "gym_data.db"
LIVEMEET_CSVS_DIR = "CSVs_final" 
MEET_MANIFEST_FILE = "discovered_meet_ids.csv" 

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

def process_livemeet_files(meet_manifest, club_alias_map):
    """
    Main function to find and process all Livemeet result CSV files.
    """
    print("\n--- Starting to process Livemeet result files ---")
    
    search_pattern = os.path.join(LIVEMEET_CSVS_DIR, "*_FINAL_*.csv")
    csv_files = glob.glob(search_pattern)

    if not csv_files:
        print(f"Warning: No result files found in '{LIVEMEET_CSVS_DIR}'.")
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
                parse_livemeet_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map)

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
            return
    except Exception as e:
        print(f"Warning: Could not read CSV file '{filepath}'. Error: {e}")
        return

    source_meet_id = os.path.basename(filepath).split('_FINAL_')[0]
    meet_details = meet_manifest.get(source_meet_id, {})
    if not meet_details.get('name'):
        meet_details['name'] = df['Meet'].iloc[0] if 'Meet' in df.columns and not df.empty else f"Livemeet {source_meet_id}"
    meet_db_id = get_or_create_meet(conn, 'livemeet', source_meet_id, meet_details, meet_cache)

    discipline_id, discipline_name, gender_heuristic = detect_discipline(df)
    print(f"  Detected Discipline: {discipline_name}")

    core_column = 'Name'
    result_columns = [col for col in df.columns if col.startswith('Result_')]
    dynamic_columns = [col for col in df.columns if col not in [core_column] and not col.startswith('Result_')]
    
    event_bases = {}
    for col in result_columns:
        match = re.search(r'Result_(.*)_(Score|D)$', col)
        if match:
            raw_event_name = match.group(1).replace('_', ' ')
            if raw_event_name not in event_bases: event_bases[raw_event_name] = raw_event_name.replace(' ', '_')

    athletes_processed = 0
    results_inserted = 0
    cursor = conn.cursor()

    for index, row in df.iterrows():
        # --- NEW LOGIC FOR ATHLETE/PERSON/CLUB ---
        
        # 1. Standardize the name
        person_name = standardize_athlete_name(row.get(core_column))
        if not person_name:
            print(f"Warning: Skipping row {index+2} due to invalid or empty athlete name.")
            continue
            
        athletes_processed += 1
        
        # 2. Get the unique Person ID
        person_id = get_or_create_person(conn, person_name, gender_heuristic, person_cache)
        
        # 3. Standardize the club name and get the unique Club ID
        club_name = standardize_club_name(row.get('Club'), club_alias_map)
        club_id = get_or_create_club(conn, club_name, club_cache)
        
        # 4. Get the unique Athlete ID that links this Person and Club
        athlete_id = get_or_create_athlete_link(conn, person_id, club_id, athlete_cache)
        
        # --- END OF NEW LOGIC ---

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
            
            cursor.execute("""
                INSERT INTO Results (meet_db_id, athlete_id, apparatus_id, score_d, score_final, score_text, rank_numeric, rank_text, details_json) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (meet_db_id, athlete_id, apparatus_id, d_numeric, score_numeric, score_text, rank_numeric, rank_text, details_json))
            results_inserted += 1
            
    conn.commit()
    print(f"  Processed {athletes_processed} athletes, inserting {results_inserted} result records.")

# ==============================================================================
#  MAIN EXECUTION
# ==============================================================================
def main():
    """
    Main execution block for the Livemeet data loader.
    """
    if setup_database(DB_FILE):
        club_aliases = load_club_aliases()
        meet_manifest = load_meet_manifest(MEET_MANIFEST_FILE)
        
        process_livemeet_files(meet_manifest, club_aliases)
        
        print("\n--- Livemeet data loading script finished ---")

if __name__ == "__main__":
    main()