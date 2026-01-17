
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

# --- CONFIGURATION (Specific to MSO) ---
DB_FILE = "gym_data.db"
MSO_CSVS_DIR = "CSVs_mso_final" 
MSO_MANIFEST_FILE = "discovered_meet_ids_mso.csv"

# ==============================================================================
#  DATA LOADING FUNCTIONS (Specific to MSO)
# ==============================================================================

def load_meet_manifest(manifest_file):
    """
    Reads the MSO meet manifest CSV into a dictionary.
    """
    print(f"--- Loading MSO meet manifest from '{manifest_file}' ---")
    try:
        manifest_df = pd.read_csv(manifest_file)
        manifest = {
            str(row['MeetID']): {
                'name': row['MeetName'],
                'start_date_iso': row['Date'], # MSO scraper saved raw date string here
                'location': row['State'], # Or we could parse filter text
                'year': None # Not explicit in manifest unless we parse date
            }
            for _, row in manifest_df.iterrows()
        }
        print(f"Successfully loaded details for {len(manifest)} MSO meets into memory.")
        return manifest
    except (FileNotFoundError, KeyError) as e:
        print(f"Warning: Could not load MSO manifest. Meet details will be incomplete. Error: {e}")
        return {} 

def process_mso_files(meet_manifest, club_alias_map):
    """
    Main function to find and process all MSO result CSV files.
    """
    print("\n--- Starting to process MSO result files ---")
    
    # MSO scraper output matches: {meet_id}_mso.csv
    search_pattern = os.path.join(MSO_CSVS_DIR, "*_mso.csv")
    csv_files = glob.glob(search_pattern)

    if not csv_files:
        print(f"Warning: No result files found in '{MSO_CSVS_DIR}'.")
        return
        
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # --- Caches ---
            person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
            club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
            apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
            meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}

            for filepath in csv_files:
                print(f"\nProcessing file: {os.path.basename(filepath)}")
                parse_mso_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map)

    except Exception as e:
        print(f"A critical error occurred during file processing: {e}")
        traceback.print_exc()

def parse_mso_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map):
    """
    Parses a single MSO CSV and loads its data into the database.
    """
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty or 'Name' not in df.columns:
            print("File is empty or missing 'Name' column. Skipping.")
            return
    except Exception as e:
        print(f"Warning: Could not read CSV file '{filepath}'. Error: {e}")
        return

    # Filename: {meet_id}_mso.csv
    filename = os.path.basename(filepath)
    source_meet_id = filename.split('_mso.csv')[0]
    
    meet_details = meet_manifest.get(source_meet_id, {})
    if not meet_details.get('name'):
        meet_details['name'] = df['Meet'].iloc[0] if 'Meet' in df.columns and not df.empty else f"MSO {source_meet_id}"
        
    meet_db_id = get_or_create_meet(conn, 'mso', source_meet_id, meet_details, meet_cache)

    discipline_id, discipline_name, gender_heuristic = detect_discipline(df)
    print(f"  Detected Discipline: {discipline_name}")

    core_column = 'Name'
    result_columns = [col for col in df.columns if col.startswith('Result_')]
    
    # Define columns to EXCLUDE from the details_json bag
    service_columns = ['Name', 'Club', 'Level', 'Age', 'Prov', 'Age_Group', 'Meet', 'Group']
    dynamic_columns = [col for col in df.columns if col not in service_columns and not col.startswith('Result_')]
    
    # Map raw event columns to clean names
    # Scraper output: Result_Floor_Score, Result_Floor_D_Score, Result_Floor_Rank
    event_bases = {}
    for col in result_columns:
        # Match 'Result_{Apparatus}_{Metric}'
        # Metric can be Score, D_Score, Rank
        parts = col.split('_')
        # Result, Part1, [Part2...], Metric
        if len(parts) >= 3:
            metric = parts[-1]
            if metric in ['Score', 'Rank']:
                raw_event_name = "_".join(parts[1:-1])
                # Handle D_Score case (metric=Score, but penultimate is D)
                if parts[-2] == 'D':
                    raw_event_name = "_".join(parts[1:-2])
            else:
                 continue
            
            clean_name = raw_event_name.replace('_', ' ')
            if clean_name not in event_bases: event_bases[clean_name] = raw_event_name

    athletes_processed = 0
    results_inserted = 0
    cursor = conn.cursor()

    for index, row in df.iterrows():
        # 1. Standardize the name
        person_name = standardize_athlete_name(row.get(core_column))
        if not person_name:
            continue
            
        athletes_processed += 1
        
        # 2. Get the unique Person ID
        person_id = get_or_create_person(conn, person_name, gender_heuristic, person_cache)
        
        # 3. Standardize the club name and get the unique Club ID
        club_name = standardize_club_name(row.get('Club'), club_alias_map)
        club_id = get_or_create_club(conn, club_name, club_cache)
        
        # 4. Get the unique Athlete ID
        athlete_id = get_or_create_athlete_link(conn, person_id, club_id, athlete_cache)
        
        details_dict = {col: val for col, val in row[dynamic_columns].items() if val}
        details_json = json.dumps(details_dict)
        
        for clean_name, raw_name in event_bases.items():
            apparatus_key = (clean_name, discipline_id)
            if apparatus_key not in apparatus_cache: 
                # Try fallback (e.g. AllAround is usually universal)
                apparatus_key = (clean_name, 99)
                 
            if apparatus_key not in apparatus_cache: 
                # print(f"Warning: Apparatus '{clean_name}' not found in cache for discipline {discipline_id}")
                continue
            
            apparatus_id = apparatus_cache[apparatus_key]
            
            # Construct column names based on scraper output
            score_col = f'Result_{raw_name}_Score'
            d_col = f'Result_{raw_name}_D_Score'
            rank_col = f'Result_{raw_name}_Rank'
            
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
            
            # --- EXTRACT NEW STANDARD FIELDS ---
            level_val = row.get('Level')
            age_raw = row.get('Age')
            prov_val = row.get('Prov')
            
            age_numeric = pd.to_numeric(age_raw, errors='coerce') if age_raw else None

            # Check if result is valid (has score or rank)
            if pd.isna(score_numeric) and not score_text and not rank_val:
                continue

            cursor.execute("""
                INSERT INTO Results (
                    meet_db_id, athlete_id, apparatus_id, 
                    level, age, province,
                    score_d, score_final, score_text, 
                    rank_numeric, rank_text, details_json
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                meet_db_id, athlete_id, apparatus_id, 
                level_val, age_numeric, prov_val,
                d_numeric, score_numeric, score_text, 
                rank_numeric, rank_text, details_json
            ))
            results_inserted += 1
            
    conn.commit()
    print(f"  Processed {athletes_processed} athletes, inserting {results_inserted} result records.")

def main():
    if not os.path.exists(DB_FILE):
        print(f"Database {DB_FILE} not found. Please run create_db.py first.")
        return

    club_aliases = load_club_aliases()
    meet_manifest = load_meet_manifest(MSO_MANIFEST_FILE)
    
    process_mso_files(meet_manifest, club_aliases)
    
    print("\n--- MSO data loading script finished ---")

if __name__ == "__main__":
    main()
