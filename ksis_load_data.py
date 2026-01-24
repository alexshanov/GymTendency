# ksis_load_data.py

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
    mark_file_processed,
    parse_rank
)

# --- CONFIGURATION (Specific to KSIS Test) ---
# --- CONFIGURATION (Production) ---
DB_FILE = "gym_data.db"
KSIS_CSVS_DIR = "CSVs_ksis_messy" 
MEET_MANIFEST_FILE = "discovered_meet_ids_ksis.csv" 

def load_meet_manifest(manifest_file):
    print(f"--- Loading KSIS manifest from '{manifest_file}' ---")
    try:
        manifest_df = pd.read_csv(manifest_file)
        manifest = {
            str(row['MeetID']): {
                'name': row['MeetName'],
                'source': 'ksis'
            }
            for _, row in manifest_df.iterrows()
        }
        return manifest
    except Exception as e:
        print(f"Warning: Could not load KSIS manifest: {e}")
        return {} 

def parse_ksis_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_alias_map):
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty or 'Name' not in df.columns:
            return True
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return False

    source_meet_id = str(df['MeetID'].iloc[0])
    meet_year = df['MeetYear'].iloc[0] if 'MeetYear' in df.columns else ""
    
    meet_details = meet_manifest.get(source_meet_id, {
        'name': df['MeetName'].iloc[0],
        'source': 'ksis',
        'year': meet_year
    })
    if not meet_details.get('year') and meet_year:
        meet_details['year'] = meet_year
    
    meet_db_id = get_or_create_meet(conn, 'ksis', source_meet_id, meet_details, meet_cache)

    # Detect discipline from the session or columns
    session_name = df['Session'].iloc[0]
    # In this project schema: 1=WAG, 2=MAG
    discipline_id = 2 # Default MAG for this test (Ontario Cup MAG)
    if "WAG" in session_name or "Women" in session_name: 
        discipline_id = 1
    
    gender_heuristic = 'M' if discipline_id == 2 else 'F'
    
    cursor = conn.cursor()
    
    # Identify apparatuses present in the columns
    # Pattern: {app}_Total, {app}_D, etc.
    app_bases = set()
    for col in df.columns:
        if col.endswith('_Total') and col != 'AA_Total':
            app_bases.add(col.replace('_Total', ''))

    # Apparatus mapping (KSIS names to standard names)
    APP_MAP = {
        'mfloor': 'Floor',
        'horse': 'Pommel Horse',
        'rings': 'Rings',
        'mvault': 'Vault',
        'pbars': 'Parallel Bars',
        'hbar': 'High Bar',
        'wvault': 'Vault',
        'ubars': 'Uneven Bars',
        'beam': 'Beam',
        'wfloor': 'Floor'
    }

    for _, row in df.iterrows():
        raw_name = row['Name']
        person_name = standardize_athlete_name(raw_name)
        if not person_name: continue
        
        person_id = get_or_create_person(conn, person_name, gender_heuristic, person_cache)
        
        raw_club = row['Club']
        club_name = standardize_club_name(raw_club, club_alias_map)
        club_id = get_or_create_club(conn, club_name, club_cache)
        athlete_id = get_or_create_athlete_link(conn, person_id, club_id, athlete_cache)
        
        # Metadata
        level = row.get('Session', '')
        
        # AA Special Case
        aa_score_str = row.get('AA_Score')
        if aa_score_str:
            aa_app_key = ('All Around', discipline_id)
            if aa_app_key in apparatus_cache:
                aa_app_id = apparatus_cache[aa_app_key]
                aa_score_final = pd.to_numeric(aa_score_str, errors='coerce')
                aa_rank = parse_rank(row.get('Place', ''))
                
                cursor.execute("""
                    INSERT INTO Results (meet_db_id, athlete_id, apparatus_id, score_final, rank_numeric, level, gender)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (meet_db_id, athlete_id, aa_app_id, float(aa_score_final) if not pd.isna(aa_score_final) else None, aa_rank, level, gender_heuristic))

        # Individual Apps
        for app_base in app_bases:
            std_app_name = APP_MAP.get(app_base, app_base)
            app_key = (std_app_name, discipline_id)
            if app_key not in apparatus_cache:
                # Try fallback
                app_key = (std_app_name, 99)
            
            if app_key not in apparatus_cache: continue
            
            app_id = apparatus_cache[app_key]
            
            total_str = row[f"{app_base}_Total"]
            d_str = row[f"{app_base}_D"]
            e_str = row[f"{app_base}_E"]
            bonus_str = row[f"{app_base}_Bonus"]
            nd_str = row[f"{app_base}_ND"]
            
            # Extract score and rank from total_str like "12.150(5)"
            match = re.search(r'([\d\.]+)\((\d+)\)', total_str)
            if match:
                score_final = match.group(1)
                rank = match.group(2)
            else:
                score_final = total_str
                rank = ""
            
            def to_f(s):
                if not s or s == '-' or s == 'nan': return None
                try: return float(s)
                except: return None

            cursor.execute("""
                INSERT INTO Results (meet_db_id, athlete_id, apparatus_id, score_final, score_d, score_e, penalty, rank_numeric, bonus, level, gender)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                meet_db_id, athlete_id, app_id, 
                to_f(score_final), to_f(d_str), to_f(e_str), to_f(nd_str), 
                parse_rank(rank), to_f(bonus_str), level, gender_heuristic
            ))
            
    conn.commit()
    return True

def main():
    if not os.path.exists(DB_FILE):
        print(f"Initializing {DB_FILE}...")
        setup_database(DB_FILE)
    
    club_aliases = load_club_aliases()
    meet_manifest = load_meet_manifest(MEET_MANIFEST_FILE)
    
    csv_files = glob.glob(os.path.join(KSIS_CSVS_DIR, "*.csv"))
    csv_files.sort()
    
    with sqlite3.connect(DB_FILE) as conn:
        person_cache = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
        club_cache = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
        athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
        apparatus_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
        meet_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}

        for filepath in csv_files:
            print(f"Loading {os.path.basename(filepath)}...")
            parse_ksis_file(filepath, conn, person_cache, club_cache, athlete_cache, apparatus_cache, meet_cache, meet_manifest, club_aliases)

if __name__ == "__main__":
    main()
