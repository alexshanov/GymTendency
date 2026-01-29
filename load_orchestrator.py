# load_orchestrator.py

import os
import sqlite3
import pandas as pd
import glob
import time
import re
import json
import argparse
import traceback
import signal
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

# Import extraction library
import extraction_library

# Import shared functions from ETL library
from etl_functions import (
    setup_database,
    load_club_aliases,
    standardize_club_name,
    standardize_athlete_name,
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
    parse_rank
)

# --- CONFIGURATION ---
DB_FILE = "gym_data.db"
KSCORE_DIR = "CSVs_kscore_final"
LIVEMEET_DIR = "CSVs_Livemeet_final"
MSO_DIR = "CSVs_mso_final"
KSIS_DIR = "CSVs_ksis_messy"

KSCORE_MANIFEST = "discovered_meet_ids_kscore.csv"
LIVEMEET_MANIFEST = "discovered_meet_ids_livemeet.csv"
MSO_MANIFEST = "discovered_meet_ids_mso.csv"
KSIS_MANIFEST = "discovered_meet_ids_ksis.csv"

# ==============================================================================
#  WORKER: READER (Parallel)
# ==============================================================================

def reader_worker(scraper_type, filepath, manifest, aliases=None):
    """
    Parallel worker that reads and extracts data from a CSV.
    """
    try:
        if scraper_type == 'kscore':
            return extraction_library.extract_kscore_data(filepath, manifest, aliases)
        elif scraper_type == 'livemeet':
            return extraction_library.extract_livemeet_data(filepath, manifest)
        elif scraper_type == 'mso':
            return extraction_library.extract_mso_data(filepath, manifest)
        elif scraper_type == 'ksis':
            return extraction_library.extract_ksis_data(filepath, manifest)
    except Exception as e:
        return {'error': str(e), 'filepath': filepath}
    return None

# ==============================================================================
#  LOADER: WRITER (Serial)
# ==============================================================================

def write_to_db(conn, data_package, caches, club_alias_map):
    """
    Serial function that takes extracted data and writes it to the database.
    """
    if not data_package or 'error' in data_package:
        if data_package and 'error' in data_package:
             logging.error(f"Extraction Error: {data_package['error']} ({data_package.get('filepath')})")
        return False
    
    source = data_package['source']
    source_meet_id = data_package['source_meet_id']
    meet_details = data_package['meet_details']
    results = data_package['results']
    
    # 1. Meet
    meet_db_id = get_or_create_meet(conn, source, source_meet_id, meet_details, caches['meet'])
    
    cursor = conn.cursor()
    inserted_count = 0
    
    for athlete_res in results:
        # 2. Athlete Identification
        person_name = standardize_athlete_name(athlete_res['raw_name'])
        if not person_name: continue
        
        person_id = get_or_create_person(conn, person_name, athlete_res['gender_heuristic'], caches['person'])
        
        club_name = standardize_club_name(athlete_res['raw_club'], club_alias_map)
        club_id = get_or_create_club(conn, club_name, caches['club'])
        
        athlete_id = get_or_create_athlete_link(conn, person_id, club_id, caches['athlete'])
        
        discipline_id = athlete_res['discipline_id']
        gender = athlete_res['gender_heuristic']
        
        # 3. Dynamic Metadata Handling
        dynamic_values = {}
        misc_details = {}
        from etl_functions import METADATA_WHITELIST
        
        for raw_col, val in athlete_res['dynamic_metadata'].items():
            safe_col = sanitize_column_name(raw_col)
            if ensure_column_exists(cursor, 'Results', safe_col, 'TEXT'):
                dynamic_values[safe_col] = val
                # Unify Group into Session for database consistency
                if safe_col == 'group' and 'session' not in dynamic_values:
                    dynamic_values['session'] = val
            else:
                misc_details[safe_col] = val

        # 4. Apparatus Results
        for app_res in athlete_res['apparatus_results']:
            # Merge misc_details with any apparatus-specific metadata
            final_details = misc_details.copy()
            if app_res.get('calculated'):
                final_details['calculated'] = True
            if app_res.get('calculated_d'):
                final_details['calculated_d'] = True
            details_json = json.dumps(final_details) if final_details else None

            raw_event = app_res['raw_event']
            
            # Normalization for apparatus mapping
            clean_name = raw_event.replace('_', ' ')
            if clean_name == "Balance Beam": clean_name = "Beam"
            if clean_name == "Uneven Bars": clean_name = "Uneven Bars"
            if clean_name == "AllAround" or clean_name == "All Around": clean_name = "All Around"
            if clean_name == "High Bar" or clean_name == "Horizontal Bar": clean_name = "High Bar"
            if clean_name == "Parallel Bars": clean_name = "Parallel Bars"
            if clean_name == "Pommel Horse": clean_name = "Pommel Horse"
            
            app_key = (clean_name, discipline_id)
            if app_key not in caches['apparatus']:
                # Try raw name too
                app_key = (raw_event, discipline_id)
            if app_key not in caches['apparatus']:
                app_key = (clean_name, 99) # Fallback to 'Other' discipline
            
            if app_key not in caches['apparatus']:
                continue
                
            apparatus_id = caches['apparatus'][app_key]
            
            # Check Session-Aware Uniqueness
            current_session = dynamic_values.get('session') or dynamic_values.get('group')
            current_level = dynamic_values.get('level')
            
            existing_result_id = check_duplicate_result(conn, meet_db_id, athlete_id, apparatus_id, session=current_session, level=current_level)
            
            # Numeric conversions
            def to_float(v):
                if v is None: return None
                try: 
                    # Remove potential comma from things like "1,000"
                    return float(str(v).replace(',', ''))
                except: return None

            score_raw = app_res.get('score_final')
            score_final = to_float(score_raw)
            score_d = to_float(app_res.get('score_d'))
            score_sv = to_float(app_res.get('score_sv'))
            score_e = to_float(app_res.get('score_e'))
            bonus = to_float(app_res.get('bonus'))
            penalty = to_float(app_res.get('penalty'))
            exec_bonus = to_float(app_res.get('execution_bonus'))
            
            rank_text = app_res.get('rank_text')
            rank_numeric = parse_rank(rank_text) if rank_text else None
            score_text = app_res.get('score_text')

            # Preservation Logic: If numeric score failed but raw text exists, keep it in score_text
            if score_final is None and score_raw and str(score_raw).strip() != '':
                if not score_text or str(score_text).strip() == '':
                    score_text = str(score_raw).strip()

            # Preserve Non-numeric D-score in details_json if possible
            score_d_raw = app_res.get('score_d')
            if score_d is None and score_d_raw and str(score_d_raw).strip() != '':
                if not final_details: final_details = {}
                final_details['score_d_text'] = str(score_d_raw).strip()
                details_json = json.dumps(final_details)

            if existing_result_id:
                # 1. Fetch existing state to see if we should update
                cursor.execute("SELECT score_final, score_d, score_text, rank_text, details_json FROM Results WHERE result_id = ?", (existing_result_id,))
                db_res = cursor.fetchone()
                db_final, db_d, db_text, db_rank, db_json_str = db_res if db_res else (None, None, None, None, None)
                db_json = json.loads(db_json_str) if db_json_str else {}
                
                # 2. OVERWRITE LOGIC:
                # Always update if current source is 'DETAILED' or 'PEREVENT'.
                # Otherwise, update ONLY if we are filling a gap (NULL -> Value).
                is_detailed = "DETAILED" in data_package.get('filepath', '') or "PEREVENT" in data_package.get('filepath', '')
                
                new_json = json.loads(details_json) if details_json else {}
                has_new_d_text = 'score_d_text' in new_json and 'score_d_text' not in db_json
                
                should_update = is_detailed or \
                               (score_final is not None and db_final is None) or \
                               (score_d is not None and db_d is None) or \
                               (score_text and not db_text) or \
                               (rank_text and not db_rank) or \
                               has_new_d_text
                
                if should_update:
                     # If not detailed, merge JSON to preserve existing metadata
                     if not is_detailed and db_json:
                         merged_json = db_json.copy()
                         merged_json.update(new_json)
                         details_json = json.dumps(merged_json)
                     
                     cursor.execute("""
                        UPDATE Results 
                        SET score_final = COALESCE(?, score_final), 
                            score_d = COALESCE(?, score_d), 
                            score_text = COALESCE(?, score_text), 
                            rank_numeric = COALESCE(?, rank_numeric), 
                            rank_text = COALESCE(?, rank_text), 
                            details_json = ? 
                        WHERE result_id = ?
                     """, (score_final, score_d, score_text, rank_numeric, rank_text, details_json, existing_result_id))
                continue
            
            # SQL Construction
            cols = ['meet_db_id', 'athlete_id', 'apparatus_id', 'gender', 'score_final', 'score_d', 'score_sv', 'score_e', 'penalty', 'rank_numeric', 'rank_text', 'score_text', 'bonus', 'execution_bonus', 'details_json']
            vals = [meet_db_id, athlete_id, apparatus_id, gender, score_final, score_d, score_sv, score_e, penalty, rank_numeric, rank_text, score_text, bonus, exec_bonus, details_json]
            
            for col_name, col_val in dynamic_values.items():
                cols.append(col_name)
                vals.append(col_val)
                
            placeholders = ', '.join(['?'] * len(cols))
            quoted_cols = [f'"{c}"' for c in cols]
            sql = f"INSERT INTO Results ({', '.join(quoted_cols)}) VALUES ({placeholders})"
            cursor.execute(sql, vals)
            inserted_count += 1
            
    return

def unify_meets(conn):
    """
    Identifies logical meets (Name + Year) and merges them into canonical records.
    Crucial for collapsing data gaps across multiple source files.
    """
    logging.info("Starting meet unification process...")
    cursor = conn.cursor()
    
    # 1. Backfill missing years from meet names if possible (Regex pass)
    cursor.execute("SELECT meet_db_id, name, comp_year FROM Meets WHERE comp_year IS NULL OR comp_year = ''")
    missing_years = cursor.fetchall()
    for m_id, name, _ in missing_years:
        if not name: continue
        match = re.search(r'(20\d{2})', name)
        if match:
            year = int(match.group(1))
            cursor.execute("UPDATE Meets SET comp_year = ? WHERE meet_db_id = ?", (year, m_id))
    
    conn.commit()

    # 2. Identify logical duplicates by (LOWER(TRIM(name)), comp_year)
    # Exclude "Unnamed:" placeholders from grouping to avoid incorrect merging
    cursor.execute("""
        SELECT name, comp_year, MIN(meet_db_id) as canonical_id, GROUP_CONCAT(meet_db_id) as all_ids
        FROM Meets
        WHERE name IS NOT NULL AND name != '' AND name NOT LIKE 'Unnamed:%'
        GROUP BY LOWER(TRIM(name)), comp_year
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    
    total_unified = 0
    for name, year, canonical_id, all_ids_str in duplicates:
        all_ids = [int(i) for i in all_ids_str.split(',')]
        others = [i for i in all_ids if i != canonical_id]
        if not others: continue
        
        logging.info(f"Unifying: '{name}' ({year}) -> Canonical ID: {canonical_id} (Merging IDs: {others})")
        
        # Merge results to canonical ID
        for other_id in others:
            cursor.execute("UPDATE Results SET meet_db_id = ? WHERE meet_db_id = ?", (canonical_id, other_id))
            
        # Delete duplicate meet headers
        placeholders = ', '.join(['?'] * len(others))
        cursor.execute(f"DELETE FROM Meets WHERE meet_db_id IN ({placeholders})", others)
        total_unified += len(others)
        
    conn.commit()
    logging.info(f"Meet unification complete. Removed {total_unified} duplicate meet records.")

def refresh_gold_tables(conn):
    """
    Creates/Updates flattened 'Gold' tables for MAG and WAG.
    MAG: Gold_Results_MAG (7 triples)
    WAG: Gold_Results_WAG (5 triples)
    """
    logging.info("Refreshing Gold_Results tables (MAG & WAG)...")
    cursor = conn.cursor()
    
    # We drop and recreate for simplicity since it's an aggregation table
    cursor.execute("DROP TABLE IF EXISTS Gold_Results;")
    cursor.execute("DROP TABLE IF EXISTS Gold_Results_MAG;")
    cursor.execute("DROP TABLE IF EXISTS Gold_Results_WAG;")
    
    # --- MAG TABLE ---
    mag_query = """
    CREATE TABLE Gold_Results_MAG AS
    SELECT
        p.full_name AS athlete_name,
        m.source AS source,
        m.comp_year AS year,
        CASE 
            WHEN MAX(r.session) IS NOT NULL AND MAX(r.session) != '' 
            THEN m.name || ' (' || MAX(r.session) || ')' 
            ELSE m.name 
        END AS meet_name,
        MAX(r.level) AS level,
        MAX(r.age) AS age,
        c.name AS club,
        
        -- Floor (fx)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Floor' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Floor' THEN r.score_text END)) AS fx_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Floor' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Floor' THEN json_extract(r.details_json, '$.score_d_text') END)) AS fx_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Floor' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Floor' THEN r.rank_text END)) AS fx_rank,
        
        -- Pommel Horse (ph)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Pommel Horse' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Pommel Horse' THEN r.score_text END)) AS ph_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Pommel Horse' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Pommel Horse' THEN json_extract(r.details_json, '$.score_d_text') END)) AS ph_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Pommel Horse' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Pommel Horse' THEN r.rank_text END)) AS ph_rank,
        
        -- Rings (sr)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Rings' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Rings' THEN r.score_text END)) AS sr_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Rings' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Rings' THEN json_extract(r.details_json, '$.score_d_text') END)) AS sr_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Rings' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Rings' THEN r.rank_text END)) AS sr_rank,
        
        -- Vault (vt)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Vault' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Vault' THEN r.score_text END)) AS vt_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Vault' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Vault' THEN json_extract(r.details_json, '$.score_d_text') END)) AS vt_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Vault' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Vault' THEN r.rank_text END)) AS vt_rank,
        
        -- Parallel Bars (pb)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Parallel Bars' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Parallel Bars' THEN r.score_text END)) AS pb_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Parallel Bars' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Parallel Bars' THEN json_extract(r.details_json, '$.score_d_text') END)) AS pb_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Parallel Bars' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Parallel Bars' THEN r.rank_text END)) AS pb_rank,
        
        -- High Bar (hb)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'High Bar' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'High Bar' THEN r.score_text END)) AS hb_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'High Bar' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'High Bar' THEN json_extract(r.details_json, '$.score_d_text') END)) AS hb_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'High Bar' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'High Bar' THEN r.rank_text END)) AS hb_rank,
        
        -- All Around (aa)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'All Around' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'All Around' THEN r.score_text END)) AS aa_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'All Around' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'All Around' THEN json_extract(r.details_json, '$.score_d_text') END)) AS aa_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'All Around' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'All Around' THEN r.rank_text END)) AS aa_rank
        
    FROM Results r
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    LEFT JOIN Clubs c ON a.club_id = c.club_id
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    WHERE r.gender = 'M'
    GROUP BY p.person_id, m.meet_db_id
    HAVING MAX(r.score_final) IS NOT NULL
    ORDER BY m.comp_year DESC, p.full_name;
    """
    
    # --- WAG TABLE ---
    wag_query = """
    CREATE TABLE Gold_Results_WAG AS
    SELECT
        p.full_name AS athlete_name,
        m.source AS source,
        m.comp_year AS year,
        CASE 
            WHEN MAX(r.session) IS NOT NULL AND MAX(r.session) != '' 
            THEN m.name || ' (' || MAX(r.session) || ')' 
            ELSE m.name 
        END AS meet_name,
        MAX(r.level) AS level,
        MAX(r.age) AS age,
        c.name AS club,
        
        -- Vault (vt)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Vault' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Vault' THEN r.score_text END)) AS vt_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Vault' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Vault' THEN json_extract(r.details_json, '$.score_d_text') END)) AS vt_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Vault' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Vault' THEN r.rank_text END)) AS vt_rank,
        
        -- Uneven Bars (ub)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Uneven Bars' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Uneven Bars' THEN r.score_text END)) AS ub_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Uneven Bars' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Uneven Bars' THEN json_extract(r.details_json, '$.score_d_text') END)) AS ub_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Uneven Bars' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Uneven Bars' THEN r.rank_text END)) AS ub_rank,
        
        -- Beam (bb)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Beam' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Beam' THEN r.score_text END)) AS bb_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Beam' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Beam' THEN json_extract(r.details_json, '$.score_d_text') END)) AS bb_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Beam' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Beam' THEN r.rank_text END)) AS bb_rank,
        
        -- Floor (fx)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Floor' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'Floor' THEN r.score_text END)) AS fx_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'Floor' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'Floor' THEN json_extract(r.details_json, '$.score_d_text') END)) AS fx_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Floor' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Floor' THEN r.rank_text END)) AS fx_rank,
        
        -- All Around (aa)
        COALESCE(CAST(MAX(CASE WHEN app.name = 'All Around' THEN r.score_final END) AS TEXT), MAX(CASE WHEN app.name = 'All Around' THEN r.score_text END)) AS aa_score,
        COALESCE(CAST(MAX(CASE WHEN app.name = 'All Around' THEN r.score_d END) AS TEXT), MAX(CASE WHEN app.name = 'All Around' THEN json_extract(r.details_json, '$.score_d_text') END)) AS aa_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'All Around' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'All Around' THEN r.rank_text END)) AS aa_rank
        
    FROM Results r
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    LEFT JOIN Clubs c ON a.club_id = c.club_id
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    WHERE r.gender = 'F'
    GROUP BY p.person_id, m.meet_db_id
    HAVING MAX(r.score_final) IS NOT NULL
    ORDER BY m.comp_year DESC, p.full_name;
    """
    
    cursor.execute(mag_query)
    cursor.execute(wag_query)
    conn.commit()
    logging.info("Gold_Results_MAG and Gold_Results_WAG updated successfully.")


# ==============================================================================
#  MAIN ORCHESTRATOR
# ==============================================================================

def load_manifest(scraper_type, filepath):
    if not os.path.exists(filepath):
        # Create empty manifest if missing, to prevent crash
        return {}
    
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        logging.error(f"Error reading manifest {filepath}: {e}")
        return {}

    manifest_data = {}
    for _, row in df.iterrows():
        mid = str(row.get('MeetID', '')).strip()
        if not mid or mid == 'nan': continue
        
        # Helper to safely get year as string without .0
        raw_year = row.get('Year')
        clean_year = None
        if pd.notnull(raw_year) and str(raw_year).strip() != '':
            try:
                clean_year = str(int(float(raw_year)))
            except:
                clean_year = str(raw_year).strip()
        
        details = {
            'name': row.get('MeetName'),
            'start_date_iso': row.get('start_date_iso') if 'start_date_iso' in row else row.get('Date'),
            'location': row.get('Location') if 'Location' in row else row.get('State'),
            'year': clean_year
        }
        manifest_data[mid] = details
        
    return manifest_data

def heal_meets_metadata(conn, kscore_manifest, livemeet_manifest, mso_manifest, ksis_manifest):
    """
    Backfills missing metadata (year, name, date, etc.) for all existing meets.
    """
    logging.info("Starting metadata healing pass...")
    cursor = conn.cursor()
    
    # Combined manifests for easier lookup
    combined = {}
    for mid, details in kscore_manifest.items():
        sid = mid.replace('kscore_', '', 1)
        combined[('kscore', sid)] = details
    for mid, details in livemeet_manifest.items():
        combined[('livemeet', mid)] = details
    for mid, details in mso_manifest.items():
        combined[('mso', mid)] = details
    for mid, details in ksis_manifest.items():
        combined[('ksis', mid)] = details

    cursor.execute("SELECT meet_db_id, source, source_meet_id, comp_year, name, start_date_iso, location FROM Meets")
    meets = cursor.fetchall()
    
    updates_count = 0
    for m_id, source, sid, db_year, db_name, db_date, db_loc in meets:
        # Try lookup with original sid and with scraper prefix if needed
        details = combined.get((source, sid))
        if not details:
            prefixed_sid = f"{source}_{sid}"
            details = combined.get((source, prefixed_sid))
        
        if not details:
            # Extra Fallback: Check if manifest ID is a prefix of sid
            for (m_src, m_sid), m_details in combined.items():
                if m_src == source and sid.startswith(m_sid):
                    details = m_details
                    break
            
        if not details: continue
        
        updates = []
        params = []
        
        manifest_year = details.get('year')
        if manifest_year and (not db_year or str(db_year).strip() == ''):
            updates.append("comp_year = ?")
            params.append(str(manifest_year))
            
        manifest_name = details.get('name')
        if manifest_name and (not db_name or "Kscore" in db_name or "Livemeet" in db_name or "Unnamed:" in db_name or db_name == 'Title not set'):
            updates.append("name = ?")
            params.append(str(manifest_name).strip())
            
        manifest_date = details.get('start_date_iso')
        if manifest_date and (not db_date or str(db_date).strip() == ''):
            updates.append("start_date_iso = ?")
            params.append(str(manifest_date))
            
        manifest_loc = details.get('location')
        if manifest_loc and str(manifest_loc) != 'N/A' and (not db_loc or str(db_loc).strip() == ''):
            updates.append("location = ?")
            params.append(str(manifest_loc))
            
        if updates:
            sql = f"UPDATE Meets SET {', '.join(updates)} WHERE meet_db_id = ?"
            params.append(m_id)
            cursor.execute(sql, params)
            updates_count += 1
            
    conn.commit()
    logging.info(f"Metadata healing pass complete. Updated {updates_count} meets.")

def main():
    parser = argparse.ArgumentParser(description="Parallel GymTendency Data Loader")
    parser.add_argument("--workers", type=int, default=50, help="Number of parallel readers")
    parser.add_argument("--sample", type=int, default=1, help="Process every Nth file")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of files to process")
    parser.add_argument("--gold-only", action="store_true", help="Skip file processing and only refresh Gold tables")
    args = parser.parse_args()

    # 1. Load context
    if not setup_database(DB_FILE):
        logging.error("Database setup failed. Exiting.")
        return
        
    club_aliases = load_club_aliases()
    kscore_manifest = load_manifest('kscore', KSCORE_MANIFEST)
    livemeet_manifest = load_manifest('livemeet', LIVEMEET_MANIFEST)
    mso_manifest = load_manifest('mso', MSO_MANIFEST)
    ksis_manifest = load_manifest('ksis', KSIS_MANIFEST)
    
    # HEAL METADATA FIRST
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        heal_meets_metadata(conn, kscore_manifest, livemeet_manifest, mso_manifest, ksis_manifest)

    # 2. Find files
    files_to_process = []
    
    # KScore
    level_aliases = {}
    if os.path.exists("kscore_level_aliases.json"):
        with open("kscore_level_aliases.json", 'r') as f:
            level_aliases = json.load(f)

    k_files = glob.glob(os.path.join(KSCORE_DIR, "*_FINAL_*.csv"))
    for f in k_files: files_to_process.append(('kscore', f, kscore_manifest.get(os.path.basename(f).split('_FINAL_')[0], {}), level_aliases))
    
    # LiveMeet
    l_files = glob.glob(os.path.join(LIVEMEET_DIR, "*_FINAL_*.csv"))
    l_files += glob.glob(os.path.join(LIVEMEET_DIR, "*_PEREVENT_*.csv"))
    l_files += glob.glob(os.path.join(LIVEMEET_DIR, "*_BYEVENT_*.csv"))
    for f in l_files: files_to_process.append(('livemeet', f, livemeet_manifest.get(os.path.basename(f).split('_')[0], {}), None))
    
    # MSO
    m_files = glob.glob(os.path.join(MSO_DIR, "*_mso.csv"))
    for f in m_files: files_to_process.append(('mso', f, mso_manifest.get(os.path.basename(f).split('_mso.csv')[0], {}), None))

    # KSIS
    ksis_files = glob.glob(os.path.join(KSIS_DIR, "*.csv"))
    for f in ksis_files: 
        # Filename example: 9143_ksis_299177_...
        # Source meet ID is the first part (9143)
        mid = os.path.basename(f).split('_')[0]
        files_to_process.append(('ksis', f, ksis_manifest.get(mid, {}), None))

    import random
    random.shuffle(files_to_process) # Shuffle to ensure parallel processing of different sources
    if args.sample > 1: files_to_process = files_to_process[::args.sample]

    if not files_to_process and not args.gold_only:
        print("No files found to process.")
        return

    completed = 0
    start_time = time.time()
    
    # 3. Filter by processed state AND apply limit
    unprocessed = []
    
    if not args.gold_only:
        with sqlite3.connect(DB_FILE) as conn:
            processed_map = {row[0]: True for row in conn.execute("SELECT file_hash FROM ProcessedFiles").fetchall()}
            
            for stype, fpath, manifest, aliases in files_to_process:
                fhash = calculate_file_hash(fpath)
                if fhash not in processed_map:
                     unprocessed.append((stype, fpath, fhash, manifest, aliases))
                     if args.limit > 0 and len(unprocessed) >= args.limit:
                         break
    
    logging.info(f"Total files found: {len(files_to_process)}. New/Changed: {len(unprocessed)}")
    print(f"Total files found: {len(files_to_process)}. New to process: {len(unprocessed)}")
    
    if not unprocessed and not args.gold_only:
        logging.info("No unprocessed files found.")
    elif not args.gold_only:
        if not unprocessed:
            logging.info("No unprocessed files found.")
        else:
            # 4. Process in Parallel
            caches = {}
            with sqlite3.connect(DB_FILE) as conn:
                caches['person'] = {row[1]: row[0] for row in conn.execute("SELECT person_id, full_name FROM Persons").fetchall()}
                caches['club'] = {row[1]: row[0] for row in conn.execute("SELECT club_id, name FROM Clubs").fetchall()}
                caches['athlete'] = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, person_id, club_id FROM Athletes").fetchall()}
                caches['apparatus'] = {(row[1], row[2]): row[0] for row in conn.execute("SELECT apparatus_id, name, discipline_id FROM Apparatus").fetchall()}
                caches['meet'] = {(row[1], row[2]): row[0] for row in conn.execute("SELECT meet_db_id, source, source_meet_id FROM Meets").fetchall()}

            total = len(unprocessed)
            batch_size = 10 
            
            stop_requested = False
            def signal_handler(sig, frame):
                nonlocal stop_requested
                logging.warning("Shutdown requested... finishing current tasks and closing DB.")
                stop_requested = True

            signal.signal(signal.SIGINT, signal_handler)

            with sqlite3.connect(DB_FILE) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                
                with ProcessPoolExecutor(max_workers=args.workers) as executor:
                    future_to_file = {
                        executor.submit(reader_worker, stype, fpath, manifest, aliases): (stype, fpath, fhash) 
                        for stype, fpath, fhash, manifest, aliases in unprocessed
                    }
                    
                    for future in as_completed(future_to_file):
                        if stop_requested:
                            executor.shutdown(wait=False, cancel_futures=True)
                            break

                        stype, fpath, fhash = future_to_file[future]
                        completed += 1
                        
                        print(f"[{stype} {completed}/{total}] {os.path.basename(fpath)}")
                        
                        try:
                            data_package = future.result()
                            if data_package:
                                write_to_db(conn, data_package, caches, club_aliases)
                                mark_file_processed(conn, fpath, fhash)
                        except Exception as e:
                            logging.error(f"Error processing {fpath}: {e}")
                        
                        if completed % batch_size == 0:
                            conn.commit()
                            elapsed = time.time() - start_time
                            rate = completed / elapsed
                            remaining = (total - completed) / rate if rate > 0 else 0
                            logging.info(f"Progress: [{completed}/{total}] ({rate:.2f} files/s, ETA: {remaining/60:.1f}m)")
                
                conn.commit()
    else:
        logging.info("Skipping CSV processing due to --gold-only flag.")

    # 5. Autonomous Cleanup & Unification
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        
        logging.info("Running Metadata Healing Pass...")
        heal_meets_metadata(conn, kscore_manifest, livemeet_manifest, mso_manifest, ksis_manifest)
        
        unify_meets(conn)
        
        refresh_gold_tables(conn)

    if not args.gold_only:
        logging.info(f"Finished! Processed {completed} files in {time.time() - start_time:.2f}s.")
    else:
        logging.info(f"Gold table refresh complete in {time.time() - start_time:.2f}s.")

if __name__ == "__main__":
    logging.basicConfig(
        filename='loader_orchestrator.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING) 
    logging.getLogger('').addHandler(console)
    
    logging.info("Loader Orchestrator Started")
    main()
