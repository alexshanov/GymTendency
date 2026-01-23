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

KSCORE_MANIFEST = "discovered_meet_ids_kscore.csv"
LIVEMEET_MANIFEST = "discovered_meet_ids_livemeet.csv"
MSO_MANIFEST = "discovered_meet_ids_mso.csv"

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
        
        # 3. Dynamic Metadata
        dynamic_values = {}
        for raw_col, val in athlete_res['dynamic_metadata'].items():
            safe_col = sanitize_column_name(raw_col)
            ensure_column_exists(cursor, 'Results', safe_col, 'TEXT')
            dynamic_values[safe_col] = val

        # 4. Apparatus Results
        for app_res in athlete_res['apparatus_results']:
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
            
            existing_result_id = check_duplicate_result(conn, meet_db_id, athlete_id, apparatus_id)
            if existing_result_id:
                # OVERWRITE LOGIC:
                # If current source file is 'DETAILED' or 'PEREVENT' and we are seeing different data, update.
                is_detailed = "DETAILED" in data_package.get('filepath', '') or "PEREVENT" in data_package.get('filepath', '')
                
                if is_detailed:
                     score_final = to_float(app_res.get('score_final'))
                     # Simple check: update if numeric score is provided
                     if score_final is not None:
                         cursor.execute("UPDATE Results SET score_final = ?, score_d = ?, rank_numeric = ?, rank_text = ? WHERE result_id = ?", 
                                        (score_final, to_float(app_res.get('score_d')), parse_rank(app_res.get('rank_text')), app_res.get('rank_text'), existing_result_id))
                
                # Rank Backfill Logic (Original)
                rank_text = app_res.get('rank_text')
                rank_numeric = parse_rank(rank_text) if rank_text else None
                if rank_text and str(rank_text).strip() != '':
                    cursor.execute("SELECT rank_text FROM Results WHERE result_id = ?", (existing_result_id,))
                    db_rank_row = cursor.fetchone()
                    db_rank = db_rank_row[0] if db_rank_row else None
                    if not db_rank or str(db_rank).strip() == '':
                        print(f"  [Backfill] Adding missing rank '{rank_text}' to existing result ID {existing_result_id} ({app_res.get('raw_event')})")
                        cursor.execute("UPDATE Results SET rank_numeric = ?, rank_text = ? WHERE result_id = ?", 
                                       (rank_numeric, rank_text, existing_result_id))
                continue
            
            # Numeric conversions
            def to_float(v):
                if v is None: return None
                try: return float(v)
                except: return None

            score_final = to_float(app_res.get('score_final'))
            score_d = to_float(app_res.get('score_d'))
            score_sv = to_float(app_res.get('score_sv'))
            score_e = to_float(app_res.get('score_e'))
            bonus = to_float(app_res.get('bonus'))
            penalty = to_float(app_res.get('penalty'))
            exec_bonus = to_float(app_res.get('execution_bonus'))
            
            rank_text = app_res.get('rank_text')
            rank_numeric = parse_rank(rank_text) if rank_text else None
            score_text = app_res.get('score_text')
            
            # SQL Construction
            cols = ['meet_db_id', 'athlete_id', 'apparatus_id', 'gender', 'score_final', 'score_d', 'score_sv', 'score_e', 'penalty', 'rank_numeric', 'rank_text', 'score_text', 'bonus', 'execution_bonus']
            vals = [meet_db_id, athlete_id, apparatus_id, gender, score_final, score_d, score_sv, score_e, penalty, rank_numeric, rank_text, score_text, bonus, exec_bonus]
            
            for col_name, col_val in dynamic_values.items():
                cols.append(col_name)
                vals.append(col_val)
                
            placeholders = ', '.join(['?'] * len(cols))
            quoted_cols = [f'"{c}"' for c in cols]
            sql = f"INSERT INTO Results ({', '.join(quoted_cols)}) VALUES ({placeholders})"
            cursor.execute(sql, vals)
            inserted_count += 1
            
    return

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
        m.name AS meet_name,
        MAX(r.level) AS level,
        MAX(r.age) AS age,
        c.name AS club,
        
        -- Floor (fx)
        MAX(CASE WHEN app.name = 'Floor' THEN r.score_final END) AS fx_score,
        MAX(CASE WHEN app.name = 'Floor' THEN r.score_d END) AS fx_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Floor' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Floor' THEN r.rank_text END)) AS fx_rank,
        
        -- Pommel Horse (ph)
        MAX(CASE WHEN app.name = 'Pommel Horse' THEN r.score_final END) AS ph_score,
        MAX(CASE WHEN app.name = 'Pommel Horse' THEN r.score_d END) AS ph_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Pommel Horse' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Pommel Horse' THEN r.rank_text END)) AS ph_rank,
        
        -- Rings (sr)
        MAX(CASE WHEN app.name = 'Rings' THEN r.score_final END) AS sr_score,
        MAX(CASE WHEN app.name = 'Rings' THEN r.score_d END) AS sr_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Rings' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Rings' THEN r.rank_text END)) AS sr_rank,
        
        -- Vault (vt)
        MAX(CASE WHEN app.name = 'Vault' THEN r.score_final END) AS vt_score,
        MAX(CASE WHEN app.name = 'Vault' THEN r.score_d END) AS vt_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Vault' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Vault' THEN r.rank_text END)) AS vt_rank,
        
        -- Parallel Bars (pb)
        MAX(CASE WHEN app.name = 'Parallel Bars' THEN r.score_final END) AS pb_score,
        MAX(CASE WHEN app.name = 'Parallel Bars' THEN r.score_d END) AS pb_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Parallel Bars' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Parallel Bars' THEN r.rank_text END)) AS pb_rank,
        
        -- High Bar (hb)
        MAX(CASE WHEN app.name = 'High Bar' THEN r.score_final END) AS hb_score,
        MAX(CASE WHEN app.name = 'High Bar' THEN r.score_d END) AS hb_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'High Bar' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'High Bar' THEN r.rank_text END)) AS hb_rank,
        
        -- All Around (aa)
        MAX(CASE WHEN app.name = 'All Around' THEN r.score_final END) AS aa_score,
        MAX(CASE WHEN app.name = 'All Around' THEN r.score_d END) AS aa_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'All Around' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'All Around' THEN r.rank_text END)) AS aa_rank
        
    FROM Results r
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    LEFT JOIN Clubs c ON a.club_id = c.club_id
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    WHERE r.gender = 'M'
    GROUP BY p.person_id, m.meet_db_id
    ORDER BY p.full_name, m.comp_year DESC;
    """
    
    # --- WAG TABLE ---
    wag_query = """
    CREATE TABLE Gold_Results_WAG AS
    SELECT
        p.full_name AS athlete_name,
        m.source AS source,
        m.comp_year AS year,
        m.name AS meet_name,
        MAX(r.level) AS level,
        MAX(r.age) AS age,
        c.name AS club,
        
        -- Vault (vt)
        MAX(CASE WHEN app.name = 'Vault' THEN r.score_final END) AS vt_score,
        MAX(CASE WHEN app.name = 'Vault' THEN r.score_d END) AS vt_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Vault' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Vault' THEN r.rank_text END)) AS vt_rank,
        
        -- Uneven Bars (ub)
        MAX(CASE WHEN app.name = 'Uneven Bars' THEN r.score_final END) AS ub_score,
        MAX(CASE WHEN app.name = 'Uneven Bars' THEN r.score_d END) AS ub_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Uneven Bars' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Uneven Bars' THEN r.rank_text END)) AS ub_rank,
        
        -- Beam (bb)
        MAX(CASE WHEN app.name = 'Beam' THEN r.score_final END) AS bb_score,
        MAX(CASE WHEN app.name = 'Beam' THEN r.score_d END) AS bb_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Beam' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Beam' THEN r.rank_text END)) AS bb_rank,
        
        -- Floor (fx)
        MAX(CASE WHEN app.name = 'Floor' THEN r.score_final END) AS fx_score,
        MAX(CASE WHEN app.name = 'Floor' THEN r.score_d END) AS fx_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'Floor' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'Floor' THEN r.rank_text END)) AS fx_rank,
        
        -- All Around (aa)
        MAX(CASE WHEN app.name = 'All Around' THEN r.score_final END) AS aa_score,
        MAX(CASE WHEN app.name = 'All Around' THEN r.score_d END) AS aa_d,
        COALESCE(CAST(MIN(CASE WHEN app.name = 'All Around' THEN r.rank_numeric END) AS TEXT), MAX(CASE WHEN app.name = 'All Around' THEN r.rank_text END)) AS aa_rank
        
    FROM Results r
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    LEFT JOIN Clubs c ON a.club_id = c.club_id
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    WHERE r.gender = 'F'
    GROUP BY p.person_id, m.meet_db_id
    ORDER BY p.full_name, m.comp_year DESC;
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
        logging.warning(f"Manifest not found: {filepath}")
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

def heal_meets_metadata(conn, kscore_manifest, livemeet_manifest, mso_manifest):
    """
    Backfills missing metadata (year, name, date, etc.) for all existing meets
    using the provided manifests. This is useful for meets that were processed
    before self-healing logic was added.
    """
    logging.info("Starting metadata healing pass...")
    cursor = conn.cursor()
    
    # Combined manifests for easier lookup
    # Key should be (source, source_meet_id)
    combined = {}
    for mid, details in kscore_manifest.items():
        # KScore source_meet_id in DB is without prefix
        sid = mid.replace('kscore_', '', 1)
        combined[('kscore', sid)] = details
    for mid, details in livemeet_manifest.items():
        combined[('livemeet', mid)] = details
    for mid, details in mso_manifest.items():
        combined[('mso', mid)] = details

    cursor.execute("SELECT meet_db_id, source, source_meet_id, comp_year, name, start_date_iso, location FROM Meets")
    meets = cursor.fetchall()
    
    updates_count = 0
    for m_id, source, sid, db_year, db_name, db_date, db_loc in meets:
        # Try lookup with original sid and with scraper prefix if needed
        details = combined.get((source, sid))
        if not details:
            # Fallback: maybe sid in DB is just the ID but manifest has the prefix
            prefixed_sid = f"{source}_{sid}"
            details = combined.get((source, prefixed_sid))
        
        if not details:
            # Extra Fallback: Check if manifest ID is a prefix of sid (e.g. for _PEREVENT_ files)
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
        if manifest_name and (not db_name or "Kscore" in db_name or "Livemeet" in db_name or db_name == 'Title not set'):
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
    parser.add_argument("--workers", type=int, default=8, help="Number of parallel readers")
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
    
    # HEAL METADATA FIRST
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        heal_meets_metadata(conn, kscore_manifest, livemeet_manifest, mso_manifest)

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
    # Also support *_PEREVENT_* and *_BYEVENT_*
    l_files += glob.glob(os.path.join(LIVEMEET_DIR, "*_PEREVENT_*.csv"))
    l_files += glob.glob(os.path.join(LIVEMEET_DIR, "*_BYEVENT_*.csv"))
    for f in l_files: files_to_process.append(('livemeet', f, livemeet_manifest.get(os.path.basename(f).split('_')[0], {}), None))
    
    # MSO
    m_files = glob.glob(os.path.join(MSO_DIR, "*_mso.csv"))
    for f in m_files: files_to_process.append(('mso', f, mso_manifest.get(os.path.basename(f).split('_mso.csv')[0], {}), None))

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
            # Pre-fetch processed hashes to avoid thousands of queries
            # This is much faster
            processed_map = {row[0]: True for row in conn.execute("SELECT file_hash FROM ProcessedFiles").fetchall()}
            
            for stype, fpath, manifest, aliases in files_to_process:
                # Calculate hash - this is fast for small files, but we can also optimize
                # by checking if path + mtime is unchanged, but let's stick to content hash or path check
                # For speed, strictly we should use path + mtime if we trust it, but hash is safer.
                # However, calculate_file_hash reads the file. That might be slow for 5000 files.
                # Let's trust the CalculateHash function or maybe check if we can skip reading?
                
                # Check 1: Is this path already in ProcessedFiles? (If hash unchanged)
                # Actually calculate_file_hash is robust.
                fhash = calculate_file_hash(fpath)
                
                if fhash not in processed_map:
                     unprocessed.append((stype, fpath, fhash, manifest, aliases))
                     # Check limit on the fly to avoid scanning everything if not needed?
                     # No, user wants the "next 100". Sorting ensures "next" is deterministic.
                     # But if we want proper "next 100", we should find the first 100 unprocessed.
                     if args.limit > 0 and len(unprocessed) >= args.limit:
                         break
    
    logging.info(f"Total files found: {len(files_to_process)}. New/Changed: {len(unprocessed)}")
    print(f"Total files found: {len(files_to_process)}. New to process: {len(unprocessed)}")
    
    if not unprocessed and not args.gold_only:
        logging.info("No unprocessed files found.")
    elif not args.gold_only:
        # Proceed with 'unprocessed' list
        # ... logic continues ...
        if not unprocessed:
            logging.info("No unprocessed files found.")
            # Even if no files, we might still want to refresh Gold tables later
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
            
            # Reduced batch size to minimize lock duration
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
                    # Submit all tasks
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
                        
                        # Terminal Progress (Concise)
                        print(f"[{stype} {completed}/{total}] {os.path.basename(fpath)}")
                        
                        try:
                            data_package = future.result()
                            if data_package:
                                write_to_db(conn, data_package, caches, club_aliases)
                                # Success! Mark as processed so we don't scan it again next time.
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

    # 5. Refresh Gold Tables
    # 5. Heal Metadata (AFTER loading files to catch new meets)
    logging.info("Running Final Metadata Healing Pass...")
    with sqlite3.connect(DB_FILE) as conn:
        heal_meets_metadata(conn, kscore_manifest, livemeet_manifest, mso_manifest)

    # 6. Refresh Gold Tables
    with sqlite3.connect(DB_FILE) as conn:
        refresh_gold_tables(conn)


    if not args.gold_only:
        logging.info(f"Finished! Processed {completed} files in {time.time() - start_time:.2f}s.")
    else:
        logging.info(f"Gold table refresh complete in {time.time() - start_time:.2f}s.")

if __name__ == "__main__":
    # Setup Logging
    logging.basicConfig(
        filename='loader_orchestrator.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING) # Only warnings/errors to console
    logging.getLogger('').addHandler(console)
    
    logging.info("Loader Orchestrator Started")
    main()
