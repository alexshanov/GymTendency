import sqlite3
import pandas as pd
import glob
import os
import json
import logging
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configuration
DB_PATH = "gym_data.db"
KSCORE_DIR = "CSVs_kscore_final"
LIVEMEET_DIR = "CSVs_Livemeet_final"
MSO_DIR = "CSVs_mso_final"
KSIS_DIR = "CSVs_cases_final"

# Target Athletes
TARGET_ATHLETES = {
    "Samuel Smith": ["Sam Smith", "Smith Samuel"],
    "Theo Freeman": ["Freeman Theo"],
    "Thoren Lawrence": ["Lawrence Thoren"],
    "Daxton Hull": ["Hull Daxton"],
    "Elian Tong": ["Tong Elian"],
    "Evan Hachey": ["Hachey Evan"],
    "George Pettigrew": ["Pettigrew George"],
    "Grayson Gutsell Vander Meulen": ["Grayson Gutsell-Vander Meulen", "Gutsell Vander Meulen Grayson", "Gutsell-Vander Meulen Grayson", "Grayson Gutsell"],
    "Isaac Hoyem": ["Hoyem Isaac"],
    "Isaiah Flack": ["Flack Isaiah"]
}

def load_aliases():
    try:
        with open("person_aliases.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def normalize_name(name):
    if pd.isna(name): return ""
    return str(name).strip().title()

def calculate_file_hash(filepath):
    """Calculates MD5 hash of a file."""
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def scan_file_worker(fpath, search_terms_map, parts_map):
    """
    Worker function to scan a single file for MULTIPLE athletes at once.
    Returns: {athlete_name: count} for found athletes, plus file metadata.
    """
    results = {}
    file_hash = calculate_file_hash(fpath)
    
    try:
        # Optimization: Read only potentially relevant columns if headers are standard?
        # Headers vary too much, so we read full file but skip bad lines.
        try:
            df = pd.read_csv(fpath, encoding='utf-8', on_bad_lines='skip', low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(fpath, encoding='latin1', on_bad_lines='skip', low_memory=False)
        
        if df.empty: return (fpath, file_hash, {})

        # Find name column
        name_col = None
        possible_cols = ['Name', 'Gymnast', 'Athlete', 'Competitor', 'Full Name']
        for col in df.columns:
            if any(p.lower() in col.lower() for p in possible_cols):
                name_col = col
                break
        
        if not name_col:
             if 'First Name' in df.columns and 'Last Name' in df.columns:
                 df['Full_Name_Temp'] = df['First Name'].astype(str) + " " + df['Last Name'].astype(str)
                 name_col = 'Full_Name_Temp'
             elif 'FirstName' in df.columns and 'LastName' in df.columns:
                 df['Full_Name_Temp'] = df['FirstName'].astype(str) + " " + df['LastName'].astype(str)
                 name_col = 'Full_Name_Temp'
        
        if not name_col:
             return (fpath, file_hash, {})

        df['norm_name'] = df[name_col].apply(normalize_name)
        
        # Check for each athlete
        for athlete, terms in search_terms_map.items():
            # A) Exact
            mask = df['norm_name'].isin(terms)
            
            # B) Fuzzy
            if not mask.any():
                first, last = parts_map[athlete]
                if first and last:
                     def strict_fuzzy(val):
                         v = val.lower()
                         return (first.lower() in v) and (last.lower() in v)
                     mask = df['norm_name'].apply(strict_fuzzy)
            
            if mask.any():
                results[athlete] = int(mask.sum())

    except Exception:
        pass
        
    return (fpath, file_hash, results)

def run_audit():
    print("Optimization: Scanning files in parallel and checking DB load status...")
    
    # 1. Load ProcessedFiles Hash Map
    conn = sqlite3.connect(DB_PATH)
    processed_hashes = set(row[0] for row in conn.execute("SELECT file_hash FROM ProcessedFiles").fetchall())
    conn.close()
    print(f"Loaded {len(processed_hashes)} processed file hashes from DB.")

    # 2. Prepare Match Data
    search_terms_map = {}
    parts_map = {}
    for athlete, aliases in TARGET_ATHLETES.items():
        search_terms_map[athlete] = set([normalize_name(athlete)] + [normalize_name(a) for a in aliases])
        p = athlete.split()
        parts_map[athlete] = (p[0], " ".join(p[1:]) if len(p)>1 else "")

    # 3. Collect Files
    all_files = []
    for d in [KSCORE_DIR, LIVEMEET_DIR, MSO_DIR, KSIS_DIR]:
        all_files.extend(glob.glob(os.path.join(d, "*.csv")))
    
    print(f"Scanning {len(all_files)} files for {len(TARGET_ATHLETES)} athletes...", flush=True)

    # 4. Parallel Scan
    # Map: filename -> {athlete: count, hash: val, is_loaded: bool}
    file_map = {} 
    
    # Use fewer workers to avoid thrashing
    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(scan_file_worker, f, search_terms_map, parts_map): f for f in all_files}
        
        count = 0
        for future in as_completed(futures):
            count += 1
            if count % 500 == 0: 
                print(f"Scanned {count}/{len(all_files)} files...", flush=True)
            
            try:
                fpath, fhash, matches = future.result()
                if matches:
                     is_loaded = fhash in processed_hashes
                     file_map[os.path.basename(fpath)] = {
                         'matches': matches,
                         'is_loaded': is_loaded,
                         'path': fpath
                     }
            except Exception as e:
                print(f"Error processing file: {e}", flush=True)

    print("Scan complete. Analyzing results...", flush=True)

    # 5. DB Check
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    with open("audit_report.txt", "w") as report_file:
        for athlete in TARGET_ATHLETES:
            print(f"Generating report for {athlete}...", flush=True)
            report_file.write(f"\n{'='*60}\n")
            report_file.write(f"REPORT: {athlete}\n")
            report_file.write(f"{'='*60}\n")
            
            # Files found
            relevant_files = [f for f, data in file_map.items() if athlete in data['matches']]
            
            # DB Meets
            cursor.execute("""
                SELECT m.name, count(*) 
                FROM Results r
                JOIN Athletes a ON r.athlete_id = a.athlete_id
                JOIN Persons p ON a.person_id = p.person_id
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                WHERE p.full_name = ?
                GROUP BY m.name
            """, (athlete,))
            db_meets = {row[0]: row[1] for row in cursor.fetchall()}
            
            report_file.write(f"[FOUND IN {len(relevant_files)} CSV FILES]\n")
            
            missing_loads = []
            
            for fname in relevant_files:
                data = file_map[fname]
                row_count = data['matches'][athlete]
                status = "LOADED" if data['is_loaded'] else "NOT LOADED"
                
                if not data['is_loaded']:
                    missing_loads.append(fname)
                    report_file.write(f"  [MISSING] {fname} ({row_count} rows) -> STATUS: {status}\n")
    
            report_file.write(f"\n[FOUND IN {len(db_meets)} DB MEETS]\n")
            for m, c in db_meets.items():
                report_file.write(f"  [DB] {m} ({c} rows)\n")
                
            if missing_loads:
                report_file.write(f"\n>>> ACTION REQUIRED: {len(missing_loads)} files containing {athlete} were NOT loaded into the DB.\n")
            else:
                report_file.write(f"\n>>> STATUS OK: All source files containing {athlete} appear to be loaded.\n")

    conn.close()
    print("Report saved to audit_report.txt", flush=True)

if __name__ == "__main__":
    run_audit()
