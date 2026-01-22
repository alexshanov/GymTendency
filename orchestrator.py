import os
import sys
import pandas as pd
import glob
import time
import re
import contextlib
import shutil
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

# Import Scrapers
import kscore_scraper
import livemeet_scraper
import mso_scraper

# --- CONFIGURATION ---
KSCORE_CSV = "discovered_meet_ids_kscore.csv"
LIVEMEET_CSV = "discovered_meet_ids_livemeet.csv"
MSO_CSV = "discovered_meet_ids_mso.csv"

KSCORE_DIR = "CSVs_kscore_final"
LIVEMEET_MESSY_DIR = "CSVs_Livemeet_messy"
LIVEMEET_FINAL_DIR = "CSVs_Livemeet_final"
MSO_DIR = "CSVs_mso_final"

WORKERS = {
    'kscore': 2,
    'livemeet': 8,
    'mso': 8  # Reduced from 15 to prevent connection exhaustion
}

# --- WORKER FUNCTIONS ---

def kscore_task(meet_id, meet_name):
    """Worker task for KScore scraping."""
    try:
        # Check skip logic again within worker to be safe in parallel
        existing = glob.glob(os.path.join(KSCORE_DIR, f"{meet_id}_FINAL_*.csv"))
        if existing:
            return f"SKIP: {meet_id}"

        with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            kscore_scraper.scrape_kscore_meet(str(meet_id), str(meet_name), KSCORE_DIR)
        return f"DONE: {meet_id}"
    except Exception as e:
        return f"ERROR: {meet_id} ({e})"

def livemeet_task(meet_id, meet_name):
    """Worker task for LiveMeet scraping and cleaning."""
    try:
        existing = glob.glob(os.path.join(LIVEMEET_FINAL_DIR, f"{meet_id}_FINAL_*.csv"))
        if existing:
            return f"SKIP: {meet_id}"

        meet_url = f"https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id={meet_id}"
        
        with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            files_saved, file_base_id = livemeet_scraper.scrape_raw_data_to_separate_files(meet_url, str(meet_id), LIVEMEET_MESSY_DIR)
            
            if files_saved > 0:
                # Process messy files
                search_pattern_messy = os.path.join(LIVEMEET_MESSY_DIR, f"{file_base_id}_MESSY_*.csv")
                for messy_path in glob.glob(search_pattern_messy):
                    messy_filename = os.path.basename(messy_path)
                    final_filename = messy_filename.replace('_MESSY_', '_FINAL_')
                    final_path = os.path.join(LIVEMEET_FINAL_DIR, final_filename)
                    livemeet_scraper.fix_and_standardize_headers(messy_path, final_path)
                
                # Process finalized files (PEREVENT and BYEVENT)
                search_pattern_final = os.path.join(LIVEMEET_MESSY_DIR, f"{file_base_id}_*EVENT_*.csv")
                for finalized_path in glob.glob(search_pattern_final):
                    final_filename = os.path.basename(finalized_path)
                    target_path = os.path.join(LIVEMEET_FINAL_DIR, final_filename)
                    shutil.move(finalized_path, target_path)

        return f"DONE: {meet_id}"
    except Exception as e:
        return f"ERROR: {meet_id} ({e})"

def mso_task(meet_id, meet_name):
    """Worker task for MSO scraping."""
    driver = None
    try:
        existing = os.path.join(MSO_DIR, f"{meet_id}_mso.csv")
        if os.path.exists(existing):
            return f"SKIP: {meet_id}"

        with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            driver = mso_scraper.setup_driver()
            # process_meet returns (success, message)
            success, msg = mso_scraper.process_meet(driver, str(meet_id), str(meet_name), 0, 0)
        
        if success:
            return f"DONE: {meet_id}"
        else:
            return f"ERROR: {meet_id} ({msg})"
    except Exception as e:
        return f"ERROR: {meet_id} ({e})"
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

# --- MAIN ORCHESTRATOR ---

def run_scraper(scraper_type, manifest_path, task_func, max_workers):
    # (This function is not actually used in main() anymore, but keeping consistent if needed later)
    pass 

def main():
    # Setup Logging
    logging.basicConfig(
        filename='scraper_orchestrator.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING) # Only show warnings/errors to console
    logging.getLogger('').addHandler(console)

    print("--- Scraper Orchestrator Started (Detailed logs in 'scraper_orchestrator.log') ---")

    # Ensure directories exist
    for d in [KSCORE_DIR, LIVEMEET_MESSY_DIR, LIVEMEET_FINAL_DIR, MSO_DIR]:
        os.makedirs(d, exist_ok=True)

    all_tasks = []
    
    # Load KScore
    if os.path.exists(KSCORE_CSV):
        df = pd.read_csv(KSCORE_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            all_tasks.append(('kscore', str(row[id_col]), str(row[name_col])))

    # Load LiveMeet
    if os.path.exists(LIVEMEET_CSV):
        df = pd.read_csv(LIVEMEET_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            all_tasks.append(('livemeet', str(row[id_col]), str(row[name_col])))

    # Load MSO
    if os.path.exists(MSO_CSV):
        df = pd.read_csv(MSO_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            all_tasks.append(('mso', str(row[id_col]), str(row[name_col])))

    logging.info(f"Total tasks loaded: {len(all_tasks)}")
    print(f"Total tasks loaded: {len(all_tasks)}")
    
    with ProcessPoolExecutor(max_workers=WORKERS['kscore']) as k_pool, \
         ProcessPoolExecutor(max_workers=WORKERS['livemeet']) as l_pool, \
         ProcessPoolExecutor(max_workers=WORKERS['mso']) as m_pool:
        
        futures = {}
        
        # Submit KScore
        k_tasks = [t for t in all_tasks if t[0] == 'kscore']
        for _, mid, mname in k_tasks:
            futures[k_pool.submit(kscore_task, mid, mname)] = (mid, 'kscore')
            
        # Submit LiveMeet
        l_tasks = [t for t in all_tasks if t[0] == 'livemeet']
        for _, mid, mname in l_tasks:
            futures[l_pool.submit(livemeet_task, mid, mname)] = (mid, 'livemeet')
            
        # Submit MSO
        m_tasks = [t for t in all_tasks if t[0] == 'mso']
        for _, mid, mname in m_tasks:
            futures[m_pool.submit(mso_task, mid, mname)] = (mid, 'mso')
            
        completed = { 'kscore': 0, 'livemeet': 0, 'mso': 0 }
        totals = { 'kscore': len(k_tasks), 'livemeet': len(l_tasks), 'mso': len(m_tasks) }
        
        for future in as_completed(futures):
            mid, stype = futures[future]
            completed[stype] += 1
            try:
                result = future.result()
                
                parts = result.split(':', 1)
                status = parts[0]
                message = parts[1].strip() if len(parts) > 1 else ""
                
                log_msg = f"[{stype} {completed[stype]}/{totals[stype]}] {mid}: {status} - {message}"
                
                if "ERROR" in status:
                     logging.error(log_msg)
                     # Optional: print error to stdout too, or keep it silent as requested
                elif "SKIP" in status:
                     logging.info(log_msg)
                else:
                     logging.info(log_msg)

            except Exception as e:
                err_msg = f"[{stype} {completed[stype]}/{totals[stype]}] {mid}: EXCEPTION ({e})"
                logging.error(err_msg)

    logging.info("Orchestrator finished.")


if __name__ == "__main__":
    main()
