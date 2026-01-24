import os
import sys
import subprocess
import pandas as pd
import glob
import time
import re
import contextlib
import shutil
import logging
import json
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from webdriver_manager.chrome import ChromeDriverManager

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

STATUS_MANIFEST = "scraped_meets_status.json"

WORKERS = {
    'kscore': 2,
    'livemeet': 3,
    'mso': 2
}

MAX_RETRIES = 3

# --- UTILS ---

def cleanup_orphaned_processes():
    """Force kill orphaned chrome and chromedriver processes."""
    print("  -> Cleaning up orphaned chrome/chromedriver processes...")
    try:
        # Use pkill if available for efficiency
        subprocess.run(["pkill", "-f", "chrome"], capture_output=True)
        subprocess.run(["pkill", "-f", "chromedriver"], capture_output=True)
        # Aggressive fallback
        subprocess.run(["killall", "-9", "chrome"], capture_output=True)
        subprocess.run(["killall", "-9", "chromedriver"], capture_output=True)
        time.sleep(2) # Allow system to release ports
    except:
        pass

def load_status():
    if os.path.exists(STATUS_MANIFEST):
        try:
            with open(STATUS_MANIFEST, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_status(status_dict):
    try:
        with open(STATUS_MANIFEST, 'w') as f:
            json.dump(status_dict, f, indent=4)
    except Exception as e:
        logging.error(f"Failed to save status manifest: {e}")

# --- WORKER FUNCTIONS ---

def kscore_task(meet_id, meet_name, driver_path=None):
    """Worker task for KScore scraping."""
    try:
        # Subtle staggered start to avoid resource spikes
        time.sleep(random.random() * 3)

        # with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
        success, count = kscore_scraper.scrape_kscore_meet(str(meet_id), str(meet_name), KSCORE_DIR, driver_path=driver_path)
        
        if success:
            return f"DONE: {meet_id}:{count}"
        else:
            return f"ERROR: {meet_id} (Scraper reported failure or partial data)"
    except Exception as e:
        return f"ERROR: {meet_id} ({e})"

def livemeet_task(meet_id, meet_name, driver_path=None):
    """Worker task for LiveMeet scraping and cleaning."""
    try:
        meet_url = f"https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id={meet_id}"
        
        # with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
        success, count, file_base_id = livemeet_scraper.scrape_raw_data_to_separate_files(meet_url, str(meet_id), LIVEMEET_MESSY_DIR, driver_path=driver_path)
            
        if success:
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
                if os.path.exists(finalized_path):
                    shutil.move(finalized_path, target_path)

        if success:
            return f"DONE: {meet_id}:{count}"
        else:
            return f"ERROR: {meet_id} (Scraper failed or results disabled)"
    except Exception as e:
        return f"ERROR: {meet_id} ({e})"

def mso_task(meet_id, meet_name, driver_path=None):
    """Worker task for MSO scraping."""
    driver = None
    try:
        # Subtle staggered start
        time.sleep(random.random() * 3)

        # with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
        driver = mso_scraper.setup_driver(driver_path=driver_path)
        # process_meet returns (success, message)
        success, msg = mso_scraper.process_meet(driver, str(meet_id), str(meet_name), 0, 0)
        
        if success:
            return f"DONE: {meet_id}:1" # MSO usually 1 file
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

def main():
    # Setup Logging
    logging.basicConfig(
        filename='scraper_orchestrator.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING) # Only show warnings/errors to console
    logging.getLogger('').addHandler(console)

    print("--- Scraper Orchestrator Started (Detailed logs in 'scraper_orchestrator.log') ---")

    # Load Status Manifest
    status_manifest = load_status()

    # Ensure directories exist
    for d in [KSCORE_DIR, LIVEMEET_MESSY_DIR, LIVEMEET_FINAL_DIR, MSO_DIR]:
        os.makedirs(d, exist_ok=True)

    # PRE-INSTALL WEBDRIVER
    print("Pre-installing/checking WebDriver...")
    # Force clear cache if needed or just trust manager
    # os.environ['WDM_LOG_LEVEL'] = '0' 
    valid_driver_path = None
    try:
        # from webdriver_manager.core.utils import ChromeType
        # Explicitly requesting the version matching the installed browser if accessible,
        # but usually .install() handles this. The ERROR says 114 vs 144.
        # Let's try to print what we are getting.
        valid_driver_path = ChromeDriverManager().install()
        print(f"WebDriver installed at: {valid_driver_path}")
    except Exception as e:
        print(f"Warning: WebDriver pre-install failed: {e}")

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

    # INTERLEAVE SOURCES: Shuffle the list so we process a mix of KScore, LiveMeet, and MSO
    random.shuffle(all_tasks)

    # Filter out already finished tasks
    queue = [t for t in all_tasks if status_manifest.get(f"{t[0]}_{t[1]}") != "DONE"]
    
    logging.info(f"Total tasks loaded: {len(all_tasks)}. Remaining: {len(queue)}")
    print(f"Total tasks loaded: {len(all_tasks)}. Remaining to process: {len(queue)}")
    
    # Graceful Shutdown Handling
    import signal
    stop_requested = False
    def signal_handler(sig, frame):
        nonlocal stop_requested
        print("\n\n!!! SHUTDOWN REQUESTED... !!!\n")
        stop_requested = True
        # Aggressive cleanup on first signal
        try:
             # Try clean shutdown first
             print("Initiating emergency pool shutdown...")
        except:
             pass

    signal.signal(signal.SIGINT, signal_handler)

    task_functions = {
        'kscore': kscore_task,
        'livemeet': livemeet_task,
        'mso': mso_task
    }

    with ProcessPoolExecutor(max_workers=WORKERS['kscore']) as k_pool, \
         ProcessPoolExecutor(max_workers=WORKERS['livemeet']) as l_pool, \
         ProcessPoolExecutor(max_workers=WORKERS['mso']) as m_pool:
        
        pools = {
            'kscore': k_pool,
            'livemeet': l_pool,
            'mso': m_pool
        }

        # Multi-attempt logic
        for attempt in range(1, MAX_RETRIES + 1):
            if stop_requested or not queue:
                break
                
            print(f"\n--- ATTEMPT {attempt}/{MAX_RETRIES} ---")
            logging.info(f"Starting attempt {attempt}/{MAX_RETRIES}")
            
            # Resource cleanup before each attempt
            cleanup_orphaned_processes()
            
            # Chunk the queue for incremental processing based on file count
            # User Request: 1000 CSVs -> 1 load
            # We process in small chunks of meets to check the file count frequently
            MEET_CHUNK_SIZE = 50 
            CSV_BATCH_THRESHOLD = 1000
            
            current_csv_count = 0
            
            for i in range(0, len(queue), MEET_CHUNK_SIZE):
                if stop_requested:
                    break
                    
                chunk = queue[i : i + MEET_CHUNK_SIZE]
                
                print(f"\nProcessing Checkpoint ({i}/{len(queue)} meets)... Current CSV Batch: {current_csv_count}/{CSV_BATCH_THRESHOLD}")
                
                futures = {}
                for stype, mid, mname in chunk:
                    pool = pools[stype]
                    func = task_functions[stype]
                    
                    # Pass driver_path to all Selenium-based tasks
                    futures[pool.submit(func, mid, mname, valid_driver_path)] = (stype, mid, mname)
                
                # Wait for this chunk to complete
                for future in as_completed(futures):
                    if stop_requested:
                        # Cancel remaining futures in this chunk
                        for f in futures: f.cancel()
                        break

                    stype, mid, mname = futures[future]
                    key = f"{stype}_{mid}"
                    
                    try:
                        result = future.result()
                        parts = result.split(':', 2) # Expect DONE:mid:count or ERROR:mid:msg
                        status = parts[0]
                        
                        message = ""
                        count = 0
                        
                        if "DONE" in status:
                            if len(parts) >= 3:
                                mid_res = parts[1]
                                try:
                                    count = int(parts[2])
                                except:
                                    count = 0
                                message = f"{count} files"
                            else:
                                message = "0 files" # Should not happen with new logic
                                
                            status_manifest[key] = "DONE"
                            current_csv_count += count
                            logging.info(f"[{stype}] {mid}: {status} - {message}")
                            print(f"  [OK] {mid} ({count} files) - Progress: {len(status_manifest)} total meets scraped")
                            
                        else:
                            # ERROR logic
                            message = parts[1] if len(parts) > 1 else "Unknown Error"
                            logging.error(f"  [FAIL] {mid}: {message}")
                            
                    except Exception as e:
                        logging.error(f"  [EXCEPTION] {mid}: {e}")
                
                # Update status after chunk
                save_status(status_manifest)
                
                # CHECK IF WE HIT THE CSV THRESHOLD
                if current_csv_count >= CSV_BATCH_THRESHOLD and not stop_requested:
                    print(f"\n>>> Batch Threshold Hit ({current_csv_count} >= {CSV_BATCH_THRESHOLD} CSVs). Running Loader... <<<")
                    try:
                        subprocess.run([sys.executable, "load_orchestrator.py"], check=False)
                        current_csv_count = 0 # Reset counter after load
                        print(">>> Loader Complete. Resuming Scraper... <<<")
                    except Exception as e:
                        logging.error(f"Failed to run incremental load: {e}")

            # Prepare queue for next attempt (retry failed items)
            queue = [t for t in queue if status_manifest.get(f"{t[0]}_{t[1]}") != "DONE"]
            
            if queue and attempt < MAX_RETRIES:
                jitter = random.randint(5, 15)
                print(f"Waiting {jitter}s before next retry attempt...")
                time.sleep(jitter)

    logging.info("Scraper Orchestration finished.")
    print("\n--- Scraper Orchestration Finished ---")

if __name__ == "__main__":
    main()
