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
import threading
import sqlite3
import etl_functions # for hash calculation

# Import Scrapers
import kscore_scraper
import livemeet_scraper
import mso_scraper
import ksis_scraper

# --- CONFIGURATION ---
KSCORE_CSV = "discovered_meet_ids_kscore.csv"
LIVEMEET_CSV = "discovered_meet_ids_livemeet.csv"
MSO_CSV = "discovered_meet_ids_mso.csv"
KSIS_CSV = "discovered_meet_ids_ksis.csv"

KSCORE_DIR = "CSVs_kscore_final"
LIVEMEET_MESSY_DIR = "CSVs_Livemeet_messy"
LIVEMEET_FINAL_DIR = "CSVs_Livemeet_final"
MSO_DIR = "CSVs_mso_final"
KSIS_DIR = "CSVs_ksis_messy"

STATUS_MANIFEST = "scraped_meets_status.json"

WORKERS = {
    'kscore': 2,
    'livemeet': 3,
    'mso': 2,
    'ksis': 2
}

MAX_RETRIES = 3

def ksis_task(meet_id, meet_name, driver_path=None):
    """Worker task for KSIS scraping."""
    import ksis_scraper  # Local import for worker safety
    try:
        # Subtle staggered start
        time.sleep(random.random() * 3)
        
        # KSIS is requests-based, doesn't use Selenium driver
        success = ksis_scraper.scrape_ksis_meet(str(meet_id), str(meet_name), KSIS_DIR)
        
        # ksis_scraper.scrape_ksis_meet returns boolean
        if success:
            # Return '1' so it parses correctly as an int for the batch counter
            return f"DONE: {meet_id}:1"
        else:
            return f"ERROR: {meet_id} (Scraper failed)"
    except Exception as e:
        return f"ERROR: {meet_id} ({e})"



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

def export_status_csv(status_dict):
    """
    Exports the status dictionary to a human-readable CSV.
    """
    csv_file = "scraped_meets_log.csv"
    try:
        # Convert dict to list of dicts for DataFrame
        data = []
        for key, status in status_dict.items():
            # key format: "{type}_{id}"
            parts = key.split('_', 1)
            if len(parts) == 2:
                stype, mid = parts
                status_val = status
                mname = "Unknown"
                if isinstance(status, dict):
                    status_val = status.get('status', 'DONE')
                    mname = status.get('name', 'Unknown')
                
                data.append({'Type': stype, 'MeetID': mid, 'MeetName': mname, 'Status': status_val})
        
        if data:
            df = pd.DataFrame(data)
            df.to_csv(csv_file, index=False)
    except Exception as e:
        logging.error(f"Failed to export status CSV: {e}")

def save_status(status_dict):
    try:
        with open(STATUS_MANIFEST, 'w') as f:
            json.dump(status_dict, f, indent=4)
        # Also export CSV
        export_status_csv(status_dict)
    except Exception as e:
        logging.error(f"Failed to save status manifest: {e}")

# --- WORKER FUNCTIONS ---

def kscore_task(meet_id, meet_name, driver_path=None):
    """Worker task for KScore scraping."""
    import kscore_scraper # Local import for worker safety
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
    import livemeet_scraper # Local import for worker safety
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
    import mso_scraper # Local import for worker safety
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

def is_high_priority(meet_type, meet_name, location=''):
    """
    Determines if a meet matches the High Priority criteria based on audio instructions.
    """
    n = str(meet_name).upper()
    l = str(location).upper()
    
    if meet_type == 'kscore':
        return 'ED VINCENT' in n
        
    if meet_type == 'livemeet':
        # Audio: Grizzly Classic
        if 'GRIZZLY CLASSIC' in n: return True
        # Match AG or Artistic meets specifically
        if any(x in n for x in ['AG ', 'ARTISTIC']):
             if any(x in n for x in ['TG ', 'T&T', 'T & T']): return False
             return True
        # Audio: AG Canadian Championships (Exclude TNT)
        if 'CANADIAN CHAMPIONSHIP' in n:
             if any(x in n for x in ['TG ', 'T&T', 'T & T']): return False
             return True
        # Audio: Elite
        if 'ELITE' in n:
             if any(x in n for x in ['TG ', 'T&T', 'T & T']): return False
             return True
        # Audio: Westerns
        if 'WESTERN' in n: 
             if any(x in n for x in ['TG ', 'T&T', 'T & T']): return False
             return True
        # Audio: Alberta Provincials
        if 'PROVINCIAL' in n:
             # Match Alberta/AB in name or location
             if any(x in n for x in ['ALBERTA']) or any(x in l for x in ['ALBERTA', ', AB', ' AB']):
                  if any(x in n for x in ['TG ', 'T&T', 'T & T']): return False
                  return True
        return False
        
    if meet_type == 'ksis':
        if 'ON CUP' in n:
            return True

    if meet_type == 'mso':
        return True # Handled manually in all_tasks
        
    return False

class StatusHeartbeat(threading.Thread):
    def __init__(self, stop_event, get_remaining_meets, get_pending_csvs, interval=30):
        super().__init__()
        self.stop_event = stop_event
        self.get_remaining_meets = get_remaining_meets
        self.get_pending_csvs = get_pending_csvs
        self.interval = interval
        self.daemon = True

    def run(self):
        while not self.stop_event.is_set():
            # Wait for interval or stop event
            if self.stop_event.wait(self.interval):
                break
            
            rem_meets = self.get_remaining_meets()
            pending_csvs = self.get_pending_csvs()
            print(f"\n[HEARTBEAT] Meets remaining: {rem_meets} | CSVs waiting for load: {pending_csvs}")

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

    # Ensure Database exists (Load once if missing)
    if not os.path.exists("gym_data.db"):
        print("\n>>> Database missing! Triggering initial load of existing CSVs... <<<")
        try:
            subprocess.run([sys.executable, "load_orchestrator.py"], check=False)
        except Exception as e:
            print(f"Warning: Initial load failed: {e}")

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
        loc_col = [c for c in df.columns if 'Location' in c][0]
        for _, row in df.iterrows():
            all_tasks.append(('livemeet', str(row[id_col]), str(row[name_col]), str(row[loc_col])))

    # Load MSO (PAUSED)
    if False and os.path.exists(MSO_CSV):
        df = pd.read_csv(MSO_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            all_tasks.append(('mso', str(row[id_col]), str(row[name_col])))

    # Manual Injection: 2025 Mens HNI & Vegas Cup 2025
    all_tasks.append(('mso', '33704', '2025 Mens HNI'))
    all_tasks.append(('mso', '33619', 'Vegas Cup 2025 - Men'))

    # Load KSIS
    if os.path.exists(KSIS_CSV):
        df = pd.read_csv(KSIS_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            all_tasks.append(('ksis', str(row[id_col]), str(row[name_col])))

    # SPLIT INTO HIGH AND LOW PRIORITY
    high_priority_tasks = []
    low_priority_tasks = []

    for t in all_tasks:
        if len(t) == 4:
            m_type, m_id, m_name, m_loc = t
        else:
            m_type, m_id, m_name = t
            m_loc = ''
            
        if is_high_priority(m_type, m_name, m_loc):
            high_priority_tasks.append((m_type, m_id, m_name))
        else:
            low_priority_tasks.append((m_type, m_id, m_name))

    # Randomize within groups
    random.shuffle(high_priority_tasks)
    random.shuffle(low_priority_tasks)
    
    # Combined Queue: High Priority FIRST
    final_queue_list = high_priority_tasks + low_priority_tasks
    
    # Filter out already finished tasks
    def get_status_simple(key):
        val = status_manifest.get(key)
        if isinstance(val, dict):
            return val.get('status')
        return val

    queue = [t for t in final_queue_list if get_status_simple(f"{t[0]}_{t[1]}") != "DONE"]
    
    logging.info(f"Total tasks loaded: {len(all_tasks)}. Remaining: {len(queue)}")
    print(f"Total tasks loaded: {len(all_tasks)}. Remaining to process: {len(queue)}")
    print(f"  > High Priority Workload: {len([t for t in queue if is_high_priority(t[0], t[2])])}")

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
        'mso': mso_task,
        'ksis': ksis_task
    }

    with ProcessPoolExecutor(max_workers=WORKERS['kscore']) as k_pool, \
         ProcessPoolExecutor(max_workers=WORKERS['livemeet']) as l_pool, \
         ProcessPoolExecutor(max_workers=WORKERS['mso']) as m_pool, \
         ProcessPoolExecutor(max_workers=WORKERS['ksis']) as ksis_pool:
        
        pools = {
            'kscore': k_pool,
            'livemeet': l_pool,
            'mso': m_pool,
            'ksis': ksis_pool
        }

        # Start Heartbeat Thread
        heartbeat_stop = threading.Event()
        
        # Start Heartbeat Thread
        heartbeat_stop = threading.Event()
        
        def count_pending_csvs():
            """
            Counts CSVs that are on disk BUT not yet in the ProcessedFiles table.
            """
            try:
                # 1. Get set of all files on disk
                files_on_disk = []
                for d in [KSCORE_DIR, LIVEMEET_FINAL_DIR, MSO_DIR, KSIS_DIR]:
                    if os.path.exists(d):
                        files_on_disk.extend(glob.glob(os.path.join(d, "*.csv")))
                
                if not files_on_disk:
                    return 0

                # 2. Get set of processed file hashes from DB
                # We can't easily query by filename because paths might differ, so we use hash or just filename if unique enough.
                # etl_functions uses hash. Calculating hash for 1000s of files every 30s is too heavy.
                # optimization: Check by filename match first?
                # ProcessedFiles table has file_path (might be absolute or relative) and file_hash.
                
                # Check DB access
                if not os.path.exists("gym_data.db"):
                    return len(files_on_disk)

                with sqlite3.connect("gym_data.db") as conn:
                    # We'll just count how many matches. 
                    # Actually, calculating hashes is slow. 
                    # Let's approximate: 
                    # Total Files on Disk - Total Files in ProcessedFiles that match the directory pattern?
                    # No, that's inaccurate if we delete files.
                    
                    # PROPOSAL:
                    # For the heartbeat, strict accuracy is less important than trend.
                    # BUT the user specifically asked for "current CSV load counter to be loaded".
                    # Let's rely on the Loader's logic: it checks ProcessedFiles.
                    # We can fetch ALL processed hashes once, then update? No, existing loader runs.
                    
                    # Fast proxy: CHECK FILE MODIFICATION TIME vs DB Last Processed?
                    # Too complex.
                    
                    # Let's just return Total Files found in the folders. 
                    # The Loader *moves* or *deletes* files? No, it keeps them.
                    # Ah, if the loader keeps them, then "files waiting for load" = Total Files - Processed Files.
                    
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM ProcessedFiles")
                    processed_count = cursor.fetchone()[0]
                    
                    total_on_disk = len(files_on_disk)
                    
                    # This is an approximation but standard "Pending" calculation
                    # Pending = Total found - Processed
                    # It might be negative if we processed files that are now deleted, but that's rare here.
                    pending = max(0, total_on_disk - processed_count)
                    return f"{pending} (Approx)"
            except Exception as e:
                return "Err"

        heartbeat = StatusHeartbeat(
            heartbeat_stop, 
            get_remaining_meets=lambda: len(queue),
            get_pending_csvs=count_pending_csvs
        )
        heartbeat.start()

        try:
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
                    
                    meets_rem = len(queue) - i
                    print(f"\n[PROGRESS] Meets remaining in this attempt: {meets_rem} | CSVs waiting for next load: {current_csv_count}/{CSV_BATCH_THRESHOLD}")
                    
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
                                    
                                status_manifest[key] = {"status": "DONE", "name": mname}
                                save_status(status_manifest) # Save immediately
                                current_csv_count += count
                                logging.info(f"[{stype}] {mid}: {status} - {message}")
                                print(f"  [OK] {mid} ({count} files) | Pending CSVs: {current_csv_count} | Meets Scraped: {len(status_manifest)}/{len(all_tasks)}")
                                
                                # CHECK IF WE HIT THE CSV THRESHOLD (inside loop for immediate trigger)
                                if current_csv_count >= CSV_BATCH_THRESHOLD and not stop_requested:
                                    print(f"\n>>> Batch Threshold Hit ({current_csv_count} >= {CSV_BATCH_THRESHOLD} CSVs). Running Loader... <<<")
                                    try:
                                        subprocess.run([sys.executable, "load_orchestrator.py"], check=False)
                                        current_csv_count = 0 # Reset counter after load
                                        print(">>> Loader Complete. Resuming Scraper... <<<")
                                    except Exception as e:
                                        logging.error(f"Failed to run incremental load: {e}")
                                
                            else:
                                # ERROR logic
                                message = parts[1] if len(parts) > 1 else "Unknown Error"
                                logging.error(f"  [FAIL] {mid}: {message}")
                                
                        except Exception as e:
                            logging.error(f"  [EXCEPTION] {mid}: {e}")
                    
                    # Update status after chunk
                    save_status(status_manifest)
    
                # Prepare queue for next attempt (retry failed items)
                queue = [t for t in queue if get_status_simple(f"{t[0]}_{t[1]}") != "DONE"]
                
                if queue and attempt < MAX_RETRIES:
                    jitter = random.randint(5, 15)
                    print(f"Waiting {jitter}s before next retry attempt...")
                    time.sleep(jitter)
        finally:
            heartbeat_stop.set()

    remaining_count = len([t for t in all_tasks if get_status_simple(f"{t[0]}_{t[1]}") != "DONE"])
    msg = f"Scraper Orchestration finished. Remaining tasks: {remaining_count}"
    logging.info(msg)
    print(f"\n--- {msg} ---")
    if remaining_count > 0:
        print(f"Warning: {remaining_count} tasks could not be completed after {MAX_RETRIES} attempts.")
    
    print("\nTriggering final load...")
    try:
        subprocess.run([sys.executable, "load_orchestrator.py"], check=False)
    except Exception as e:
        logging.error(f"Final load failed: {e}")

if __name__ == "__main__":
    main()
