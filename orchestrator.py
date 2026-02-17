import os
import sys
import traceback
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
MAX_FAILURES = 5 # Permanent failure threshold
POLL_INTERVAL = 60  # Seconds to wait before re-checking for new tasks
CSV_BATCH_THRESHOLD = 300  # Trigger loader after this many CSVs scraped

def ksis_task(meet_id, meet_name, driver_path=None):
    """Worker task for KSIS scraping."""
    import ksis_scraper  # Local import for worker safety
    import glob
    try:
        # CRASH PROTECTION: Delete existing files for this meet to ensure a fresh start
        for f in glob.glob(os.path.join(KSIS_DIR, f"{meet_id}_*.csv")):
            try: os.remove(f)
            except: pass

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
    import glob
    try:
        # CRASH PROTECTION: Delete existing files for this meet to ensure a fresh start
        for f in glob.glob(os.path.join(KSCORE_DIR, f"{meet_id}_*.csv")):
            try: os.remove(f)
            except: pass

        # Subtle staggered start to avoid resource spikes
        time.sleep(random.random() * 3)

        # with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
        success, count = kscore_scraper.scrape_kscore_meet(str(meet_id), str(meet_name), KSCORE_DIR, driver_path=driver_path)
        
        if success and count > 0:
            return f"DONE: {meet_id}:{count}"
        else:
            return f"ERROR: {meet_id} (0 files scraped, avoiding false positive DONE)"
    except Exception as e:
        return f"ERROR: {meet_id} ({e})"

def livemeet_task(meet_id, meet_name, driver_path=None):
    """Worker task for LiveMeet scraping and cleaning."""
    import livemeet_scraper # Local import for worker safety
    import glob
    try:
        # CRASH PROTECTION: Delete existing files for this meet to ensure a fresh start
        # Livemeet files have complex names, we usually use the meet_id as the base
        patterns = [
            os.path.join(LIVEMEET_MESSY_DIR, f"{meet_id}_*.csv"),
            os.path.join(LIVEMEET_FINAL_DIR, f"{meet_id}_*.csv")
        ]
        for pattern in patterns:
            for f in glob.glob(pattern):
                try: os.remove(f)
                except: pass

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
    import glob
    driver = None
    try:
        # CRASH PROTECTION: Delete existing files for this meet to ensure a fresh start
        for f in glob.glob(os.path.join(MSO_DIR, f"{meet_id}_*.csv")):
            try: os.remove(f)
            except: pass

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

    return False
    
def is_high_priority(meet_type, meet_name, location='', meet_id=None, priority_ids=None):
    """
    Determines if a meet matches the High Priority criteria.
    """
    n = str(meet_name).upper()
    l = str(location).upper()
    
    # Check if this meet is a prioritized LiveMeet (passed from caller)
    if priority_ids and meet_id in priority_ids:
        return True

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

class BackgroundLoader:
    def __init__(self, interval=600): # Default 10 minutes
        self.interval = interval
        self.last_run = 0
        self.process = None

    def is_running(self):
        if self.process is None:
            return False
        return self.process.poll() is None

    def check_and_trigger(self, force=False):
        """Triggers the loader if interval passed and not already running."""
        now = time.time()
        
        if self.is_running():
            return False # Already working
            
        if force or (now - self.last_run >= self.interval):
            print(f"\n>>> Launching Background Loader (Scheduled trigger)... <<<")
            try:
                # Use Popen for non-blocking execution
                # We use sys.executable to ensure we use the same environment
                self.process = subprocess.Popen(
                    [sys.executable, "load_orchestrator.py"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT
                )
                self.last_run = now
                return True
            except Exception as e:
                logging.error(f"Failed to launch background loader: {e}")
                print(f"Error launching loader: {e}")
        return False

class GoldRefresher:
    def __init__(self, interval=1800): # 30 minutes
        self.interval = interval
        self.last_run = 0
        self.process = None

    def is_running(self):
        if self.process is None:
            return False
        return self.process.poll() is None

    def check_and_trigger(self, force=False):
        """Triggers the gold refresh if interval passed and not already running."""
        now = time.time()
        
        if self.is_running():
            return False
            
        if force or (now - self.last_run >= self.interval):
            print(f"\n>>> Launching Scheduled Gold Refresh (30-min timer)... <<<")
            try:
                self.process = subprocess.Popen(
                    [sys.executable, "load_orchestrator.py", "--gold-only"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT
                )
                self.last_run = now
                return True
            except Exception as e:
                logging.error(f"Failed to launch gold refresher: {e}")
        return False

class StatusHeartbeat(threading.Thread):
    def __init__(self, stop_event, get_remaining_meets, get_pending_csvs, loader=None, gold_refresher=None, interval=30):
        super().__init__()
        self.stop_event = stop_event
        self.get_remaining_meets = get_remaining_meets
        self.get_pending_csvs = get_pending_csvs
        self.loader = loader
        self.gold_refresher = gold_refresher
        self.interval = interval
        self.daemon = True

    def run(self):
        while not self.stop_event.is_set():
            # Wait for interval or stop event
            if self.stop_event.wait(self.interval):
                break
            
            rem_meets = self.get_remaining_meets()
            pending_csvs = self.get_pending_csvs()
            
            l_status = "IDLE"
            if self.loader and self.loader.is_running():
                l_status = "RUNNING"
            
            g_status = "IDLE"
            if self.gold_refresher and self.gold_refresher.is_running():
                g_status = "RUNNING"
                
            print(f"\n[ORCHESTRATOR STATUS] Remaining: {rem_meets} meets | Unloaded CSVs: {pending_csvs} | Loader: {l_status} | Gold Refresh: {g_status}")

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

    # Global Exception Hook
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logging.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
        print("CRITICAL: Uncaught exception logged to file.", file=sys.stderr)
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)

    sys.excepthook = handle_exception

    print("--- Scraper Orchestrator Started (Detailed logs in 'scraper_orchestrator.log') ---")
    logging.info("--- Scraper Orchestrator Started ---")




    # Load Status Manifest
    status_manifest = load_status()

    # Determine priority LiveMeet IDs from Gold tables (L1/L2)
    # Priority: Meets that made it into L1/L2 AND were scraped off livemeet
    priority_ids = []
    if os.path.exists("gym_data.db"):
        try:
            with sqlite3.connect("gym_data.db") as conn:
                q = """
                    SELECT DISTINCT source_meet_id 
                    FROM Meets m 
                    JOIN Results r ON m.meet_db_id = r.meet_db_id 
                    WHERE m.source = 'livemeet' 
                      AND r.athlete_id IN (
                          SELECT athlete_id FROM Gold_Results_MAG_Filtered_L1 
                          UNION 
                          SELECT athlete_id FROM Gold_Results_MAG_Filtered_L2
                          UNION
                          SELECT athlete_id FROM Gold_Results_WAG -- WAG doesn't always have L1 filter yet
                      )
                """
                priority_ids = [row[0] for row in conn.execute(q).fetchall()]
                print(f"  -> Identified {len(priority_ids)} LiveMeet meets in Gold L1/L2 for priority queue.")
        except Exception as e:
            print(f"  -> Warning: Failed to query priority meets from DB: {e}")
    
    status_manifest['priority_livemeet_ids'] = priority_ids

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

    def load_all_tasks():
        """Load all tasks from manifest files and manual injections."""
        tasks = []
        
        # Load KScore
        if os.path.exists(KSCORE_CSV):
            df = pd.read_csv(KSCORE_CSV)
            id_col = [c for c in df.columns if 'MeetID' in c][0]
            name_col = [c for c in df.columns if 'MeetName' in c][0]
            for _, row in df.iterrows():
                tasks.append(('kscore', str(row[id_col]), str(row[name_col])))

        # Load LiveMeet
        if os.path.exists(LIVEMEET_CSV):
            df = pd.read_csv(LIVEMEET_CSV)
            id_col = [c for c in df.columns if 'MeetID' in c][0]
            name_col = [c for c in df.columns if 'MeetName' in c][0]
            loc_col = [c for c in df.columns if 'Location' in c][0]
            for _, row in df.iterrows():
                tasks.append(('livemeet', str(row[id_col]), str(row[name_col]), str(row[loc_col])))

        # Load MSO (PAUSED)
        if False and os.path.exists(MSO_CSV):
            df = pd.read_csv(MSO_CSV)
            id_col = [c for c in df.columns if 'MeetID' in c][0]
            name_col = [c for c in df.columns if 'MeetName' in c][0]
            for _, row in df.iterrows():
                tasks.append(('mso', str(row[id_col]), str(row[name_col])))

        # Manual Injection: 2025 Mens HNI & Vegas Cup 2025
        tasks.append(('mso', '33704', '2025 Mens HNI'))
        tasks.append(('mso', '33619', 'Vegas Cup 2025 - Men'))
        tasks.append(('mso', '35898', '2026 HNI')) # ADDED PER USER REQUEST

        # Load KSIS
        if os.path.exists(KSIS_CSV):
            df = pd.read_csv(KSIS_CSV)
            id_col = [c for c in df.columns if 'MeetID' in c][0]
            name_col = [c for c in df.columns if 'MeetName' in c][0]
            for _, row in df.iterrows():
                tasks.append(('ksis', str(row[id_col]), str(row[name_col])))
        
        return tasks

    def build_queue(all_tasks, status_manifest):
        """Build prioritized queue from tasks, filtering out completed ones."""
        high_priority_tasks = []
        low_priority_tasks = []

        for t in all_tasks:
            if len(t) == 4:
                m_type, m_id, m_name, m_loc = t
            else:
                m_type, m_id, m_name = t
                m_loc = ''
                
            if is_high_priority(m_type, m_name, m_loc, meet_id=m_id, priority_ids=status_manifest.get('priority_livemeet_ids', [])):
                high_priority_tasks.append((m_type, m_id, m_name))
            else:
                low_priority_tasks.append((m_type, m_id, m_name))

        # Prioritize MSO within the High Priority group
        high_priority_tasks.sort(key=lambda x: 0 if x[0] == 'mso' else 1)
        # Randomize others (optional, but keep it simple)
        random.shuffle(low_priority_tasks)
        
        # Combined Queue: High Priority FIRST
        final_queue_list = high_priority_tasks + low_priority_tasks
        
        # Filter out already finished tasks
        def get_status_simple(key):
            val = status_manifest.get(key)
            if isinstance(val, dict):
                return val.get('status')
            return val

        queue = [t for t in final_queue_list if get_status_simple(f"{t[0]}_{t[1]}") not in ["DONE", "FAILED"]]
        return queue, get_status_simple
    
    # Initial load
    all_tasks = load_all_tasks()
    queue, get_status_simple = build_queue(all_tasks, status_manifest)
    
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

    # Main continuous loop
    while True:
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
            
            def count_pending_csvs():
                """Counts CSVs on disk not yet in ProcessedFiles table."""
                try:
                    files_on_disk = set()
                    for d in [KSCORE_DIR, LIVEMEET_FINAL_DIR, MSO_DIR, KSIS_DIR]:
                        if os.path.exists(d):
                            for f in glob.glob(os.path.join(d, "*.csv")):
                                files_on_disk.add(os.path.basename(f))
                    
                    if not files_on_disk:
                        return 0

                    if not os.path.exists("gym_data.db"):
                        return len(files_on_disk)

                    # Use a timeout and WAL mode to be safe against concurrent loader writes
                    with sqlite3.connect("gym_data.db", timeout=10) as conn:
                        conn.execute("PRAGMA journal_mode=WAL")
                        cursor = conn.cursor()
                        cursor.execute("SELECT file_path FROM ProcessedFiles")
                        processed_files = set(os.path.basename(row[0]) for row in cursor.fetchall())
                        
                        pending = len(files_on_disk - processed_files)
                        return pending
                except Exception as e:
                    return f"Err ({e})"

            # Background Loader Setup
            loader = BackgroundLoader(interval=600) # Every 10 mins
            gold_refresher = GoldRefresher(interval=1800) # Every 30 mins

            current_progress = 0
            
            def get_remaining_meets():
                """Calculate actual remaining meets from status manifest."""
                all_done_count = len([k for k,v in status_manifest.items() if (isinstance(v, dict) and v.get('status') == 'DONE') or v == 'DONE'])
                return len(all_tasks) - all_done_count

            heartbeat = StatusHeartbeat(
                heartbeat_stop, 
                get_remaining_meets=get_remaining_meets,
                get_pending_csvs=count_pending_csvs,
                loader=loader,
                gold_refresher=gold_refresher
            )
            heartbeat.start()

            # Initial DB check/load (Background)
            if not os.path.exists("gym_data.db") or count_pending_csvs() > 0:
                loader.check_and_trigger(force=True)

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
                    # Trigger loader every CSV_BATCH_THRESHOLD files
                    MEET_CHUNK_SIZE = 50 
                    
                    current_csv_count = 0
                    
                    for i in range(0, len(queue), MEET_CHUNK_SIZE):
                        if stop_requested:
                            break
                            
                        chunk = queue[i : i + MEET_CHUNK_SIZE]
                        
                        meets_rem = len(queue) - i
                        print(f"\n[PROGRESS] Meets remaining in this attempt: {meets_rem} | CSVs waiting for next push: {current_csv_count}/{CSV_BATCH_THRESHOLD}")
                        
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
                                    current_progress += 1 # Important for heartbeat
                                    
                                    # Calculate real-time stats
                                    all_done_count = len([k for k,v in status_manifest.items() if (isinstance(v, dict) and v.get('status') == 'DONE') or v == 'DONE'])
                                    total_loaded = len(all_tasks)
                                    # Fallback simple count
                                    rem_total = total_loaded - all_done_count
                                    
                                    logging.info(f"[{stype}] {mid}: {status} - {message}")
                                    print(f"  [OK] {mid} ({count:2} files) | Total Rem: {rem_total:<4} | Pending: {current_csv_count}/{CSV_BATCH_THRESHOLD} | Scraped: {all_done_count}/{total_loaded}")
                                    
                                    # Periodic Trigger check
                                    loader.check_and_trigger()
                                    gold_refresher.check_and_trigger()

                                    # Threshold Trigger check
                                    if current_csv_count >= CSV_BATCH_THRESHOLD and not stop_requested:
                                        if loader.check_and_trigger(force=True):
                                            current_csv_count = 0 
                                    
                                else:
                                    # ERROR logic
                                    message = parts[1] if len(parts) > 1 else "Unknown Error"
                                    logging.error(f"  [FAIL] {mid}: {message}")
                                    print(f"  [FAIL] {mid}: {message}")
                                    
                                    # --- FAILURE TRACKING ---
                                    fail_data = status_manifest.get(key, {})
                                    if not isinstance(fail_data, dict):
                                        fail_data = {}
                                    
                                    current_fails = fail_data.get('fail_count', 0) + 1
                                    fail_data['fail_count'] = current_fails
                                    fail_data['name'] = mname 
                                    
                                    if current_fails >= MAX_FAILURES:
                                        fail_data['status'] = "FAILED"
                                        fail_message = f"Exceeded max retries ({current_fails}). Marking FAILED."
                                        logging.error(f"  [PERMANENT FAIL] {mid} {fail_message}")
                                        print(f"  [STOP] {mid} {fail_message}")
                                    else:
                                        # Ensure we don't accidentally mark it as DONE or FAILED yet
                                        fail_data['status'] = fail_data.get('status', 'RETRYING')

                                    status_manifest[key] = fail_data
                                    save_status(status_manifest)
                                    # ------------------------
                                    current_progress += 1 # Still progress even if fail
                                    
                            except Exception as e:
                                logging.error(f"  [EXCEPTION] {mid}: {e}")
                                print(f"  [EXCEPTION] {mid}: {e}")
                                current_progress += 1
                        
                        # Update status after chunk
                        save_status(status_manifest)
        
                    # Prepare queue for next attempt (retry failed items)
                    # Prepare queue for next attempt (retry failed items)
                    new_queue = []
                    for t in queue:
                        # Re-check status in case it failed in this chunk and is now FAILED
                        s = get_status_simple(f"{t[0]}_{t[1]}")
                        if s not in ["DONE", "FAILED"]:
                            new_queue.append(t)
                    queue = new_queue
                    
                    if queue and attempt < MAX_RETRIES:
                        jitter = random.randint(5, 15)
                        print(f"Waiting {jitter}s before next retry attempt...")
                        time.sleep(jitter)
            finally:
                heartbeat_stop.set()

        # Check if we should continue with a new round
        remaining_count = len([t for t in all_tasks if get_status_simple(f"{t[0]}_{t[1]}") not in ["DONE", "FAILED"]])
        
        if remaining_count == 0:
            print("\n=== All tasks completed! Waiting for new tasks... ===")
            # break  <-- REMOVED to enable persistence
        elif stop_requested:
            print(f"\n=== Shutdown requested. {remaining_count} tasks remaining. ===")
            break
        else:
            print(f"\n--- Round complete. {remaining_count} tasks remaining. Reloading in {POLL_INTERVAL}s... ---")
            time.sleep(POLL_INTERVAL)
            
            # Reload status manifest and rebuild queue
            status_manifest = load_status()
            all_tasks = load_all_tasks()
            queue, get_status_simple = build_queue(all_tasks, status_manifest)
            
            if not queue:
                print("\n=== No pending tasks after reload. Waiting... ===")
                # break <-- REMOVED to enable persistence
            else:
                print(f"Reloaded: {len(all_tasks)} total tasks, {len(queue)} remaining")

    # Final summary
    final_remaining = len([t for t in all_tasks if get_status_simple(f"{t[0]}_{t[1]}") not in ["DONE", "FAILED"]])
    msg = f"Scraper Orchestration finished. Remaining tasks: {final_remaining}"
    logging.info(msg)
    print(f"\n--- {msg} ---")
    if final_remaining > 0:
        print(f"Warning: {final_remaining} tasks could not be completed.")
    
    print("\nTriggering final load...")
    try:
        subprocess.run([sys.executable, "load_orchestrator.py"], check=False)
    except KeyboardInterrupt:
        print("\nOrchestrator stopped by user.")
    except Exception as e:
        logging.critical("Fatal error in main loop", exc_info=True)
        print(f"\nCRITICAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
