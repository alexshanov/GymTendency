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
import argparse
import etl_functions # for hash calculation

# Import Scrapers
import kscore_scraper
import livemeet_scraper
import mso_scraper
import ksis_scraper

# --- CONFIGURATION ---
BASE_DIR = "/home/alex-shanov/GymTendency"
KSCORE_CSV = os.path.join(BASE_DIR, "discovered_meet_ids_kscore.csv")
LIVEMEET_CSV = os.path.join(BASE_DIR, "discovered_meet_ids_livemeet.csv")
MSO_CSV = os.path.join(BASE_DIR, "discovered_meet_ids_mso.csv")
KSIS_CSV = os.path.join(BASE_DIR, "discovered_meet_ids_ksis.csv")

KSCORE_DIR = os.path.join(BASE_DIR, "CSVs_kscore_final")
LIVEMEET_MESSY_DIR = os.path.join(BASE_DIR, "CSVs_Livemeet_messy")
LIVEMEET_FINAL_DIR = os.path.join(BASE_DIR, "CSVs_Livemeet_final")
MSO_DIR = os.path.join(BASE_DIR, "CSVs_mso_final")
KSIS_DIR = os.path.join(BASE_DIR, "CSVs_ksis_messy")

STATUS_MANIFEST = os.path.join(BASE_DIR, "scraped_meets_status.json")

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
        patterns = [
            os.path.join(LIVEMEET_MESSY_DIR, f"{meet_id}_*.csv"),
            os.path.join(LIVEMEET_FINAL_DIR, f"{meet_id}_*.csv")
        ]
        for pattern in patterns:
            for f in glob.glob(pattern):
                try: os.remove(f)
                except: pass

        meet_url = f"https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id={meet_id}"
        
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

        driver = mso_scraper.setup_driver(driver_path=driver_path)
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
    
def is_high_priority(meet_type, meet_name, location='', meet_id=None, priority_keys=None):
    """
    Determines if a meet matches the High Priority criteria.
    """
    n = str(meet_name).upper()
    l = str(location).upper()
    
    # Check if this meet is in our priority keys (source, id)
    if priority_keys and (meet_type, str(meet_id)) in priority_keys:
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

def _parse_meet_date(row):
    """Parse the date from a manifest row. Returns pd.Timestamp or None."""
    if 'start_date_iso' in row.index and pd.notna(row.get('start_date_iso')):
        try: return pd.Timestamp(row['start_date_iso'])
        except: pass
    if 'Dates' in row.index and pd.notna(row.get('Dates')):
        try:
            d_str = str(row['Dates'])
            euro_match = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', d_str)
            if euro_match:
                day, month, year = euro_match.groups()
                return pd.Timestamp(f"{year}-{month}-{day}")
            d_str = d_str.split('-')[0].strip()
            return pd.Timestamp(d_str)
        except: pass
    if 'Year' in row.index and pd.notna(row.get('Year')):
        try:
            y = int(row['Year'])
            return pd.Timestamp(f"{y}-01-02")
        except: pass
    return None

def _passes_date_filter(row, cutoff):
    """Returns True if the meet is after cutoff, or if no cutoff/no date."""
    if cutoff is None:
        return True
    meet_date = _parse_meet_date(row)
    if meet_date is None:
        return False
    return meet_date >= cutoff

def load_all_tasks(days_cutoff=None, days_arg=None, priority_only=False, priority_keys=None):
    """Load all tasks from manifest files and manual injections."""
    tasks = []
    skipped = 0
    if os.path.exists(KSCORE_CSV):
        df = pd.read_csv(KSCORE_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            if _passes_date_filter(row, days_cutoff):
                tasks.append(('kscore', str(row[id_col]), str(row[name_col])))
            else: skipped += 1
    if os.path.exists(LIVEMEET_CSV):
        df = pd.read_csv(LIVEMEET_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        loc_col = [c for c in df.columns if 'Location' in c][0]
        for _, row in df.iterrows():
            if _passes_date_filter(row, days_cutoff):
                tasks.append(('livemeet', str(row[id_col]), str(row[name_col]), str(row[loc_col])))
            else: skipped += 1
    manual_mso = [
        ('mso', '33704', '2025 Mens HNI', 2025),
        ('mso', '33619', 'Vegas Cup 2025 - Men', 2025),
        ('mso', '35898', '2026 HNI', 2026),
    ]
    for mtype, mid, mname, myear in manual_mso:
        if days_cutoff is None or pd.Timestamp(f"{myear}-01-02") >= days_cutoff:
            tasks.append((mtype, mid, mname))
    if os.path.exists(KSIS_CSV):
        df = pd.read_csv(KSIS_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            if _passes_date_filter(row, days_cutoff):
                tasks.append(('ksis', str(row[id_col]), str(row[name_col])))
            else: skipped += 1
    if days_cutoff is not None:
        print(f"  [FILTER] Skipped {skipped} meets outside the {days_arg}-day window.")
    return tasks

def build_queue(all_tasks, status_manifest, priority_only=False, priority_keys=None):
    if priority_only:
        all_tasks = [t for t in all_tasks if (t[0], str(t[1])) in priority_keys]
    high, low = [], []
    for t in all_tasks:
        m_type, m_id, m_name = t[0], t[1], t[2]
        m_loc = t[3] if len(t) == 4 else ''
        if is_high_priority(m_type, m_name, m_loc, meet_id=m_id, priority_keys=priority_keys):
            high.append((m_type, m_id, m_name))
        else: low.append((m_type, m_id, m_name))
    high.sort(key=lambda x: 0 if x[0] == 'mso' else 1)
    random.shuffle(low)
    final = high + low
    def get_status_simple(key):
        val = status_manifest.get(key)
        return val.get('status') if isinstance(val, dict) else val
    if priority_only: return final, get_status_simple
    queue = [t for t in final if get_status_simple(f"{t[0]}_{t[1]}") not in ["DONE", "FAILED"]]
    return queue, get_status_simple

# --- MAIN ORCHESTRATOR ---

def main():
    logging.basicConfig(filename='scraper_orchestrator.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filemode='a')
    console = logging.StreamHandler(); console.setLevel(logging.WARNING); logging.getLogger('').addHandler(console)
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt): sys.__excepthook__(exc_type, exc_value, exc_traceback); return
        logging.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)
    sys.excepthook = handle_exception
    parser = argparse.ArgumentParser(description="Scraper Orchestrator")
    parser.add_argument("--priority-only", action="store_true")
    parser.add_argument("--days", type=int, default=None)
    args = parser.parse_args()
    days_cutoff = None
    if args.days is not None:
        days_cutoff = pd.Timestamp.now() - pd.Timedelta(days=args.days)
        print(f"  [FILTER] --days {args.days} active. Only processing meets since {days_cutoff.date()}.")
    print("--- Scraper Orchestrator Started ---")
    cleanup_orphaned_processes()
    status_manifest = load_status()
    priority_keys = set()
    if os.path.exists("priority_meets.json"):
        try:
            with open("priority_meets.json", "r") as f:
                raw = json.load(f)
                priority_keys = set((row[0], str(row[1])) for row in raw)
        except: pass
    status_manifest['priority_keys'] = list(priority_keys)
    for d in [KSCORE_DIR, LIVEMEET_MESSY_DIR, LIVEMEET_FINAL_DIR, MSO_DIR, KSIS_DIR]: os.makedirs(d, exist_ok=True)
    print("Pre-installing WebDriver...")
    valid_driver_path = None
    try: valid_driver_path = ChromeDriverManager().install()
    except Exception as e: print(f"Warning: WebDriver pre-install failed: {e}")
    if not os.path.exists("gym_data.db"):
        try: subprocess.run([sys.executable, "load_orchestrator.py"], check=False)
        except: pass
    all_tasks = load_all_tasks(days_cutoff, args.days, args.priority_only, priority_keys)
    queue, get_status_simple = build_queue(all_tasks, status_manifest, args.priority_only, priority_keys)
    logging.info(f"Loaded: {len(all_tasks)}. Remaining: {len(queue)}")
    print(f"Total tasks: {len(all_tasks)}. Remaining: {len(queue)}")
    import signal
    stop_requested = False
    def signal_handler(sig, frame): nonlocal stop_requested; print("\nSHUTDOWN REQUESTED..."); stop_requested = True
    signal.signal(signal.SIGINT, signal_handler)
    task_functions = {'kscore': kscore_task, 'livemeet': livemeet_task, 'mso': mso_task, 'ksis': ksis_task}
    while True:
        with ProcessPoolExecutor(max_workers=WORKERS['kscore']) as k_pool, \
             ProcessPoolExecutor(max_workers=WORKERS['livemeet']) as l_pool, \
             ProcessPoolExecutor(max_workers=WORKERS['mso']) as m_pool, \
             ProcessPoolExecutor(max_workers=WORKERS['ksis']) as ksis_pool:
            pools = {'kscore': k_pool, 'livemeet': l_pool, 'mso': m_pool, 'ksis': ksis_pool}
            heartbeat_stop = threading.Event()
            def count_pending_csvs():
                try:
                    on_disk = set()
                    for d in [KSCORE_DIR, LIVEMEET_FINAL_DIR, MSO_DIR, KSIS_DIR]:
                        if os.path.exists(d):
                            for f in glob.glob(os.path.join(d, "*.csv")): on_disk.add(os.path.basename(f))
                    if not on_disk: return 0
                    with sqlite3.connect("gym_data.db", timeout=10) as conn:
                        processed = set(os.path.basename(row[0]) for row in conn.execute("SELECT file_path FROM ProcessedFiles").fetchall())
                        return len(on_disk - processed)
                except: return "Err"
            loader = BackgroundLoader(); gold_refresher = GoldRefresher()
            def get_rem(): return len(all_tasks) - len([k for k,v in status_manifest.items() if (isinstance(v, dict) and v.get('status') == 'DONE') or v == 'DONE'])
            heartbeat = StatusHeartbeat(heartbeat_stop, get_remaining_meets=get_rem, get_pending_csvs=count_pending_csvs, loader=loader, gold_refresher=gold_refresher)
            heartbeat.start()
            try:
                for attempt in range(1, MAX_RETRIES + 1):
                    if stop_requested or not queue: break
                    print(f"--- ATTEMPT {attempt}/{MAX_RETRIES} ---")
                    cleanup_orphaned_processes()
                    CH_SIZE = 50; current_csv_count = 0
                    for i in range(0, len(queue), CH_SIZE):
                        if stop_requested: break
                        chunk = queue[i : i + CH_SIZE]
                        futures = {}
                        for stype, mid, mname in chunk:
                            futures[pools[stype].submit(task_functions[stype], mid, mname, valid_driver_path)] = (stype, mid, mname)
                        for future in as_completed(futures):
                            if stop_requested: break
                            stype, mid, mname = futures[future]; key = f"{stype}_{mid}"
                            try:
                                res = future.result(); parts = res.split(':', 2)
                                if "DONE" in parts[0]:
                                    count = int(parts[2]) if len(parts) >= 3 else 0
                                    status_manifest[key] = {"status": "DONE", "name": mname}
                                    save_status(status_manifest); current_csv_count += count
                                    done_c = len([k for k,v in status_manifest.items() if (isinstance(v, dict) and v.get('status') == 'DONE') or v == 'DONE'])
                                    print(f"  [OK] {mid} | Rem: {len(all_tasks)-done_c} | Scraped: {done_c}/{len(all_tasks)}")
                                    loader.check_and_trigger(); gold_refresher.check_and_trigger()
                                else:
                                    msg = parts[1] if len(parts) > 1 else "Error"
                                    f_data = status_manifest.get(key, {}); c_f = (f_data.get('fail_count', 0) if isinstance(f_data, dict) else 0) + 1
                                    status_manifest[key] = {"status": "FAILED" if c_f >= MAX_FAILURES else "RETRYING", "fail_count": c_f, "name": mname}
                                    save_status(status_manifest); print(f"  [FAIL] {mid}: {msg}")
                            except Exception as e: print(f"  [EXE] {mid}: {e}")
                    new_q = []
                    for t in queue:
                        if get_status_simple(f"{t[0]}_{t[1]}") not in ["DONE", "FAILED"]: new_q.append(t)
                    queue = new_q
                    if queue and attempt < MAX_RETRIES: time.sleep(random.randint(5, 15))
            finally: heartbeat_stop.set()
        rem = len([t for t in all_tasks if get_status_simple(f"{t[0]}_{t[1]}") not in ["DONE", "FAILED"]])
        if rem == 0 or stop_requested: break
        print(f"--- Round complete. {rem} tasks remaining. Reloading in {POLL_INTERVAL}s... ---")
        time.sleep(POLL_INTERVAL); status_manifest = load_status()
        all_tasks = load_all_tasks(days_cutoff, args.days, args.priority_only, priority_keys)
        queue, get_status_simple = build_queue(all_tasks, status_manifest, args.priority_only, priority_keys)

if __name__ == "__main__":
    main()
