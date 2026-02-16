import argparse
import json
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION (Defaults) ---
STATUS_FILE = "scraped_meets_status.json"
# Default cutoff is effectively disabled or set to a hardcoded recent date if no args provided
# We will handle defaults in main()

SOURCE_FILES = {
    "kscore": "discovered_meet_ids_kscore.csv",
    "ksis": "discovered_meet_ids_ksis.csv",      
    "livemeet": "discovered_meet_ids_livemeet.csv", 
    "mso": "discovered_meet_ids_mso.csv"
}

TARGET_SOURCES = ['kscore', 'ksis', 'livemeet'] 

def reset_recent_meets(days_back=None, years_back=None):
    
    cutoff_date = None
    if days_back is not None:
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_back)
        print(f"--- RESETTING MEETS (Last {days_back} days -> Since {cutoff_date.date()}) ---")
    elif years_back is not None:
        # Approximate years
        cutoff_date = pd.Timestamp.now() - pd.DateOffset(years=years_back)
        print(f"--- RESETTING MEETS (Last {years_back} years -> Since {cutoff_date.date()}) ---")
    else:
        # Default fallback if run without args: Use the original "2026-01-01" logic or error?
        # Let's keep the user's original "Recent" request (Jan 1, 2026) as default for now
        cutoff_date = pd.Timestamp("2026-01-01")
        print(f"--- RESETTING RECENT MEETS (Default: Since {cutoff_date.date()}) ---")

    # 1. Load Status Manifest
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r') as f:
            status_manifest = json.load(f)
    else:
        print("Status manifest not found. Nothing to reset.")
        status_manifest = {}

    reset_count = 0
    
    # 2. Iterate each source CSV
    for stype, csv_file in SOURCE_FILES.items():
        if stype not in TARGET_SOURCES:
            print(f"Skipping source '{stype}' based on config (MSO excluded).")
            continue
            
        if not os.path.exists(csv_file):
            print(f"Source file '{csv_file}' not found. Skipping.")
            continue
            
        print(f"Processing {stype} from {csv_file}...")
        try:
            df = pd.read_csv(csv_file)
            count_for_source = 0
            
            for _, row in df.iterrows():
                mid = str(row['MeetID'])
                meet_date = None
                
                # ... [Date Parsing Logic maintained] ...
                if 'start_date_iso' in row and pd.notna(row['start_date_iso']):
                    try: meet_date = pd.Timestamp(row['start_date_iso'])
                    except: pass
                
                if meet_date is None and 'Dates' in row and pd.notna(row['Dates']):
                    try:
                        d_str = str(row['Dates']).split('-')[0].strip()
                        meet_date = pd.Timestamp(d_str)
                    except: pass
                
                if meet_date is None and 'Year' in row:
                    try:
                        y = int(row['Year'])
                        if y >= cutoff_date.year:
                             # If we only have year, assume Jan 2nd of that year
                             meet_date = pd.Timestamp(f"{y}-01-02")
                    except: pass

                if meet_date and meet_date >= cutoff_date:
                    key = f"{stype}_{mid}"
                    if stype == "ksis" and row.get('Source') == 'cases':
                         key = f"ksis_{mid}"
                    
                    current_val = status_manifest.get(key)
                    is_done = False
                    if isinstance(current_val, dict):
                        if current_val.get("status") == "DONE": is_done = True
                    elif current_val == "DONE":
                        is_done = True
                        
                    if is_done:
                        print(f"  -> Resetting status for: {stype} {mid} ({row.get('MeetName', '')}) - Date: {meet_date.date()}")
                        del status_manifest[key]
                        reset_count += 1
                        count_for_source += 1
            
            print(f"  Result: Reset {count_for_source} meets for {stype}.")
            
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")

    # 3. Save Manifest
    if reset_count > 0:
        import shutil
        shutil.copy(STATUS_FILE, f"{STATUS_FILE}.bak_recent_reset")
        with open(STATUS_FILE, 'w') as f:
            json.dump(status_manifest, f, indent=4)
        print(f"\nSUCCESS: Reset status for {reset_count} meets. Backup saved.")
    else:
        print("\nNo meets found in the target window needing reset.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset 'DONE' status for meets within a certain timeframe.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--days", type=int, help="Reset meets from the last X days.")
    group.add_argument("--years", type=int, help="Reset meets from the last X years.")
    
    args = parser.parse_args()
    
    reset_recent_meets(days_back=args.days, years_back=args.years)
