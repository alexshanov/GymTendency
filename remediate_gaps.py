
import json
import os
import glob
import time
import shutil
from livemeet_scraper import scrape_raw_data_to_separate_files, fix_and_standardize_headers

import sys
import argparse

QUEUE_FILE = "remediation_queue.json"
MESSY_DIR = "CSVs_Livemeet_messy"
FINAL_DIR = "CSVs_Livemeet_final"

def remediate():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Limit number of operations")
    args = parser.parse_args()

    if not os.path.exists(QUEUE_FILE):
        print("Queue file not found.", flush=True)
        return

    with open(QUEUE_FILE, 'r') as f:
        queue = json.load(f)

    full_ops = queue.get('full_scrapes', [])
    targeted_ops = queue.get('targeted_scrapes', [])
    
    total_ops = len(full_ops) + sum(len(x['levels']) for x in targeted_ops)
    print(f"--- Starting Remediation: {total_ops} total scrape operations ---", flush=True)
    
    processed_count = 0
    ops_limit = args.limit if args.limit > 0 else total_ops
    
    # 1. Full Scrapes
    for item in full_ops:
        if processed_count >= ops_limit: break
        meet_id = item['meet_id']
        print(f"\n[Full Scrape] Meeting ID: {meet_id} ({item.get('reason')})", flush=True)
        url = f"https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id={meet_id}"
        
        success, count, _ = scrape_raw_data_to_separate_files(url, meet_id, MESSY_DIR)
        if success:
            finalize_files(meet_id)
        processed_count += 1
        print(f"Progress: {processed_count}/{total_ops}", flush=True)
        
        # Incremental Load
        if processed_count % 10 == 0:
            print("--- Performing Incremental Load ---", flush=True)
            os.system("python3 load_orchestrator.py")

    # 2. Targeted Scrapes
    for item in targeted_ops:
        if processed_count >= ops_limit: break
        meet_id = item['meet_id']
        levels = item['levels']
        
        for lvl in levels:
            if processed_count >= ops_limit: break
            print(f"\n[Targeted Scrape] Meet ID: {meet_id} | Level: {lvl}", flush=True)
            url = f"https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id={meet_id}"
            
            # Scrape specific level
            success, count, _ = scrape_raw_data_to_separate_files(url, meet_id, MESSY_DIR, target_level_name=lvl)
            if success:
                # We finalize broadly for this meet ID, as scrape might save files differently
                finalize_files(meet_id)
            
            processed_count += 1
            print(f"Progress: {processed_count}/{total_ops}", flush=True)

            # Incremental Load
            if processed_count % 10 == 0:
                print("--- Performing Incremental Load ---", flush=True)
                os.system("python3 load_orchestrator.py")
            
    print("\n--- Scraping Phase Complete. Triggering Loader... ---", flush=True)
    os.system("python3 load_orchestrator.py")

def finalize_files(meet_id):
    """
    Moves files from MESSY to FINAL, applying fixes if needed.
    """
    messy_files = glob.glob(os.path.join(MESSY_DIR, f"{meet_id}_*.csv"))
    
    for messy_path in messy_files:
        filename = os.path.basename(messy_path)
        
        if "_PEREVENT_" in filename and "_DETAILED" in filename:
            # Ready to move
            final_path = os.path.join(FINAL_DIR, filename)
            shutil.move(messy_path, final_path)
            print(f"  -> Promoted: {filename}")
            
        elif "_MESSY_" in filename:
            # Needs fixing
            final_name = filename.replace('_MESSY_', '_FINAL_').replace('_BYEVENT_', '_AA_')
            final_path = os.path.join(FINAL_DIR, final_name)
            
            if fix_and_standardize_headers(messy_path, final_path):
                print(f"  -> Fixed & Promoted: {final_name}")
                # Clean up source to keep messy tidy
                os.remove(messy_path)

if __name__ == "__main__":
    remediate()
