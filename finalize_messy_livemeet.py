import os
import glob
import shutil
from livemeet_scraper import fix_and_standardize_headers

MESSY_DIR = "CSVs_Livemeet_messy"
FINAL_DIR = "CSVs_Livemeet_final"

def finalize_all_messy():
    print(f"--- Processing files in {MESSY_DIR} ---")
    
    # 1. Finalize PEREVENT files (just move)
    perevent_files = glob.glob(os.path.join(MESSY_DIR, "*_PEREVENT_*.csv"))
    for f in perevent_files:
        dest = os.path.join(FINAL_DIR, os.path.basename(f))
        shutil.move(f, dest)
        print(f"Moved: {os.path.basename(f)}")
        
    # 2. Finalize BYEVENT files (just move)
    byevent_files = glob.glob(os.path.join(MESSY_DIR, "*_BYEVENT_*.csv"))
    for f in byevent_files:
        dest = os.path.join(FINAL_DIR, os.path.basename(f))
        shutil.move(f, dest)
        print(f"Moved: {os.path.basename(f)}")
        
    # 3. Finalize MESSY files (fix headers then move)
    messy_files = glob.glob(os.path.join(MESSY_DIR, "*_MESSY_*.csv"))
    for f in messy_files:
        filename = os.path.basename(f)
        final_name = filename.replace('_MESSY_', '_FINAL_').replace('_BYEVENT_', '_AA_')
        dest = os.path.join(FINAL_DIR, final_name)
        
        if fix_and_standardize_headers(f, dest):
            print(f"Fixed & Moved: {final_name}")
            os.remove(f)
        else:
            print(f"FAILED to fix: {filename}")

if __name__ == "__main__":
    finalize_all_messy()
