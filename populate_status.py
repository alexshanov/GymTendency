import os
import glob
import json
import pandas as pd

KSCORE_DIR = "CSVs_kscore_final"
LIVEMEET_DIR = "CSVs_Livemeet_final"
MSO_DIR = "CSVs_mso_final"
STATUS_MANIFEST = "scraped_meets_status.json"

def main():
    status = {}
    
    # 1. KScore
    k_files = glob.glob(os.path.join(KSCORE_DIR, "*_FINAL_*.csv"))
    for f in k_files:
        mid = os.path.basename(f).split('_FINAL_')[0]
        # Skip ev25 to ensure it gets re-scraped/verified by the new orchestrator logic
        if "ev25" not in mid:
            status[f"kscore_{mid}"] = "DONE"
            
    # 2. LiveMeet
    l_files = glob.glob(os.path.join(LIVEMEET_DIR, "*_FINAL_*.csv"))
    for f in l_files:
        mid = os.path.basename(f).split('_')[0]
        status[f"livemeet_{mid}"] = "DONE"
        
    # 3. MSO
    m_files = glob.glob(os.path.join(MSO_DIR, "*_mso.csv"))
    for f in m_files:
        mid = os.path.basename(f).replace('_mso.csv', '')
        status[f"mso_{mid}"] = "DONE"
        
    with open(STATUS_MANIFEST, 'w') as f:
        json.dump(status, f, indent=4)
    
    print(f"Populated {len(status)} finished meets into {STATUS_MANIFEST}")
    print("Note: Ed Vincent 2025 (ev25) was intentionally excluded to trigger a fresh scan.")

if __name__ == "__main__":
    main()
