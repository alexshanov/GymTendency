import json
import pandas as pd
import os

STATUS_FILE = "scraped_meets_status.json"
SOURCE_FILES = {
    "kscore": "discovered_meet_ids_kscore.csv",
    "ksis": "discovered_meet_ids_ksis.csv",
    "livemeet": "discovered_meet_ids_livemeet.csv",
    "mso": "discovered_meet_ids_mso.csv"
}

def find_unscraped():
    if not os.path.exists(STATUS_FILE):
        print("Status manifest not found.")
        return

    with open(STATUS_FILE, "r") as f:
        status = json.load(f)

    # DONE keys look like "livemeet_ID" or "ksis_ID"
    # Some values are just "DONE", others are dicts with {"status": "DONE"}
    done_ids = set()
    for k, v in status.items():
        is_done = False
        if isinstance(v, dict):
            if v.get("status") == "DONE":
                is_done = True
        elif v == "DONE":
            is_done = True
        
        if is_done:
            done_ids.add(k)

    unscraped_list = []

    for stype, filename in SOURCE_FILES.items():
        if not os.path.exists(filename):
            continue
        
        df = pd.read_csv(filename)
        # Handle LiveMeet/KScore having 'Source' column vs others
        # Actually we just care about MeetID and MeetName
        
        for _, row in df.iterrows():
            mid = str(row['MeetID'])
            mname = str(row['MeetName'])
            
            key = f"{stype}_{mid}"
            if key not in done_ids:
                # Get more details if available
                year = row.get('Year', 'N/A')
                location = row.get('Location', 'N/A')
                unscraped_list.append({
                    "Type": stype,
                    "ID": mid,
                    "Name": mname,
                    "Year": year,
                    "Location": location
                })

    if not unscraped_list:
        print("No unscraped meets found.")
    else:
        # Sort by Type then Name
        unscraped_list.sort(key=lambda x: (x['Type'], x['Name']))
        
        print(f"Found {len(unscraped_list)} unscraped meets:\n")
        print(f"{'TYPE':<10} | {'NAME':<50} | {'YEAR':<5} | {'ID'}")
        print("-" * 85)
        for m in unscraped_list:
            # Shorten name if too long for display
            name = m['Name'][:48] + ".." if len(m['Name']) > 50 else m['Name']
            print(f"{m['Type']:<10} | {name:<50} | {m['Year']:<5} | {m['ID']}")

if __name__ == "__main__":
    find_unscraped()
