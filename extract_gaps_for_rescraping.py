import pandas as pd
import re
import json
import os

def extract_gaps():
    print("--- Extracting Gaps for Remediation ---")
    
    # 1. Load LiveMeet Manifest
    manifest_file = "discovered_meet_ids_livemeet.csv"
    if not os.path.exists(manifest_file):
        print(f"Error: {manifest_file} not found.")
        return

    manifest_df = pd.read_csv(manifest_file)
    # create map: CleanName -> MeetID
    # We strip whitespace and handle case
    name_to_id = {}
    for _, row in manifest_df.iterrows():
        name = str(row['MeetName']).strip()
        mid = row['MeetID']
        name_to_id[name] = mid
        name_to_id[name.lower()] = mid # fallback
        
    print(f"Loaded {len(name_to_id)} meet mappings.")

    # 2. Parse Ghost Entries (Full Re-scrape)
    ghost_meets = set()
    audit_file = "audit_patterns_report.txt"
    
    if os.path.exists(audit_file):
        with open(audit_file, 'r') as f:
            content = f.read()
            
        # Parse Ghost Section
        # Expected format: 
        # source       meet_name         full_name
        # 0  livemeet  Panther Invite      Emily Liskai
        # ...
        
        # Regex to find lines with 'livemeet' in ghost section
        # We look for the section header first
        if "--- 3. Ghost Entries" in content:
            ghost_section = content.split("--- 3. Ghost Entries")[1]
            matches = re.findall(r'livemeet\s+(.*?)\s{2,}', ghost_section)
            for m in matches:
                meet_name = m.strip()
                # Clean up if it grabbed too much (e.g. into the name col)
                # The output format is fixed width-ish from pandas, so usually double space separates cols
                if "  " in meet_name:
                    meet_name = meet_name.split("  ")[0]
                ghost_meets.add(meet_name)
    
    print(f"Identified {len(ghost_meets)} potential ghost meets.")

    # 3. Parse Missing Apparatus (Targeted Re-scrape)
    # Format: [livemeet] Meet Name (Lvl LevelName): Missing App
    targeted_scrapes = []
    
    with open(audit_file, 'r') as f:
        for line in f:
            if "[livemeet]" in line and "Missing" in line:
                # [livemeet] Copeland Classic WAG 2025 (Lvl XG): Missing Floor ...
                try:
                    match = re.search(r'\[livemeet\] (.*?) \(Lvl (.*?)\): Missing', line)
                    if match:
                        meet_name = match.group(1).strip()
                        level_name = match.group(2).strip()
                        targeted_scrapes.append({
                            'meet_name': meet_name,
                            'level': level_name
                        })
                except Exception:
                    pass

    print(f"Identified {len(targeted_scrapes)} targeted level gaps.")

    # 4. Map to IDs and Build Queue
    remediation_queue = {
        "full_scrapes": [],
        "targeted_scrapes": []
    }
    
    # Map Ghosts
    for name in ghost_meets:
        mid = name_to_id.get(name) or name_to_id.get(name.lower())
        if mid:
            remediation_queue['full_scrapes'].append({'meet_id': mid, 'reason': 'Ghost Entries'})
        else:
            print(f"Warning: Could not map Ghost Meet '{name}' to ID.")

    # Map Targets
    for item in targeted_scrapes:
        name = item['meet_name']
        mid = name_to_id.get(name) or name_to_id.get(name.lower())
        if mid:
            # Deduplicate: Don't add if already in full scrape list
            if not any(x['meet_id'] == mid for x in remediation_queue['full_scrapes']):
                # Check if we already have this meet in targeted
                existing = next((x for x in remediation_queue['targeted_scrapes'] if x['meet_id'] == mid), None)
                if existing:
                    if item['level'] not in existing['levels']:
                        existing['levels'].append(item['level'])
                else:
                    remediation_queue['targeted_scrapes'].append({
                        'meet_id': mid, 
                        'levels': [item['level']],
                        'reason': 'Missing Apparatus'
                    })
        else:
             # Try stricter lookup? sometimes unicode issues
             pass

    # Save
    with open("remediation_queue.json", "w") as f:
        json.dump(remediation_queue, f, indent=2)
        
    print(f"Queue generated: {len(remediation_queue['full_scrapes'])} full scrapes, {len(remediation_queue['targeted_scrapes'])} targeted meets.")

if __name__ == "__main__":
    extract_gaps()
