import re
import pandas as pd
import os
from collections import Counter

# --- CONFIGURATION ---
LOG_FILE = "scraper_orchestrator.log"
OUTPUT_REPORT = "failed_meets_report.md"

# CSV Files
KSCORE_CSV = "discovered_meet_ids_kscore.csv"
LIVEMEET_CSV = "discovered_meet_ids_livemeet.csv"
MSO_CSV = "discovered_meet_ids_mso.csv"
KSIS_CSV = "discovered_meet_ids_ksis.csv"

# URL Patterns
def get_url(meet_type, meet_id):
    if meet_type == 'kscore':
        raw_id = str(meet_id).replace('kscore_', '')
        return f"https://live.kscore.ca/results/{raw_id}"
    elif meet_type == 'livemeet':
        return f"https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id={meet_id}"
    elif meet_type == 'mso':
        return f"https://www.meetscoresonline.com/Results/{meet_id}"
    elif meet_type == 'ksis':
        return f"https://rgform.eu/resultx.php?id_prop={meet_id}"
    return "Unknown URL"

def main():
    print("Analyzing logs...")
    
    # 1. Parse Logs for Failures
    failure_pattern = re.compile(r"\[FAIL\] ([A-Za-z0-9_]+):")
    failures = []
    
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            for line in f:
                match = failure_pattern.search(line)
                if match:
                    failures.append(match.group(1))
    else:
        print(f"Log file {LOG_FILE} not found!")
        return

    failure_counts = Counter(failures)
    
    # 2. Load Metadata
    print("Loading metadata...")
    metadata = {}
    
    # Load KScore
    if os.path.exists(KSCORE_CSV):
        df = pd.read_csv(KSCORE_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            metadata[str(row[id_col])] = {'name': row[name_col], 'type': 'kscore'}

    # Load LiveMeet
    if os.path.exists(LIVEMEET_CSV):
        df = pd.read_csv(LIVEMEET_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            metadata[str(row[id_col])] = {'name': row[name_col], 'type': 'livemeet'}

    # Load MSO
    if os.path.exists(MSO_CSV):
        df = pd.read_csv(MSO_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            metadata[str(row[id_col])] = {'name': row[name_col], 'type': 'mso'}

    # Load KSIS
    if os.path.exists(KSIS_CSV):
        df = pd.read_csv(KSIS_CSV)
        id_col = [c for c in df.columns if 'MeetID' in c][0]
        name_col = [c for c in df.columns if 'MeetName' in c][0]
        for _, row in df.iterrows():
            metadata[str(row[id_col])] = {'name': row[name_col], 'type': 'ksis'}

    # 3. Generate Report
    print("Generating report...")
    
    top_failures = failure_counts.most_common(50)
    
    with open(OUTPUT_REPORT, "w") as f:
        f.write("# Top Failing Meets Report\n\n")
        f.write(f"Generated from `{LOG_FILE}`. The table below lists the top failing meets that are causing the infinite retry loop.\n\n")
        f.write("| Fail Count | Meet ID | Type | Meet Name | URL (For Verification) |\n")
        f.write("|------------|---------|------|-----------|------------------------|\n")
        
        for meet_id, count in top_failures:
            # Infer type if missing from metadata
            meet_type = "Unknown"
            meet_name = "Unknown Meet"
            
            if meet_id in metadata:
                meet_type = metadata[meet_id]['type']
                meet_name = metadata[meet_id]['name']
            else:
                # Basic inference
                if len(meet_id) == 32: # LiveMeet MD5 hash length
                    meet_type = 'livemeet'
                elif meet_id.startswith('kscore_'):
                    meet_type = 'kscore'
                elif meet_id.isdigit():
                    # Could be MSO or KSIS. 
                    # KSIS usually 4 digits? MSO usually 4-5?
                    pass 
                
            url = get_url(meet_type, meet_id)
            
            # Clean name for markdown table (escape pipes)
            clean_name = str(meet_name).replace("|", "-")
            
            f.write(f"| {count} | `{meet_id}` | {meet_type} | {clean_name} | [Verify Link]({url}) |\n")

    print(f"Report generated: {OUTPUT_REPORT}")

if __name__ == "__main__":
    main()
