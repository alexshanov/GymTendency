import pandas as pd
import os

manifest_file = "discovered_meet_ids_ksis.csv"

def repair_manifest():
    print(f"Repairing {manifest_file}...")
    
    # Read raw lines
    with open(manifest_file, 'r') as f:
        lines = f.readlines()
        
    print(f"Read {len(lines)} lines.")
    
    header = "MeetID,MeetName,Year,Date,Location\n"
    new_lines = [header]
    
    seen_ids = set()
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line or line.startswith("MeetID"): continue
        
        # Simple CSV split is risky with quotes, but let's try robust parsing
        # Or better yet, use Python's csv module
        pass

    import csv
    import io
    
    # Re-read properly
    data = []
    
    # Treat 2-col rows specially using pandas if possible, but mixed is hard.
    # Manual parsing:
    
    with open(manifest_file, 'r') as f:
        # standard reader might fail on row 3 if it expects 2 cols based on row 1
        # so lets read as list of lists
        reader = csv.reader(f)
        try:
            rows = list(reader)
        except Exception as e:
            print(f"CSV read error: {e}")
            return

    # Process rows
    valid_rows = []
    
    # header is row 0
    # First row is typically MeetID, MeetName
    
    for row in rows:
        if not row: continue
        if row[0] == 'MeetID': continue
        
        meet_id = row[0]
        meet_name = row[1] if len(row) > 1 else ""
        year = row[2] if len(row) > 2 else ""
        date = row[3] if len(row) > 3 else ""
        loc = row[4] if len(row) > 4 else ""
        
        if meet_id in seen_ids: continue
        seen_ids.add(meet_id)
        
        # Backfill Year for 9143 if missing (I know it's 2025 from prev context)
        if meet_id == '9143' and not year: year = '2025'
        
        valid_rows.append({
            'MeetID': meet_id,
            'MeetName': meet_name,
            'Year': year,
            'Date': date,
            'Location': loc
        })
        
    df = pd.DataFrame(valid_rows)
    print(f"Recovered {len(df)} rows.")
    df.to_csv(manifest_file, index=False)
    print("Manifest repaired.")

if __name__ == "__main__":
    repair_manifest()
