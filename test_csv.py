
import csv
import re
import os

filepath = "CSVs_Livemeet_final/D6A65C67D6FEE84AAF3BB44FDE4ECC93_FINAL_AA_MAG_Novice_Combined_MAG.csv"

with open(filepath, 'r') as f:
    reader = csv.DictReader(f)
    print(f"Headers: {reader.fieldnames}")
    
    # Event identification logic
    result_columns = [col for col in reader.fieldnames if col.startswith('Result_')]
    event_bases = {}
    for col in result_columns:
        match = re.search(r'Result_(.*)_(Score|D|E|Rnk|Total)$', col)
        if match:
            raw_event_name = match.group(1)
            event_bases[raw_event_name] = raw_event_name
    
    print(f"Event Bases: {list(event_bases.keys())}")
    
    for row in reader:
        if "Anton Prosolin" in row['Name']:
            print(f"Athlete: {row['Name']}")
            for key in event_bases:
                score_val = row.get(f'Result_{key}_Score')
                d_val = row.get(f'Result_{key}_D')
                print(f"  {key}: Score={score_val!r}, D={d_val!r}")
