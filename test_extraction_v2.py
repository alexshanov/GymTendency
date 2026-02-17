
import sys
import os
print("Starting script...")
sys.stdout.flush()

import extraction_library
print("Imported library")
sys.stdout.flush()

filepath = "CSVs_Livemeet_final/D6A65C67D6FEE84AAF3BB44FDE4ECC93_FINAL_AA_MAG_Novice_Combined_MAG.csv"
meet_details = {'name': 'Test Meet', 'year': 2026}

print(f"Calling extraction on {filepath}...")
sys.stdout.flush()

result = extraction_library.extract_livemeet_data(filepath, meet_details)

print("Extraction Complete")
sys.stdout.flush()

if result and 'results' in result:
    print(f"Success! Found {len(result['results'])} athletes.")
else:
    print("Failure info:", result)
