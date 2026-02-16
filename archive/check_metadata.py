
import os
import sys
import json

# Add current dir to path
sys.path.append(os.getcwd())

import extraction_library

file1 = "CSVs_Livemeet_final/D6A65C67D6FEE84AAF3BB44FDE4ECC93_FINAL_AA_MAG_Novice_Novice_Day_1_-_Fri_Feb_6_8_00_am_MAG.csv"
file2 = "CSVs_Livemeet_final/D6A65C67D6FEE84AAF3BB44FDE4ECC93_FINAL_AA_MAG_Novice_Novice_Day_1_-_Fri_Feb_6_8_00_am_MAG.csv" # Wait, I meant Day 2
file2 = "CSVs_Livemeet_final/D6A65C67D6FEE84AAF3BB44FDE4ECC93_FINAL_AA_MAG_Novice_Novice_Day_2_-_Sat_Feb_7_4_00_pm_MAG.csv"

meet_details = {"name": "Test Meet"}

print("Extracting File 1...")
res1 = extraction_library.extract_livemeet_data(file1, meet_details)
if res1 and res1['results']:
    print(f"File 1 first athlete dynamic_metadata: {json.dumps(res1['results'][0]['dynamic_metadata'], indent=2)}")

print("\nExtracting File 2...")
res2 = extraction_library.extract_livemeet_data(file2, meet_details)
if res2 and res2['results']:
    print(f"File 2 first athlete dynamic_metadata: {json.dumps(res2['results'][0]['dynamic_metadata'], indent=2)}")
