
import sys
import os
sys.path.append(os.getcwd())
import extraction_library
import json

filepath = "CSVs_Livemeet_final/D6A65C67D6FEE84AAF3BB44FDE4ECC93_FINAL_AA_MAG_Novice_Combined_MAG.csv"
meet_details = {'name': 'Test Meet', 'year': 2026}

result = extraction_library.extract_livemeet_data(filepath, meet_details)

print("Extraction Complete")

if result and 'results' in result:
    print(f"Total results extracted: {len(result['results'])}")
    for res in result['results']:
        if 'Anton Prosolin' in res['raw_name']:
            print(f"Athlete identified: {res['raw_name']}")
            print(f"Discipline ID: {res['discipline_id']}")
            print(f"Gender: {res['gender_heuristic']}")
            for app in res['apparatus_results']:
                score = app['score_final']
                d_score = app['score_d']
                print(f"  Event '{app['raw_event']}': score_final={score!r} (type {type(score)}), score_d={d_score!r} (type {type(d_score)})")
else:
    print("No results extracted")
