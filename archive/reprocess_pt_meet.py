
import sqlite3
import os
import json
from extraction_library import extract_livemeet_data
from etl_functions import get_or_create_athlete, determine_age

def reprocess_file(filepath):
    conn = sqlite3.connect('gym_data.db')
    cursor = conn.cursor()
    
    # meet_db_id for Copeland 2022 was 118 (we deleted its results earlier)
    # We need to make sure we use that ID or find the meet again.
    # Let's find the meet ID first.
    cursor.execute("SELECT meet_db_id FROM Meets WHERE name LIKE 'Copeland Classic MAG 2022'")
    row = cursor.fetchone()
    if not row:
        print("Meet not found!")
        return
    meet_db_id = row[0]
    print(f"Reprocessing for meet_db_id: {meet_db_id}")
    
    meet_details = {'name': 'Copeland Classic MAG 2022', 'year': '2022'}
    extracted_data = extract_livemeet_data(filepath, meet_details)
    
    if not extracted_data or not extracted_data.get('results'):
        print("Extraction failed or no results found.")
        return

    for res in extracted_data['results']:
        athlete_name = res['raw_name']
        club_name = res['raw_club']
        full_level = "E2" # Hardcode for this file to be safe, or extract dynamically
        
        # Get Athlete ID
        athlete_id = get_or_create_athlete(cursor, athlete_name, club_name, '2022', full_level, 2)
        
        # Determine Age
        age = determine_age(res['dynamic_metadata'], '2022')
        
        for app in res['apparatus_results']:
            app_name = app['raw_event']
            # Map app name to ID
            cursor.execute("SELECT apparatus_id FROM Apparatus WHERE name = ?", (app_name,))
            app_row = cursor.fetchone()
            if not app_row:
                 # Try mapping
                 pass # simplified
                 continue
            app_id = app_row[0]
            
            cursor.execute("""
                INSERT INTO Results (
                    meet_db_id, athlete_id, apparatus_id,
                    level, age, gender, session,
                    score_final, score_d, score_e, rank_numeric, rank_text,
                    details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                meet_db_id, athlete_id, app_id,
                full_level, age, 'M', 'Combined',
                app['score_final'], app['score_d'], app.get('score_e'),
                None, app['rank_text'],  # Simplified rank handling
                json.dumps(app)
            ))
            
    conn.commit()
    conn.close()
    print("Reprocessing complete.")

if __name__ == "__main__":
    reprocess_file('CSVs_Livemeet_final/6BB3817D94653EE6A7DB325B26F91996_MESSY_BYEVENT_Elite_2_Combined_MAG.csv')
