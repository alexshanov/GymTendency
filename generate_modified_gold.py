import sqlite3
import json
import os
import argparse
import pandas as pd

DB_PATH = 'gym_data.db'
ROSTER_PATH = 'internal_roster.json'
ALIAS_PATH = 'person_aliases.json'
CLUB_ALIAS_PATH = 'club_aliases.json'

def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, 'r') as f:
        return json.load(f)

def generate_modified_gold(db_path=DB_PATH):
    if not os.path.exists(ROSTER_PATH):
        print(f"Error: {ROSTER_PATH} not found.")
        return

    person_aliases = load_json(ALIAS_PATH)
    club_aliases = load_json(CLUB_ALIAS_PATH)
    with open(ROSTER_PATH, 'r') as f:
        roster = json.load(f)

    # Get the list of canonical names from the roster
    target_names = set([a['full_name'] for a in roster if a['full_name'] != '-'])
    
    print(f"Targeting {len(target_names)} unique full names from roster.")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout = 60000")
    
    # 1. Load the main Gold table into a DataFrame for easier manipulation
    print("Loading Gold_Results_MAG into memory...")
    df = pd.read_sql_query("SELECT * FROM Gold_Results_MAG", conn)
    
    # 2. Apply Aliases
    print("Applying person aliases...")
    df['athlete_name'] = df['athlete_name'].map(lambda x: person_aliases.get(x, x))
    
    print("Applying club aliases...")
    df['club'] = df['club'].map(lambda x: club_aliases.get(x, x))
    
    # 3. Deduplicate (Aliases might have caused new duplicates)
    # We keep the row with the most non-null scores
    score_cols = [c for c in df.columns if c.endswith('_score')]
    df['non_null_count'] = df[score_cols].notnull().sum(axis=1)
    
    # Sort to keep the best record first
    df = df.sort_values(by=['athlete_name', 'date', 'meet_name', 'level', 'age', 'non_null_count'], ascending=[True, True, True, True, True, False])
    df = df.drop_duplicates(subset=['athlete_name', 'date', 'meet_name', 'level', 'age'], keep='first')
    df = df.drop(columns=['non_null_count'])

    # 4. Filter for L1 (Athletes in Roster)
    print("Creating Gold_Results_MAG_Filtered_L1...")
    l1_df = df[df['athlete_name'].isin(target_names)]
    l1_df.to_sql("Gold_Results_MAG_Filtered_L1", conn, if_exists="replace", index=False)
    print(f"L1 created with {len(l1_df)} rows.")

    # 5. Filter for L2 (Roster athletes + their session peers)
    print("Creating Gold_Results_MAG_Filtered_L2...")
    # Define session groups based on the roster athletes' sessions
    target_sessions = l1_df[['meet_name', 'level', 'age']].drop_duplicates()
    target_sessions['age'] = target_sessions['age'].fillna('')
    
    # Merge back to the full dataset to find peers
    df_with_age = df.copy()
    df_with_age['age_match'] = df_with_age['age'].fillna('')
    
    l2_df = pd.merge(
        df_with_age, 
        target_sessions, 
        left_on=['meet_name', 'level', 'age_match'], 
        right_on=['meet_name', 'level', 'age'],
        suffixes=('', '_r')
    )
    
    # Cleanup extra columns from merge
    l2_df = l2_df[df.columns]
    
    l2_df.to_sql("Gold_Results_MAG_Filtered_L2", conn, if_exists="replace", index=False)
    print(f"L2 created with {len(l2_df)} rows.")

    conn.commit()
    conn.close()
    print("Done!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-file", type=str, default=DB_PATH, help="Path to SQLite database")
    args = parser.parse_args()
    
    generate_modified_gold(db_path=args.db_file)
