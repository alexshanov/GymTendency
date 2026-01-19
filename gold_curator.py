import sqlite3
import pandas as pd
import os
import json

def get_athlete_gold_data(db_path, athlete_name, meet_name):
    """
    Fetches MAG data for a specific athlete and meet, formatting it into a 7x3 grid.
    Columns: Floor, Pommel, Rings, Vault, P-Bars, H-Bar, All Around
    Rows: Final Score, D-Score, Rank
    """
    conn = sqlite3.connect(db_path)
    
    # Define apparatus order
    apparatus_cols = [
        ('fx', 'Floor'), 
        ('ph', 'Pommel'), 
        ('sr', 'Rings'), 
        ('vt', 'Vault'), 
        ('pb', 'P-Bars'), 
        ('hb', 'H-Bar'), 
        ('aa', 'All Around')
    ]
    
    query = f"""
    SELECT * FROM Silver_MAG_Export 
    WHERE LOWER(athlete_name) = LOWER(?) 
    AND meet_name = ?
    """
    
    df = pd.read_sql_query(query, conn, params=(athlete_name, meet_name))
    conn.close()
    
    if df.empty:
        return None

    # We take the first row (assuming one entry per meet/athlete)
    row = df.iloc[0]
    
    data = {
        'Metric': ['Final Score', 'D-Score', 'Rank']
    }
    
    for prefix, display_name in apparatus_cols:
        data[display_name] = [
            row.get(f'{prefix}_score'),
            row.get(f'{prefix}_d'),
            row.get(f'{prefix}_rank')
        ]
        
    gold_df = pd.DataFrame(data)
    return gold_df

def main():
    db_path = 'gym_data.db'
    output_dir = 'Gold_Curations'
    os.makedirs(output_dir, exist_ok=True)
    
    # Target athletes
    target_names = [
        'Anton Shanov',
        'Kipton Teare',
        'David Mykolaichuk',
        'Lachlan Teare'
    ]
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Discovery: Find all (Athlete, Meet) pairs for these targets in Silver table
    placeholders = ','.join(['?'] * len(target_names))
    query = f"""
        SELECT DISTINCT athlete_name, meet_name 
        FROM Silver_MAG_Export 
        WHERE athlete_name IN ({placeholders})
        ORDER BY athlete_name, meet_date DESC
    """
    
    targets = cursor.execute(query, target_names).fetchall()
    conn.close()
    
    print(f"Found {len(targets)} meet records for target athletes.")
    
    for name, meet in targets:
        print(f"Curating Gold data for {name} at {meet}...")
        gold_df = get_athlete_gold_data(db_path, name, meet)
        
        if gold_df is not None:
            # Sanitize filename
            safe_name = name.replace(' ', '_')
            safe_meet = "".join([c if c.isalnum() else "_" for c in meet])
            filename = f"Gold_{safe_name}_{safe_meet}.csv"
            filepath = os.path.join(output_dir, filename)
            gold_df.to_csv(filepath, index=False)
            print(f"  -> Saved to {filepath}")
        else:
            print(f"  -> No data found for {name} at {meet}")

if __name__ == "__main__":
    main()
