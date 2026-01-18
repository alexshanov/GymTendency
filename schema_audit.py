import sqlite3
import pandas as pd

DB_FILE = "gym_data.db"

def inspect_schema():
    print(f"--- Inspecting 'Results' Table Schema in {DB_FILE} ---")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Get all columns
        cursor.execute("PRAGMA table_info(Results)")
        columns_info = cursor.fetchall()
        all_columns = [col[1] for col in columns_info]
        
        # Define standard columns (The "Normal" ones)
        standard_columns = [
            'result_id', 'meet_db_id', 'athlete_id', 'apparatus_id', 
            'score_final', 'score_d', 'score_e', 'score_nd', 
            'rank_numeric', 'rank_text', 'gender', 'created_at'
        ]
        
        # Identify "Weird" (Dynamic) columns
        dynamic_columns = [col for col in all_columns if col not in standard_columns]
        
        print("\n✅ STANDARD COLUMNS:")
        print(", ".join(standard_columns))
        
        print(f"\n✨ DYNAMIC ('WEIRD') COLUMNS ({len(dynamic_columns)} found):")
        if dynamic_columns:
            for col in sorted(dynamic_columns):
                print(f"  - {col}")
                
            # Show a sample of data for these columns
            print("\n--- DATA SAMPLE (First 5 Non-Null Rows) ---")
            for col in dynamic_columns:
                print(f"\nColumn: '{col}'")
                # Quote column name for safety (e.g. "group" is a reserved word)
                query = f'SELECT "{col}" FROM Results WHERE "{col}" IS NOT NULL AND "{col}" != "" LIMIT 5'
                rows = cursor.execute(query).fetchall()
                if rows:
                    for r in rows:
                        print(f"  Value: {r[0]}")
                else:
                    print("  (No data found)")
                    
        else:
            print("  (None found)")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    inspect_schema()
