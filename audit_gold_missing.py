import sqlite3
import pandas as pd

DB_FILE = "gym_data.db"

def audit_missing_columns():
    with sqlite3.connect(DB_FILE) as conn:
        # Get all columns in Results (source of truth for dynamic schema)
        results_df = pd.read_sql_query("PRAGMA table_info(Results)", conn)
        all_results_cols = set(results_df['name'].tolist())

        # Columns explicitly included in Gold_MAG_Export (from create_gold_tables.py)
        # We know these are: level, age, province + identity/meet info
        # Identity/Meet info comes from other tables, so we focus on what comes from 'Results'
        # In create_gold_tables.py, 'r_base' joins level, age, province.
        
        included_service_cols = {'level', 'age', 'province', 'session', 'group', 'gender'} # Added session/group as likely candidates, will verify
        
        # Standard columns to ignore (keys, scores, ranks)
        ignored_cols = {
            'result_id', 'meet_db_id', 'athlete_id', 'apparatus_id', 
            'score_final', 'score_d', 'score_e', 'score_text', 
            'rank_numeric', 'rank_text', 'details_json'
        }

        missing_cols = []
        for col in all_results_cols:
            if col in included_service_cols:
                continue
            if col in ignored_cols:
                continue
            
            # Check if this column has ANY data for MAG athletes
            # (Optimization: We only care if it's relevant to MAG)
            has_data = pd.read_sql_query(f"""
                SELECT 1 FROM Results r
                JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                JOIN Disciplines d ON a.discipline_id = d.discipline_id
                WHERE d.discipline_name = 'MAG' AND "{col}" IS NOT NULL LIMIT 1
            """, conn)
            
            if not has_data.empty:
                missing_cols.append(col)

        print("--- Service Columns MISSING from Gold_MAG_Export ---")
        if missing_cols:
            for col in sorted(missing_cols):
                # Sample a value
                sample = pd.read_sql_query(f"""
                    SELECT "{col}" FROM Results r
                    JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                    JOIN Disciplines d ON a.discipline_id = d.discipline_id
                    WHERE d.discipline_name = 'MAG' AND "{col}" IS NOT NULL LIMIT 1
                """, conn).iloc[0,0]
                print(f"- {col} (Example: {sample})")
        else:
            print("None. All relevant MAG service columns are included.")

if __name__ == "__main__":
    audit_missing_columns()
