import sqlite3
import json
import time

DB_FILE = "gym_data.db"

CANONICAL_COLUMNS = {
    'result_id', 'meet_db_id', 'athlete_id', 'apparatus_id', 'gender', 
    'level', 'age', 'province', 'score_d', 'score_final', 'score_text', 
    'rank_numeric', 'rank_text', 'details_json', 'age_group', 'meet', 
    '"group"', 'state', 'session', 'num', 'bonus', 'execution_bonus', 
    'score_sv', 'score_e', 'penalty'
}

# The actual column names in the DB (unquoted for internal logic)
BASE_COLS = [
    'result_id', 'meet_db_id', 'athlete_id', 'apparatus_id', 'gender', 
    'level', 'age', 'province', 'score_d', 'score_final', 'score_text', 
    'rank_numeric', 'rank_text', 'details_json', 'age_group', 'meet', 
    'group', 'state', 'session', 'num', 'bonus', 'execution_bonus', 
    'score_sv', 'score_e', 'penalty'
]

def migrate():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print(f"--- Starting Migration of Results table in {DB_FILE} ---")
    
    # 1. Get all current columns
    cursor.execute("PRAGMA table_info(Results)")
    all_cols_info = cursor.fetchall()
    all_col_names = [info[1] for info in all_cols_info]
    
    garbage_cols = [c for c in all_col_names if c not in BASE_COLS]
    print(f"Found {len(all_col_names)} total columns.")
    print(f"Found {len(garbage_cols)} garbage columns to consolidate.")
    
    # 2. Create the new lean table
    print("Creating Results_v2 table...")
    cursor.execute("DROP TABLE IF EXISTS Results_v2")
    # Using the same schema as etl_functions.py
    cursor.execute("""
        CREATE TABLE Results_v2 (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            meet_db_id INTEGER NOT NULL,
            athlete_id INTEGER NOT NULL,
            apparatus_id INTEGER NOT NULL,
            gender TEXT,
            level TEXT,
            age REAL,
            province TEXT,
            score_d REAL,
            score_final REAL,
            score_text TEXT,
            rank_numeric INTEGER,
            rank_text TEXT,
            details_json TEXT,
            age_group TEXT,
            meet TEXT,
            "group" TEXT,
            state TEXT,
            session TEXT,
            num TEXT,
            bonus REAL,
            execution_bonus REAL,
            score_sv REAL,
            score_e REAL,
            penalty REAL,
            FOREIGN KEY (meet_db_id) REFERENCES Meets (meet_db_id),
            FOREIGN KEY (athlete_id) REFERENCES Athletes (athlete_id),
            FOREIGN KEY (apparatus_id) REFERENCES Apparatus (apparatus_id)
        )
    """)
    
    # 3. Migrate data in batches
    print("Migrating data (this may take a while)...")
    
    # We'll fetch everything from Results
    # To handle garbage columns, we'll select then in Python
    
    batch_size = 5000
    cursor.execute("SELECT count(*) FROM Results")
    total_rows = cursor.fetchone()[0]
    print(f"Total rows to migrate: {total_rows}")
    
    # Get indices for columns
    col_to_idx = {name: i for i, name in enumerate(all_col_names)}
    
    processed = 0
    start_time = time.time()
    
    # Create separate cursors for reading and writing
    reader = conn.cursor()
    writer = conn.cursor()
    
    # Fetch rows from Results
    reader.execute("SELECT * FROM Results")
    
    while True:
        rows = reader.fetchmany(batch_size)
        if not rows:
            break
            
        data_to_insert = []
        for row in rows:
            # Create the record for Results_v2
            record = {}
            for col in BASE_COLS:
                record[col] = row[col_to_idx[col]]
            
            # Extract garbage data
            misc = {}
            # If current details_json has data, parse it
            existing_json = record['details_json']
            if existing_json:
                try:
                    misc = json.loads(existing_json)
                except:
                    misc = {"old_details_error": str(existing_json)}
            
            # Add garbage col values if they are not NULL/empty
            for g_col in garbage_cols:
                val = row[col_to_idx[g_col]]
                if val is not None and str(val).strip() != '':
                    misc[g_col] = val
            
            record['details_json'] = json.dumps(misc) if misc else None
            
            # Prepare for insert (ordered by BASE_COLS)
            data_to_insert.append(tuple(record[col] for col in BASE_COLS))
        
        # Insert into v2
        placeholders = ', '.join(['?'] * len(BASE_COLS))
        quoted_cols = [f'"{c}"' if c == 'group' else c for c in BASE_COLS]
        sql = f"INSERT INTO Results_v2 ({', '.join(quoted_cols)}) VALUES ({placeholders})"
        writer.executemany(sql, data_to_insert)
        
        processed += len(rows)
        if processed % 10000 == 0 or processed == total_rows:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            print(f"  Processed {processed}/{total_rows} ({rate:.0f} rows/s)...")
            conn.commit()

    # 4. Verification
    writer.execute("SELECT count(*) FROM Results_v2")
    new_count = writer.fetchone()[0]
    print(f"Migration complete. Old count: {total_rows}, New count: {new_count}")
    
    if total_rows == new_count:
        print("Verification successful! Swapping tables...")
        cursor.execute("DROP TABLE Results")
        cursor.execute("ALTER TABLE Results_v2 RENAME TO Results")
        
        print("Recreating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_athlete ON Results (athlete_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_meet ON Results (meet_db_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_apparatus ON Results (apparatus_id)")
        
        conn.commit()
        print("Migration finished successfully.")
    else:
        print("CRITICAL ERROR: Row count mismatch! Aborting swap.")
        conn.rollback()

    conn.close()

if __name__ == "__main__":
    migrate()
