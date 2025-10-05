# load_data.py

import sqlite3
import pandas as pd
import os
import glob
import traceback
import json
import re

# --- CONFIGURATION ---
DB_FILE = "gym_data.db"
MEETS_CSV_FILE = "discovered_meet_ids.csv"
FINAL_CSVS_DIR = "CSVs_final"

# --- DATABASE SETUP ---

def setup_database():
    """
    Creates all necessary tables if they don't exist and populates
    the definition tables (Disciplines, Events).
    This function makes the script runnable from scratch.
    """
    print("--- Setting up database schema and definitions ---")
    
    # The schema is defined here. Note the `details_json` column in Results,
    # which will hold our dynamic "middle" columns.
    schema_queries = [
        """
        CREATE TABLE IF NOT EXISTS Disciplines (
            discipline_id INTEGER PRIMARY KEY,
            discipline_name TEXT NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT NOT NULL,
            discipline_id INTEGER NOT NULL,
            sort_order INTEGER,
            FOREIGN KEY (discipline_id) REFERENCES Disciplines (discipline_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Athletes (
            athlete_id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            club TEXT,
            gender TEXT,
            UNIQUE(full_name, club)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Meets (
            meet_id TEXT PRIMARY KEY,
            name TEXT,
            start_date_iso TEXT,
            location TEXT,
            year INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            meet_id TEXT NOT NULL,
            athlete_id INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            score_d REAL,
            score_final REAL,
            score_text TEXT,
            rank_numeric INTEGER,
            rank_text TEXT,
            details_json TEXT, -- To store dynamic columns like Group, Age_Group, Level, etc.
            FOREIGN KEY (meet_id) REFERENCES Meets (meet_id),
            FOREIGN KEY (athlete_id) REFERENCES Athletes (athlete_id),
            FOREIGN KEY (event_id) REFERENCES Events (event_id)
        );
        """
    ]

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            for query in schema_queries:
                cursor.execute(query)
            
            # --- Populate Definition Tables ---
            
            disciplines = [
                (1, 'WAG'),
                (2, 'MAG'),
                (99, 'Other')
            ]
            cursor.executemany("INSERT OR IGNORE INTO Disciplines (discipline_id, discipline_name) VALUES (?, ?)", disciplines)

            # Clear, structured event definitions
            WAG_EVENTS = {'Vault': 1, 'Uneven Bars': 2, 'Beam': 3, 'Floor': 4}
            MAG_EVENTS = {'Floor': 1, 'Pommel Horse': 2, 'Rings': 3, 'Vault': 4, 'Parallel Bars': 5, 'High Bar': 6}
            OTHER_EVENTS = {'AllAround': 99, 'Physical Preparation': 100}

            all_events = []
            # WAG Events (discipline_id = 1)
            for name, order in WAG_EVENTS.items():
                all_events.append((name, 1, order))
            # MAG Events (discipline_id = 2)
            for name, order in MAG_EVENTS.items():
                all_events.append((name, 2, order))
            # Other Events (discipline_id = 99)
            for name, order in OTHER_EVENTS.items():
                all_events.append((name, 99, order))

            # Use "INSERT OR IGNORE" with a unique constraint on (event_name, discipline_id)
            # This is more complex to set up, so we'll do a check instead.
            for event_name, disc_id, sort_order in all_events:
                cursor.execute("SELECT 1 FROM Events WHERE event_name = ? AND discipline_id = ?", (event_name, disc_id))
                if cursor.fetchone() is None:
                    cursor.execute("INSERT INTO Events (event_name, discipline_id, sort_order) VALUES (?, ?, ?)", 
                                   (event_name, disc_id, sort_order))
            
            conn.commit()
        print("Database setup complete.")
        return True
    except Exception as e:
        print(f"Error during database setup: {e}")
        traceback.print_exc()
        return False


# --- DATA LOADING FUNCTIONS ---

def load_meets_data(conn):
    """
    Reads the meets CSV file and populates the 'Meets' table.
    This is a full replacement operation.
    """
    print("--- Loading meets data ---")

    # Step 1: Check if the source CSV file exists before trying to read it.
    if not os.path.exists(MEETS_CSV_FILE):
        print(f"Warning: Meets CSV file not found at '{MEETS_CSV_FILE}'. Skipping this step.")
        return

    try:
        # Step 2: Read the entire CSV file into a pandas DataFrame.
        df = pd.read_csv(MEETS_CSV_FILE)

        # Step 3: Select only the columns we need and rename them to match the
        # database table schema ('Meets' table: meet_id, name, start_date_iso, location, year).
        # This prevents errors if the CSV has extra columns and ensures consistency.
        df_for_db = df[['MeetID', 'MeetName', 'start_date_iso', 'Location', 'Year']].rename(columns={
            'MeetID': 'meet_id',
            'MeetName': 'name',
            'start_date_iso': 'start_date_iso',
            'Location': 'location',
            'Year': 'year'
        })

        # Step 4: Write the prepared DataFrame to the 'Meets' table in the database.
        # - 'Meets': The name of the table to write to.
        # - conn: The active database connection.
        # - if_exists='replace': This is the key part. It will DROP the table if it
        #   already exists and create a new one, ensuring a full replacement.
        # - index=False: Do not write the pandas DataFrame index as a column in the DB.
        df_for_db.to_sql('Meets', conn, if_exists='replace', index=False)

        print(f"Successfully loaded and replaced {len(df_for_db)} records in the 'Meets' table.")

    except KeyError as e:
        print(f"Error: A required column is missing from '{MEETS_CSV_FILE}'. Missing column: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while loading meets data: {e}")
        traceback.print_exc()

def process_results_files():
    """
    Main function to find and process all final result CSV files.
    """
    print("--- Starting to process result files ---")
    
    search_pattern = os.path.join(FINAL_CSVS_DIR, "*_FINAL_*.csv")
    csv_files = glob.glob(search_pattern)

    if not csv_files:
        print(f"Warning: No result files found matching pattern '{search_pattern}'.")
        return
        
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # Pre-load caches for performance
            # Creates a dictionary like: {('Athlete Name', 'Club Name'): 123}
            athlete_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT athlete_id, full_name, club FROM Athletes").fetchall()}
            event_cache = {(row[1], row[2]): row[0] for row in conn.execute("SELECT event_id, event_name, discipline_id FROM Events").fetchall()}

            for filepath in csv_files:
                print(f"\nProcessing file: {os.path.basename(filepath)}")
                parse_and_load_file(filepath, conn, athlete_cache, event_cache)

    except Exception as e:
        print(f"A critical error occurred during file processing: {e}")
        traceback.print_exc()

def parse_and_load_file(filepath, conn, athlete_cache, event_cache):
    
    """
    Parses a single CSV file and loads its data into the Athletes and Results tables.
    --- ИСПРАВЛЕНА ОШИБКА: Корректно обрабатывает 'rank' с текстом (например, '15T'). ---
    """
    try:
        df = pd.read_csv(filepath, keep_default_na=False) # keep_default_na=False - ВАЖНО
        if df.empty:
            print("File is empty. Skipping.")
            return
        if 'Name' not in df.columns:
            print("Warning: Mandatory 'Name' column not found. Skipping file.")
            return
    except (pd.errors.EmptyDataError, FileNotFoundError):
        print("Warning: File is empty or could not be read. Skipping.")
        return

    try:
        filename = os.path.basename(filepath)
        meet_id = filename.split('_FINAL_')[0]
    except IndexError:
        print(f"Warning: Filename '{filename}' does not match expected format. Skipping.")
        return

    discipline_id, discipline_name, gender_heuristic = detect_discipline(df)
    print(f"Detected Discipline: {discipline_name}")

    core_column = 'Name'
    result_columns = [col for col in df.columns if col.startswith('Result_')]
    dynamic_columns = [col for col in df.columns if col != core_column and col not in result_columns]

    event_name_regex = re.compile(r'Result_(.*)_Score')
    event_bases = {}
    for col in result_columns:
        match = event_name_regex.match(col)
        if match:
            raw_event_name = match.group(1)
            clean_event_name = raw_event_name.replace('_', ' ')
            event_bases[clean_event_name] = raw_event_name

    athletes_processed = 0
    results_inserted = 0
    cursor = conn.cursor()

    for index, row in df.iterrows():
        athlete_name = row.get(core_column)
        if not athlete_name or not str(athlete_name).strip():
            print(f"Warning: Skipping row {index+2} due to missing athlete name.")
            continue
        
        athletes_processed += 1
        athlete_id = get_or_create_athlete(conn, row, gender_heuristic, athlete_cache)
        
        details_dict = {col: (row.get(col) if str(row.get(col)) else None) for col in dynamic_columns}
        details_json = json.dumps(details_dict)

        for clean_name, raw_name in event_bases.items():
            event_key = (clean_name, discipline_id)
            if event_key not in event_cache:
                event_key = (clean_name, 99) 
            if event_key not in event_cache:
                print(f"Warning: Event '{clean_name}' for discipline '{discipline_name}' not in definitions. Skipping.")
                continue
            event_id = event_cache[event_key]
            
            d_col, score_col, rank_col = f'Result_{raw_name}_D', f'Result_{raw_name}_Score', f'Result_{raw_name}_Rnk'
            d_val, score_val, rank_val = row.get(d_col), row.get(score_col), row.get(rank_col)
            
            # --- НАДЕЖНАЯ ОБРАБОТКА ЗНАЧЕНИЙ ЗДЕСЬ ---
            
            # Обработка Score
            score_numeric = pd.to_numeric(score_val, errors='coerce')
            score_text = None if pd.notna(score_numeric) else (str(score_val) if str(score_val).strip() else None)

            # Обработка Rank
            rank_numeric = None
            rank_text = None
            if rank_val and str(rank_val).strip():
                # Пытаемся преобразовать в число. Если получается - отлично.
                temp_rank_num = pd.to_numeric(rank_val, errors='coerce')
                if pd.notna(temp_rank_num):
                    rank_numeric = int(temp_rank_num)
                else:
                    # Если не получилось - это текст. Сохраняем его.
                    rank_text = str(rank_val)
                    # И пытаемся извлечь из него число.
                    cleaned_rank_str = re.sub(r'\D', '', str(rank_val))
                    if cleaned_rank_str:
                        rank_numeric = int(cleaned_rank_str)

            # Обработка D-score
            d_numeric = pd.to_numeric(d_val, errors='coerce')

            cursor.execute("""
                INSERT INTO Results (
                    meet_id, athlete_id, event_id, 
                    score_d, score_final, score_text, 
                    rank_numeric, rank_text, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                meet_id, athlete_id, event_id,
                d_numeric, score_numeric, score_text,
                rank_numeric, rank_text, details_json
            ))
            results_inserted += 1

    conn.commit()
    print(f"Processed {athletes_processed} athletes, inserting {results_inserted} result records.")
def detect_discipline(df):
    """
    Analyzes the DataFrame's columns to determine if it's WAG or MAG.
    Returns: (discipline_id, discipline_name, gender_heuristic)
    """
    column_names = set(df.columns)
    
    # These are apparatus unique to each discipline
    MAG_INDICATORS = {'Pommel_Horse', 'Rings', 'Parallel_Bars', 'High_Bar'}
    WAG_INDICATORS = {'Uneven_Bars', 'Beam'}

    for col in column_names:
        # Check if any unique MAG apparatus is mentioned in a column name
        if any(indicator in col for indicator in MAG_INDICATORS):
            return 2, 'MAG', 'M'
        # Check if any unique WAG apparatus is mentioned in a column name
        if any(indicator in col for indicator in WAG_INDICATORS):
            return 1, 'WAG', 'F'
            
    # If no unique indicators are found, we can't determine the discipline
    return 99, 'Other', 'Unknown'


def get_or_create_athlete(conn, row, gender_heuristic, athlete_cache):
    """
    Finds an athlete in the cache/DB or creates a new one.
    Handles potentially missing 'Club' data.
    Returns: athlete_id
    """
    cursor = conn.cursor()
    # Clean the name and club data from the row
    name = str(row.get('Name')).strip()
    club_raw = row.get('Club')
    club = str(club_raw).strip() if pd.notna(club_raw) and str(club_raw).strip() else None

    # Use a tuple of (name, club) as the unique key for an athlete
    athlete_key = (name, club)

    # 1. Check the cache first for performance
    if athlete_key in athlete_cache:
        return athlete_cache[athlete_key]

    # 2. If not in cache, try to find in DB (in case cache is out of sync)
    if club is None:
        cursor.execute("SELECT athlete_id FROM Athletes WHERE full_name = ? AND club IS NULL", (name,))
    else:
        cursor.execute("SELECT athlete_id FROM Athletes WHERE full_name = ? AND club = ?", (name, club))
    
    result = cursor.fetchone()
    if result:
        athlete_id = result[0]
        athlete_cache[athlete_key] = athlete_id # Update cache
        return athlete_id

    # 3. If not found, create a new athlete record
    cursor.execute(
        "INSERT INTO Athletes (full_name, club, gender) VALUES (?, ?, ?)",
        (name, club, gender_heuristic)
    )
    new_athlete_id = cursor.lastrowid
    athlete_cache[athlete_key] = new_athlete_id # Add new athlete to cache
    return new_athlete_id


# --- MAIN EXECUTION ---

if __name__ == "__main__":
    if setup_database():
        # Connect once to pass the connection object around
        with sqlite3.connect(DB_FILE) as main_conn:
            load_meets_data(main_conn)
        
        # This function will manage its own connection for batch processing
        process_results_files()
        
        print("\n--- Data loading script finished ---")