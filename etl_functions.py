# etl_functions.py

import sqlite3
import pandas as pd
import os
import json
import traceback

# ==============================================================================
#  DATABASE SETUP AND DEFINITIONS
# ==============================================================================

def setup_database(db_file):
    """
    Creates the new, professional database schema with Persons, Clubs,
    and a linking Athletes table.
    """
    print("--- Setting up new professional database schema ---")
    
    # Drop tables in reverse order of dependency to avoid errors
    drop_queries = [
        "DROP TABLE IF EXISTS Results;",
        "DROP TABLE IF EXISTS Athletes;",
        "DROP TABLE IF EXISTS Persons;",
        "DROP TABLE IF EXISTS Clubs;",
        "DROP TABLE IF EXISTS Apparatus;",
        "DROP TABLE IF EXISTS Events;", 
        "DROP TABLE IF EXISTS Disciplines;"
    ]

    schema_queries = [
        "CREATE TABLE IF NOT EXISTS Disciplines (discipline_id INTEGER PRIMARY KEY, discipline_name TEXT NOT NULL UNIQUE);",
        "CREATE TABLE IF NOT EXISTS Apparatus (apparatus_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, discipline_id INTEGER NOT NULL, sort_order INTEGER, FOREIGN KEY (discipline_id) REFERENCES Disciplines (discipline_id), UNIQUE(name, discipline_id));",
        
        # --- NEW SCHEMA TABLES ---
        """CREATE TABLE IF NOT EXISTS Persons (
            person_id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL UNIQUE,
            gender TEXT,
            dob TEXT
        );""",
        """CREATE TABLE IF NOT EXISTS Clubs (
            club_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );""",
        """CREATE TABLE IF NOT EXISTS Athletes (
            athlete_id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER NOT NULL,
            club_id INTEGER,
            FOREIGN KEY (person_id) REFERENCES Persons (person_id),
            FOREIGN KEY (club_id) REFERENCES Clubs (club_id),
            UNIQUE(person_id, club_id)
        );""",
        # --- END OF NEW SCHEMA TABLES ---

        """CREATE TABLE IF NOT EXISTS Meets (
            meet_db_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_meet_id TEXT NOT NULL,
            name TEXT,
            start_date_iso TEXT,
            location TEXT,
            year INTEGER,
            UNIQUE(source, source_meet_id)
        );""",
        """CREATE TABLE IF NOT EXISTS Results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            meet_db_id INTEGER NOT NULL,
            athlete_id INTEGER NOT NULL, -- This now links to the new Athletes table
            apparatus_id INTEGER NOT NULL,
            score_d REAL,
            score_final REAL,
            score_text TEXT,
            rank_numeric INTEGER,
            rank_text TEXT,
            details_json TEXT,
            FOREIGN KEY (meet_db_id) REFERENCES Meets (meet_db_id),
            FOREIGN KEY (athlete_id) REFERENCES Athletes (athlete_id),
            FOREIGN KEY (apparatus_id) REFERENCES Apparatus (apparatus_id)
        );"""
    ]

    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            print("Dropping old tables to ensure a clean slate...")
            for query in drop_queries: cursor.execute(query)
            
            print("Creating new tables with professional schema...")
            for query in schema_queries: cursor.execute(query)
            
            disciplines = [(1, 'WAG'), (2, 'MAG'), (99, 'Other')]
            cursor.executemany("INSERT OR IGNORE INTO Disciplines (discipline_id, discipline_name) VALUES (?, ?)", disciplines)
            WAG_EVENTS = {'Vault': 1, 'Uneven Bars': 2, 'Beam': 3, 'Floor': 4}
            MAG_EVENTS = {'Floor': 1, 'Pommel Horse': 2, 'Rings': 3, 'Vault': 4, 'Parallel Bars': 5, 'High Bar': 6}
            OTHER_EVENTS = {'AllAround': 99, 'All-Around': 99}
            all_apparatus = []
            for name, order in WAG_EVENTS.items(): all_apparatus.append((name, 1, order))
            for name, order in MAG_EVENTS.items(): all_apparatus.append((name, 2, order))
            for name, order in OTHER_EVENTS.items(): all_apparatus.append((name, 99, order))
            cursor.executemany("INSERT OR IGNORE INTO Apparatus (name, discipline_id, sort_order) VALUES (?, ?, ?)", all_apparatus)
            
            conn.commit()
        print("Database setup complete.")
        return True
    except Exception as e:
        print(f"Error during database setup: {e}"); traceback.print_exc(); return False

# ==============================================================================
#  GENERIC HELPER FUNCTIONS
# ==============================================================================

def load_club_aliases(filepath="club_aliases.json"):
    try:
        with open(filepath, 'r') as f:
            aliases = json.load(f)
            return {key.title(): value for key, value in aliases.items()}
    except FileNotFoundError:
        print(f"Warning: '{filepath}' not found. No club aliases will be applied."); return {}
    except json.JSONDecodeError:
        print(f"Error: Could not parse '{filepath}'."); return {}

def standardize_club_name(club_str, alias_map):
    if not club_str or not isinstance(club_str, str): return None
    cleaned_club = club_str.strip().title()
    return alias_map.get(cleaned_club, cleaned_club)

def standardize_athlete_name(name_str):
    if not isinstance(name_str, str) or not name_str.strip(): return None
    name_str = name_str.strip()
    if ',' in name_str:
        parts = [p.strip() for p in name_str.split(',', 1)]
        if len(parts) == 2: return f"{parts[1].title()} {parts[0].title()}"
    words = name_str.split()
    if len(words) > 1 and words[0].isupper() and all(c.isupper() for c in words[0]) and any(c.islower() for c in ' '.join(words[1:])):
        return f"{' '.join(words[1:])} {words[0].title()}"
    return ' '.join(word.capitalize() for word in words)

def detect_discipline(df):
    column_names = set(df.columns)
    MAG_INDICATORS = {'Pommel_Horse', 'Rings', 'Parallel_Bars', 'High_Bar'}
    WAG_INDICATORS = {'Uneven_Bars', 'Beam'}
    for col in column_names:
        if any(indicator in col for indicator in MAG_INDICATORS): return 2, 'MAG', 'M'
        if any(indicator in col for indicator in WAG_INDICATORS): return 1, 'WAG', 'F'
    return 99, 'Other', 'Unknown'

# ==============================================================================
#  NEW DATABASE INTERACTION FUNCTIONS
# ==============================================================================

def get_or_create_person(conn, full_name, gender, cache):
    if full_name in cache: return cache[full_name]
    cursor = conn.cursor()
    cursor.execute("SELECT person_id FROM Persons WHERE full_name = ?", (full_name,))
    result = cursor.fetchone()
    if result:
        person_id = result[0]
    else:
        cursor.execute("INSERT INTO Persons (full_name, gender) VALUES (?, ?)", (full_name, gender))
        person_id = cursor.lastrowid
    cache[full_name] = person_id
    return person_id

def get_or_create_club(conn, club_name, cache):
    if club_name is None: return None # Handle athletes with no club
    if club_name in cache: return cache[club_name]
    cursor = conn.cursor()
    cursor.execute("SELECT club_id FROM Clubs WHERE name = ?", (club_name,))
    result = cursor.fetchone()
    if result:
        club_id = result[0]
    else:
        cursor.execute("INSERT INTO Clubs (name) VALUES (?)", (club_name,))
        club_id = cursor.lastrowid
    cache[club_name] = club_id
    return club_id

def get_or_create_athlete_link(conn, person_id, club_id, cache):
    athlete_key = (person_id, club_id)
    if athlete_key in cache: return cache[athlete_key]
    cursor = conn.cursor()
    if club_id is None:
        cursor.execute("SELECT athlete_id FROM Athletes WHERE person_id = ? AND club_id IS NULL", (person_id,))
    else:
        cursor.execute("SELECT athlete_id FROM Athletes WHERE person_id = ? AND club_id = ?", athlete_key)
    result = cursor.fetchone()
    if result:
        athlete_id = result[0]
    else:
        cursor.execute("INSERT INTO Athletes (person_id, club_id) VALUES (?, ?)", athlete_key)
        athlete_id = cursor.lastrowid
    cache[athlete_key] = athlete_id
    return athlete_id

def get_or_create_meet(conn, source, source_meet_id, meet_details, cache):
    meet_key = (source, source_meet_id)
    if meet_key in cache: return cache[meet_key]
    cursor = conn.cursor()
    cursor.execute("SELECT meet_db_id FROM Meets WHERE source = ? AND source_meet_id = ?", meet_key)
    result = cursor.fetchone()
    if result:
        meet_db_id = result[0]
    else:
        cursor.execute("INSERT INTO Meets (source, source_meet_id, name, start_date_iso, location, year) VALUES (?, ?, ?, ?, ?, ?)",
            (source, source_meet_id, meet_details.get('name'), meet_details.get('start_date_iso'), meet_details.get('location'), meet_details.get('year')))
        meet_db_id = cursor.lastrowid
        print(f"  -> New meet added: '{meet_details.get('name')}' (ID: {meet_db_id})")
    cache[meet_key] = meet_db_id
    return meet_db_id