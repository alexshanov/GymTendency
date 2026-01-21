# etl_functions.py

import sqlite3
import pandas as pd
import os
import json
import traceback
import re
import hashlib
from datetime import datetime

# ==============================================================================
#  T&T (TRAMPOLINE & TUMBLING) EXCLUSION
#  These patterns identify non-artistic gymnastics meets to be excluded
# ==============================================================================
TT_EXCLUSION_PATTERNS = [
    r'\bT\s*&\s*T\b',           # T&T, T & T
    r'\bT\s*N\s*T\b',           # TNT, T N T
    r'\btrampoline\b',          # trampoline
    r'\btumbling\b',            # tumbling
    r'\bdouble\s*mini\b',       # double mini
    r'\bdmt\b',                 # DMT (Double Mini Trampoline)
    r'\bpower\s*tumbling\b',    # power tumbling
    r'\bsynchro\s*tramp',       # synchro trampoline
]

def is_tt_meet(meet_name):
    """
    Check if a meet name indicates a Trampoline & Tumbling meet.
    Returns True if it should be EXCLUDED from processing.
    """
    if not meet_name:
        return False
    name_lower = meet_name.lower()
    for pattern in TT_EXCLUSION_PATTERNS:
        if re.search(pattern, name_lower, re.IGNORECASE):
            return True
    return False

# ==============================================================================
#  COUNTRY DETECTION
#  Detect meet country from location strings, source, or other indicators
# ==============================================================================
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL',
    'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT',
    'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
    'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'
}

CANADIAN_PROVINCES = {
    'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT',
    # Common variations
    'ALBERTA', 'BRITISH COLUMBIA', 'MANITOBA', 'NEW BRUNSWICK', 'NEWFOUNDLAND',
    'NOVA SCOTIA', 'ONTARIO', 'PRINCE EDWARD ISLAND', 'QUEBEC', 'SASKATCHEWAN'
}

def detect_country(location=None, source=None, meet_name=None):
    """
    Detect the country for a meet based on available information.
    Returns 'USA', 'CAN', or None if unable to determine.
    
    Priority:
    1. Source-based (K-Score is predominantly Canadian)
    2. Location-based (state/province codes)
    3. Meet name patterns
    """
    # 1. Source-based detection
    if source:
        source_lower = source.lower()
        if source_lower == 'kscore':
            return 'CAN'  # K-Score is primarily Canadian
        # MSO and LiveMeet are primarily US but can have Canadian meets
    
    # 2. Location-based detection
    if location:
        loc_upper = location.upper().strip()
        
        # Check for exact state/province code match
        loc_parts = re.split(r'[,\s]+', loc_upper)
        for part in loc_parts:
            part_clean = part.strip()
            if part_clean in US_STATES:
                return 'USA'
            if part_clean in CANADIAN_PROVINCES:
                return 'CAN'
        
        # Check for province names in location string
        for prov in ['ONTARIO', 'QUEBEC', 'ALBERTA', 'BRITISH COLUMBIA', 'MANITOBA', 
                     'SASKATCHEWAN', 'NOVA SCOTIA', 'NEW BRUNSWICK']:
            if prov in loc_upper:
                return 'CAN'
    
    # 3. Meet name patterns (last resort)
    if meet_name:
        name_upper = meet_name.upper()
        if 'CANADIAN' in name_upper or 'CANADA' in name_upper:
            return 'CAN'
        if 'USA' in name_upper or 'USAG' in name_upper or 'AAU' in name_upper:
            return 'USA'
    
    return None  # Unable to determine

# ==============================================================================
#  DATABASE SETUP AND DEFINITIONS
# ==============================================================================



def load_column_aliases(filepath="column_aliases.json"):
    """
    Loads column aliases from a JSON file.
    """
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"Error: Could not parse '{filepath}'.")
        return {}

# Load on module import
COLUMN_ALIASES = load_column_aliases()

def load_person_aliases(filepath="person_aliases.json"):
    """
    Loads manual person aliases from a JSON file.
    Example: {"Trinadad Mirabelle": "Trinidad Mirabelle"}
    """
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"Error: Could not parse '{filepath}'.")
        return {}

PERSON_ALIASES = load_person_aliases()

def load_club_aliases(filepath="club_aliases.json"):
    """
    Loads manual club aliases from a JSON file.
    Example: {"Flicka Gymnastics Club": "Flicka"}
    """
    try:
        with open(filepath, 'r') as f:
            aliases = json.load(f)
            # Ensure keys are title-cased for case-insensitive matching
            return {key.strip().title(): value.strip() for key, value in aliases.items()}
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"Error: Could not parse '{filepath}'.")
        return {}

CLUB_ALIASES = load_club_aliases()

def sanitize_column_name(col_name):
    """
    Sanitizes a raw column name to be SQL-safe (snake_case).
    Checks centralized alias map first.
    Example: "Start Value" -> "start_value", "Sess" -> "session"
    """
    if not col_name: return "col_unknown"
    
    # 1. Check strict alias map first
    raw_key = str(col_name).strip()
    if raw_key in COLUMN_ALIASES:
        return COLUMN_ALIASES[raw_key]
        
    # 2. Check case-insensitive alias map
    # (Pre-computing this ideally, but doing it here for simplicity)
    for alias_key, target in COLUMN_ALIASES.items():
        if alias_key.lower() == raw_key.lower():
            return target

    # 3. Standard sanitation
    clean = raw_key.lower()
    clean = clean.replace('#', 'num').replace('%', 'pct')
    clean = re.sub(r'[^a-z0-9]+', '_', clean)
    clean = clean.strip('_')
    return clean if clean else "col_unknown"

def ensure_column_exists(cursor, table_name, column_name, col_type='TEXT'):
    """
    Checks if a column exists in the table. If not, adds it dynamically.
    Returns True if column was added or already exists.
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    
    if column_name not in columns:
        print(f"  -> Schema Evolution: Adding column '{column_name}' to '{table_name}'")
        try:
            # SQLite does not support adding columns safely inside a transaction if it's potentially locked,
            # but usually allowed. ALTER TABLE ADD COLUMN is atomic in newer SQLite.
            # Quote the identifier to handle reserved words like "group" or "order"
            cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {col_type}')
            return True
        except Exception as e:
            print(f"  -> Error adding column {column_name}: {e}")
            return False
    return True

def setup_database(db_file):
    """
    Creates the new, professional database schema with Persons, Clubs,
    and a linking Athletes table.
    """
    print("--- Setting up new professional database schema ---")
    
    # schema_queries = [ # Moved below
    
    # We use CREATE TABLE IF NOT EXISTS, so no need to drop unless explicitly requested.
    # drop_queries = [
    #     "DROP TABLE IF EXISTS ScoringStandards;",
    #     "DROP TABLE IF EXISTS Results;",
    #     "DROP TABLE IF EXISTS Athletes;",
    #     "DROP TABLE IF EXISTS Persons;",
    #     "DROP TABLE IF EXISTS Clubs;",
    #     "DROP TABLE IF EXISTS Apparatus;",
    #     "DROP TABLE IF EXISTS Events;", 
    #     "DROP TABLE IF EXISTS Disciplines;"
    # ]

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
        """CREATE TABLE IF NOT EXISTS PersonAliases (
            alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias_name TEXT NOT NULL UNIQUE,
            canonical_person_id INTEGER NOT NULL,
            FOREIGN KEY (canonical_person_id) REFERENCES Persons (person_id)
        );""",
        """CREATE TABLE IF NOT EXISTS ClubAliases (
            alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
            club_alias_name TEXT NOT NULL UNIQUE,
            canonical_club_id INTEGER NOT NULL,
            FOREIGN KEY (canonical_club_id) REFERENCES Clubs (club_id)
        );""",
        # --- END OF NEW SCHEMA TABLES ---

        """CREATE TABLE IF NOT EXISTS Meets (
            meet_db_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_meet_id TEXT NOT NULL,
            name TEXT,
            start_date_iso TEXT,
            comp_year INTEGER,
            location TEXT,
            country TEXT,
            competition_type TEXT,
            UNIQUE(source, source_meet_id)
        );""",

        """CREATE TABLE IF NOT EXISTS Results (
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
        );""",
        """CREATE TABLE IF NOT EXISTS ProcessedFiles (
            file_path TEXT PRIMARY KEY,
            file_hash TEXT,
            last_processed TIMESTAMP
        );""",
        """CREATE TABLE IF NOT EXISTS ScrapeErrors (
            error_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_meet_id TEXT,
            error_message TEXT,
            error_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        );""",

        """CREATE TABLE IF NOT EXISTS ScoringStandards (
            standard_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT NOT NULL,
            level_system TEXT NOT NULL,
            level_name TEXT NOT NULL,
            max_score REAL,
            has_d_score BOOLEAN DEFAULT 0,
            UNIQUE(country, level_system, level_name)
        );"""
    ]

    # Initial data for Scoring Standards
    scoring_data = [
        # USA JO / DP (Development Program)
        ('USA', 'USAG_DP', 'Level 1', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 2', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 3', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 4', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 5', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 6', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 7', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 8', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 9', 10.0, 0),
        ('USA', 'USAG_DP', 'Level 10', 10.0, 0), # Max 10.0 (with bonus)
        
        # USA Xcel
        ('USA', 'USAG_XCEL', 'Bronze', 10.0, 0),
        ('USA', 'USAG_XCEL', 'Silver', 10.0, 0),
        ('USA', 'USAG_XCEL', 'Gold', 10.0, 0),
        ('USA', 'USAG_XCEL', 'Platinum', 10.0, 0),
        ('USA', 'USAG_XCEL', 'Diamond', 10.0, 0),
        ('USA', 'USAG_XCEL', 'Sapphire', 10.0, 0),

        # Canada CCP (similar to US DP)
        ('CAN', 'CAN_CCP', 'Level 1', 10.0, 0),
        ('CAN', 'CAN_CCP', 'Level 2', 10.0, 0),
        ('CAN', 'CAN_CCP', 'Level 3', 10.0, 0),
        ('CAN', 'CAN_CCP', 'Level 4', 10.0, 0),
        ('CAN', 'CAN_CCP', 'Level 5', 10.0, 0),
        ('CAN', 'CAN_CCP', 'Level 6', 10.0, 0),
        ('CAN', 'CAN_CCP', 'Level 7', 10.0, 0),
        ('CAN', 'CAN_CCP', 'Level 8', 10.0, 0),
        ('CAN', 'CAN_CCP', 'Level 9', 10.0, 0), # 10.0 start value
        ('CAN', 'CAN_CCP', 'Level 10', 10.0, 0),

        # Canada Xcel (Adopted)
        ('CAN', 'CAN_XCEL', 'Bronze', 10.0, 0),
        ('CAN', 'CAN_XCEL', 'Silver', 10.0, 0),
        ('CAN', 'CAN_XCEL', 'Gold', 10.0, 0),
        ('CAN', 'CAN_XCEL', 'Platinum', 10.0, 0),
        ('CAN', 'CAN_XCEL', 'Diamond', 10.0, 0),

        # Canada Aspire / HP (High Performance) - Open ended / FIG
        ('CAN', 'CAN_HP', 'Novice', None, 1),
        ('CAN', 'CAN_HP', 'Junior', None, 1),
        ('CAN', 'CAN_HP', 'Senior', None, 1),
        ('USA', 'USAG_ELITE', 'Junior', None, 1),
        ('USA', 'USAG_ELITE', 'Senior', None, 1),
    ]

    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            # for query in drop_queries: cursor.execute(query) # Skip dropping
            
            print("Creating new tables with professional schema...")
            for query in schema_queries: cursor.execute(query)
            
            # Populate Scoring Standards
            print(f"Populating ScoringStandards with {len(scoring_data)} reference records...")
            cursor.executemany("""
                INSERT OR IGNORE INTO ScoringStandards (country, level_system, level_name, max_score, has_d_score)
                VALUES (?, ?, ?, ?, ?)
            """, scoring_data)
            
            disciplines = [(1, 'WAG'), (2, 'MAG'), (99, 'Other')]
            cursor.executemany("INSERT OR IGNORE INTO Disciplines (discipline_id, discipline_name) VALUES (?, ?)", disciplines)
            WAG_EVENTS = {'Vault': 1, 'Uneven Bars': 2, 'Beam': 3, 'Floor': 4, 'All Around': 99}
            MAG_EVENTS = {'Floor': 1, 'Pommel Horse': 2, 'Rings': 3, 'Vault': 4, 'Parallel Bars': 5, 'High Bar': 6, 'All Around': 99}
            OTHER_EVENTS = {'All Around': 99}
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

# Removed load_club_aliases (moved to top)

def standardize_club_name(club_str, alias_map):
    if not club_str or not isinstance(club_str, str): return None
    cleaned_club = club_str.strip().title()
    return alias_map.get(cleaned_club, cleaned_club)

def standardize_athlete_name(name_str, remove_middle_initial=True):
    """
    Standardize athlete names for consistent matching across sources.
    Handles:
    - "Last, First" → "First Last"
    - "LAST FIRST" (all caps) → "First Last" (if we suspect order flip)
    - Extra whitespace cleanup
    - Title case normalization
    """
    if not isinstance(name_str, str) or not name_str.strip(): 
        return None
    
    # Filter out garbage values from uncollapsed headers or data issues
    garbage_values = {'#', 'name', 'athlete', 'competitor', 'gymnast', 'nan', ''}
    cleaned = name_str.strip().lower()
    if cleaned in garbage_values or len(cleaned) <= 1 or cleaned.isdigit():
        return None
    
    # 1. Clean up extra whitespace
    name_str = ' '.join(name_str.strip().split())
    
    # 2. Handle "Last, First" format
    if ',' in name_str:
        parts = [p.strip() for p in name_str.split(',', 1)]
        if len(parts) == 2:
            name_str = f"{parts[1]} {parts[0]}"
    
    # 3. Handle ALL CAPS (which often implies LAST FIRST in some systems)
    words = name_str.split()
    if name_str.isupper() and len(words) == 2:
        # We don't blindly flip, but we title-case it. 
        # The get_or_create_person will handle the flip check in the DB.
        name_str = ' '.join(word.capitalize() for word in words)
        words = name_str.split()
    else:
        # standard title case for each word
        words = [word.capitalize() if word.isupper() or word.islower() else word for word in words]
    
    # 4. Remove middle initial (optional)
    if remove_middle_initial and len(words) > 2:
        filtered_words = []
        for i, word in enumerate(words):
            if i == 0 or i == len(words) - 1:
                filtered_words.append(word)
            elif len(word.replace('.', '')) > 1:
                filtered_words.append(word)
        words = filtered_words
    
    return ' '.join(words)


def detect_discipline(df):
    column_names = set(df.columns)
    MAG_INDICATORS = {'Pommel_Horse', 'PommelHorse', 'Rings', 'Parallel_Bars', 'ParallelBars', 'High_Bar', 'HighBar'}
    WAG_INDICATORS = {'Uneven_Bars', 'UnevenBars', 'Beam'}
    for col in column_names:
        if any(indicator in col for indicator in MAG_INDICATORS): return 2, 'MAG', 'M'
        if any(indicator in col for indicator in WAG_INDICATORS): return 1, 'WAG', 'F'
    return 99, 'Other', 'Unknown'
    
def parse_rank(rank_str):
    """
    Extracts numeric rank from strings like '1', '7T', 'Gold'. 
    Returns None if no digit found.
    """
    if not rank_str or not isinstance(rank_str, str): return None
    clean = re.sub(r'\D', '', rank_str)
    return int(clean) if clean else None

# ==============================================================================
#  NEW DATABASE INTERACTION FUNCTIONS
# ==============================================================================

def get_or_create_person(conn, full_name, gender, cache):
    """
    Get or create a person record.
    Uses PersonAliases table to handle inconsistent name orders (e.g. First Last vs Last First),
    and a manual person_aliases.json for typo correction.
    """
    if full_name in cache: return cache[full_name]
    
    # 0. Manual Alias Check (Typo correction)
    if full_name in PERSON_ALIASES:
        full_name = PERSON_ALIASES[full_name]

    cursor = conn.cursor()
    
    # 1. Check Alias table first
    cursor.execute("SELECT canonical_person_id FROM PersonAliases WHERE alias_name = ?", (full_name,))
    result = cursor.fetchone()
    if result:
        person_id = result[0]
        cache[full_name] = person_id
        return person_id
        
    # 2. Check Persons table (direct match)
    cursor.execute("SELECT person_id FROM Persons WHERE full_name = ?", (full_name,))
    result = cursor.fetchone()
    if result:
        person_id = result[0]
        # Register this name as an alias too for faster lookup next time
        cursor.execute("INSERT OR IGNORE INTO PersonAliases (alias_name, canonical_person_id) VALUES (?, ?)", (full_name, person_id))
        cache[full_name] = person_id
        return person_id

    # 3. Create new person if no match found
    # (Automatic flipping removed to ensure manual verification via audit/apply scripts)
    cursor.execute("INSERT INTO Persons (full_name, gender) VALUES (?, ?)", (full_name, gender))
    person_id = cursor.lastrowid
    
    # Initialize the first alias as the canonical name
    cursor.execute("INSERT INTO PersonAliases (alias_name, canonical_person_id) VALUES (?, ?)", (full_name, person_id))
    
    cache[full_name] = person_id
    return person_id

def get_or_create_club(conn, club_name, cache):
    """
    Get or create a club record.
    Uses ClubAliases table and club_aliases.json for normalization.
    """
    if club_name is None: return None
    if club_name in cache: return cache[club_name]
    
    # 1. Manual Alias Check (JSON)
    normalized_name = club_name.strip().title()
    if normalized_name in CLUB_ALIASES:
        club_name = CLUB_ALIASES[normalized_name]

    cursor = conn.cursor()
    
    # 2. Check Database Alias Table
    cursor.execute("SELECT canonical_club_id FROM ClubAliases WHERE club_alias_name = ?", (club_name,))
    result = cursor.fetchone()
    if result:
        club_id = result[0]
        cache[club_name] = club_id
        return club_id

    # 3. Check Clubs Table Directly
    cursor.execute("SELECT club_id FROM Clubs WHERE name = ?", (club_name,))
    result = cursor.fetchone()
    if result:
        club_id = result[0]
    else:
        # 4. Create New Club
        cursor.execute("INSERT INTO Clubs (name) VALUES (?)", (club_name,))
        club_id = cursor.lastrowid
        # Also register this name as an alias for future consistency
        cursor.execute("INSERT OR IGNORE INTO ClubAliases (club_alias_name, canonical_club_id) VALUES (?, ?)", (club_name, club_id))
    
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
        # Use INSERT OR IGNORE for thread-safety and robustness
        cursor.execute("INSERT OR IGNORE INTO Athletes (person_id, club_id) VALUES (?, ?)", athlete_key)
        # If it was ignored, we need to fetch the existing ID
        cursor.execute("SELECT athlete_id FROM Athletes WHERE person_id = ? AND club_id IS " + ("NULL" if club_id is None else "?"), (person_id,) if club_id is None else athlete_key)
        athlete_id = cursor.fetchone()[0]
    cache[athlete_key] = athlete_id
    return athlete_id

def get_or_create_meet(conn, source, source_meet_id, meet_details, cache):
    meet_key = (source, source_meet_id)
    if meet_key in cache: return cache[meet_key]
    
    # Extract year: prefer explicit year, then parse from date string
    comp_year = meet_details.get('year') or meet_details.get('comp_year')
    if not comp_year and meet_details.get('start_date_iso'):
        date_str = str(meet_details.get('start_date_iso'))
        year_match = re.search(r'(20\d{2})', date_str)
        if year_match:
            comp_year = int(year_match.group(1))
    
    # Auto-detect country if not provided
    country = meet_details.get('country')
    if not country:
        country = detect_country(
            location=meet_details.get('location'),
            source=source,
            meet_name=meet_details.get('name')
        )
    
    cursor = conn.cursor()
    cursor.execute("SELECT meet_db_id FROM Meets WHERE source = ? AND source_meet_id = ?", meet_key)
    result = cursor.fetchone()
    if result:
        meet_db_id = result[0]
    else:
        cursor.execute("""INSERT INTO Meets 
            (source, source_meet_id, name, start_date_iso, comp_year, location, country, competition_type) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (source, source_meet_id, meet_details.get('name'), meet_details.get('start_date_iso'), 
             comp_year, meet_details.get('location'), country, meet_details.get('competition_type')))
        meet_db_id = cursor.lastrowid
        country_str = country or 'Unknown'
        print(f"  -> New meet added: '{meet_details.get('name')}' (ID: {meet_db_id}, Year: {comp_year}, Country: {country_str})")
    cache[meet_key] = meet_db_id
    return meet_db_id


# --- ERROR LOGGING ---
def log_scrape_error(conn, source, source_meet_id, error_message):
    """Log a scraping error to the ScrapeErrors table."""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO ScrapeErrors (source, source_meet_id, error_message) VALUES (?, ?, ?)",
        (source, source_meet_id, str(error_message)[:500])  # Truncate long messages
    )
    conn.commit()

# --- DUPLICATE DETECTION ---
def check_duplicate_result(conn, meet_db_id, athlete_id, apparatus_id):
    """Check if a result already exists. Returns existing result_id or None."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT result_id FROM Results WHERE meet_db_id = ? AND athlete_id = ? AND apparatus_id = ?",
        (meet_db_id, athlete_id, apparatus_id)
    )
    result = cursor.fetchone()
    return result[0] if result else None

# --- SCORE VALIDATION ---
def validate_score(score_final, score_d=None, apparatus_name=""):
    """
    Validate score values are within expected ranges.
    Returns (is_valid, warning_message or None)
    """
    warnings = []
    
    # AllAround is sum of all events, so skip high-score check
    is_all_around = 'all' in apparatus_name.lower() or apparatus_name.lower() == 'allaround'
    
    if score_final is not None:
        if score_final < 0:
            warnings.append(f"Negative score: {score_final}")
        elif score_final > 16.5 and not is_all_around:
            warnings.append(f"Unusually high score: {score_final}")
    
    if score_d is not None:
        if score_d < 0:
            warnings.append(f"Negative D-score: {score_d}")
        elif score_d > 7.5:
            warnings.append(f"Very high D-score (elite level?): {score_d}")
    
    return (len(warnings) == 0, "; ".join(warnings) if warnings else None)


# --- DNS/DNF/SCRATCH STANDARDIZATION ---
def standardize_score_status(score_text):
    """
    Standardize score status codes.
    Returns standardized code or original text.
    """
    if not score_text:
        return None
    
    text_upper = str(score_text).strip().upper()
    
    STATUS_MAP = {
        'DNS': 'DNS', 'DID NOT START': 'DNS', 'NO SHOW': 'DNS',
        'DNF': 'DNF', 'DID NOT FINISH': 'DNF', 'INCOMPLETE': 'DNF',
        'SCR': 'SCR', 'SCRATCH': 'SCR', 'SCRATCHED': 'SCR', 'WD': 'SCR', 'WITHDREW': 'SCR',
        'DQ': 'DQ', 'DISQUALIFIED': 'DQ',
        'EXH': 'EXH', 'EXHIBITION': 'EXH',
    }
    
    return STATUS_MAP.get(text_upper, score_text)
# ==============================================================================
#  FILE TRACKING HELPERS
# ==============================================================================

def calculate_file_hash(filepath):
    """Calculates MD5 hash of a file."""
    import hashlib
    if not os.path.exists(filepath): return None
    hasher = hashlib.md5()
    try:
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except: return None

def is_file_processed(conn, filepath, file_hash):
    """Checks if file hash already processed."""
    cursor = conn.cursor()
    cursor.execute("SELECT file_hash FROM ProcessedFiles WHERE file_path = ?", (filepath,))
    res = cursor.fetchone()
    return True if res and res[0] == file_hash else False

def mark_file_processed(conn, filepath, file_hash):
    """Updates processed state of a file."""
    cursor = conn.cursor()
    from datetime import datetime
    cursor.execute("INSERT OR REPLACE INTO ProcessedFiles (file_path, file_hash, last_processed) VALUES (?, ?, ?)",
                   (filepath, file_hash, datetime.now().isoformat()))
    conn.commit()
