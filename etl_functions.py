# etl_functions.py

import sqlite3
import pandas as pd
import os
import json
import traceback
import re

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



def setup_database(db_file):
    """
    Creates the new, professional database schema with Persons, Clubs,
    and a linking Athletes table.
    """
    print("--- Setting up new professional database schema ---")
    
    # Drop tables in reverse order of dependency to avoid errors
    drop_queries = [
        "DROP TABLE IF EXISTS ScoringStandards;",
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
            FOREIGN KEY (meet_db_id) REFERENCES Meets (meet_db_id),
            FOREIGN KEY (athlete_id) REFERENCES Athletes (athlete_id),
            FOREIGN KEY (apparatus_id) REFERENCES Apparatus (apparatus_id)
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
            print("Dropping old tables to ensure a clean slate...")
            for query in drop_queries: cursor.execute(query)
            
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

def standardize_athlete_name(name_str, remove_middle_initial=True):
    """
    Standardize athlete names for consistent matching across sources.
    
    Handles:
    - "Last, First" → "First Last"
    - "LAST FIRST" (all caps) → "First Last"  
    - Extra whitespace cleanup
    - Middle initial removal (optional, for deduplication)
    - Title case normalization
    """
    if not isinstance(name_str, str) or not name_str.strip(): 
        return None
    
    # 1. Clean up extra whitespace
    name_str = ' '.join(name_str.strip().split())
    
    # 2. Handle "Last, First" format
    if ',' in name_str:
        parts = [p.strip() for p in name_str.split(',', 1)]
        if len(parts) == 2:
            name_str = f"{parts[1]} {parts[0]}"
    
    # 3. Handle ALL CAPS (assume "LAST FIRST" or "FIRST LAST")
    words = name_str.split()
    if len(words) >= 2 and name_str.isupper():
        # All caps - check if first word looks like last name (common pattern: SMITH JOHN)
        # Heuristic: if second word is shorter, it might be first name abbreviated
        # Default: assume "FIRST LAST" order for all caps
        name_str = ' '.join(word.title() for word in words)
        words = name_str.split()
    
    # 4. Apply title case to each word
    words = [word.title() if word.isupper() or word.islower() else word for word in words]
    
    # 5. Remove middle initial (single letter or letter with period)
    if remove_middle_initial and len(words) > 2:
        filtered_words = []
        for i, word in enumerate(words):
            # Keep first and last word, filter middle initials
            if i == 0 or i == len(words) - 1:
                filtered_words.append(word)
            elif len(word.replace('.', '')) > 1:  # Not a single initial
                filtered_words.append(word)
            # else: skip middle initial
        words = filtered_words
    
    # 6. Final cleanup - remove any remaining periods from initials kept
    result = ' '.join(words)
    return result


def detect_discipline(df):
    column_names = set(df.columns)
    MAG_INDICATORS = {'Pommel_Horse', 'PommelHorse', 'Rings', 'Parallel_Bars', 'ParallelBars', 'High_Bar', 'HighBar'}
    WAG_INDICATORS = {'Uneven_Bars', 'UnevenBars', 'Beam'}
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