#!/usr/bin/env python3
"""Re-ingest all Copeland 2022 MAG files for meet_db_id 118."""
import sqlite3, glob, json, logging
from load_orchestrator import reader_worker, write_to_db, flush_pending_inserts, load_manifest, DB_FILE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

conn = sqlite3.connect(DB_FILE, timeout=30)
cursor = conn.cursor()

# Load manifests for meet details
livemeet_manifest = load_manifest('livemeet', 'discovered_meet_ids_livemeet.csv')

# Build caches (same as orchestrator main())
from functools import lru_cache

person_cache = {}
cursor.execute("SELECT person_id, full_name FROM Persons")
for pid, fn in cursor.fetchall():
    person_cache[fn.strip().lower()] = pid

athlete_cache = {}
cursor.execute("SELECT athlete_id, person_id, club_id FROM Athletes")
for aid, pid, cid in cursor.fetchall():
    athlete_cache[(pid, cid)] = aid

club_cache = {}
cursor.execute("SELECT club_id, name FROM Clubs")
for cid, cn in cursor.fetchall():
    club_cache[cn.strip().lower()] = cid

meet_cache = {}
cursor.execute("SELECT meet_db_id, source, source_meet_id FROM Meets")
for mid, src, smid in cursor.fetchall():
    meet_cache[(src, str(smid))] = mid

apparatus_cache = {}
cursor.execute("SELECT apparatus_id, name FROM Apparatus")
for aid, an in cursor.fetchall():
    apparatus_cache[an] = aid

caches = {
    'person': person_cache,
    'athlete': athlete_cache,
    'club': club_cache,
    'meet': meet_cache,
    'apparatus': apparatus_cache,
}

# Load club aliases
club_alias_map = {}
try:
    with open('club_aliases.json', 'r') as f:
        club_alias_map = json.load(f)
except:
    pass

# Build existing results set for dedup
existing_results = set()
cursor.execute("SELECT meet_db_id, athlete_id, apparatus_id, session FROM Results")
for row in cursor.fetchall():
    existing_results.add(row)

# Find all MAG files for this meet
files = glob.glob('CSVs_Livemeet_final/6BB3817D94653EE6A7DB325B26F91996_FINAL_BYEVENT_*_MAG.csv')
logging.info(f"Found {len(files)} MAG files to re-ingest for Copeland 2022")

pending_inserts = []

for fpath in files:
    data = reader_worker('livemeet', fpath, livemeet_manifest)
    if data:
        write_to_db(conn, data, caches, club_alias_map, existing_results, pending_inserts)

if pending_inserts:
    flush_pending_inserts(cursor, pending_inserts)
    conn.commit()

# Verify
cursor.execute("SELECT COUNT(*) FROM Results WHERE meet_db_id = 118")
count = cursor.fetchone()[0]
logging.info(f"Meet 118 now has {count} results")

conn.close()
logging.info("Done.")
