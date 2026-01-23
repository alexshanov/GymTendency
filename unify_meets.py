
# unify_meets.py
import sqlite3
import logging

def unify_meets(db_file="gym_data.db"):
    logging.info("Starting meet unification process...")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # 1. Identify logical meets (Name + Year) that have multiple IDs
    cursor.execute("""
        SELECT name, comp_year, MIN(meet_db_id) as canonical_id, GROUP_CONCAT(meet_db_id) as all_ids
        FROM Meets
        WHERE name IS NOT NULL AND name != ''
        GROUP BY LOWER(TRIM(name)), comp_year
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    
    # Extra pass for meets with missing years: see if we can parse the year from the name
    cursor.execute("SELECT meet_db_id, name, comp_year FROM Meets WHERE comp_year IS NULL OR comp_year = ''")
    missing_years = cursor.fetchall()
    for m_id, name, _ in missing_years:
        import re
        match = re.search(r'(20\d{2})', name)
        if match:
            year = int(match.group(1))
            cursor.execute("UPDATE Meets SET comp_year = ? WHERE meet_db_id = ?", (year, m_id))
    
    conn.commit()
    
    # Rerun canonical search after year backfills
    cursor.execute("""
        SELECT name, comp_year, MIN(meet_db_id) as canonical_id, GROUP_CONCAT(meet_db_id) as all_ids
        FROM Meets
        WHERE name IS NOT NULL AND name != ''
        GROUP BY LOWER(TRIM(name)), comp_year
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    
    total_unified = 0
    for name, year, canonical_id, all_ids_str in duplicates:
        all_ids = [int(i) for i in all_ids_str.split(',')]
        others = [i for i in all_ids if i != canonical_id]
        
        if not others: continue
        
        logging.info(f"Unifying meet: '{name}' ({year}) -> Canonical ID: {canonical_id} (Merging IDs: {others})")
        
        # 2. Update Results table to point to the canonical ID
        for other_id in others:
            cursor.execute("UPDATE Results SET meet_db_id = ? WHERE meet_db_id = ?", (canonical_id, other_id))
            
        # 3. Delete the duplicate meet records
        placeholders = ', '.join(['?'] * len(others))
        cursor.execute(f"DELETE FROM Meets WHERE meet_db_id IN ({placeholders})", others)
        
        total_unified += len(others)
        
    conn.commit()
    conn.close()
    logging.info(f"Meet unification complete. Removed {total_unified} duplicate meet records.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unify_meets()
