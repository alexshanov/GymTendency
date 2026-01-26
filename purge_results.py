
import sqlite3

def purge_results():
    ids = [
        '64B251CDA4B732D2CA89B7E3EA6AE489', # Copeland 2020
        '6BE6424622F00D49D5E11C551B107CA4', # EC 2023
        '4649BF2ADA3B997EACE1F4457CAA6435', # Spruce Moose 2019
        '7D54F21B6C5736440F73A456B704E93F', # Summit 2019
        '70814920CC04E0097D7EE32FE0F90F19', # Turoff 2021
        '6BB3817D94653EE6A7DB325B26F91996'  # Copeland 2022
    ]
    
    # Use 30 second timeout for lock contention
    conn = sqlite3.connect('gym_data.db', timeout=30)
    cursor = conn.cursor()
    
    total_deleted = 0
    for mid in ids:
        cursor.execute("SELECT meet_db_id FROM Meets WHERE source_meet_id = ?", (mid,))
        res = cursor.fetchall()
        if res:
            m_ids = [r[0] for r in res]
            placeholders = ', '.join(['?'] * len(m_ids))
            cursor.execute(f"DELETE FROM Results WHERE meet_db_id IN ({placeholders})", m_ids)
            total_deleted += cursor.rowcount
            print(f"Deleted {cursor.rowcount} results for source meet ID: {mid}")
        
    conn.commit()
    conn.close()
    print(f"Purge complete. Total records deleted: {total_deleted}")

if __name__ == "__main__":
    purge_results()
