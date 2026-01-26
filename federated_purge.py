
import sqlite3

def federated_purge():
    # List of (source_meet_id, meet_name, year) to be absolutely sure
    targets = [
        ('64B251CDA4B732D2CA89B7E3EA6AE489', 'Copeland Classic MAG 2020%', 2020),
        ('6BE6424622F00D49D5E11C551B107CA4', '2023 MAG Elite Canada%', 2023),
        ('4649BF2ADA3B997EACE1F4457CAA6435', 'Spruce Moose Invitational MAG 2019%', 2019),
        ('7D54F21B6C5736440F73A456B704E93F', '2019 Summit Invitational MAG%', 2019),
        ('70814920CC04E0097D7EE32FE0F90F19', 'Fred Turoff Invitational%', 2021),
        ('6BB3817D94653EE6A7DB325B26F91996', 'Copeland Classic MAG 2022%', 2022)
    ]
    
    conn = sqlite3.connect('gym_data.db', timeout=30)
    cursor = conn.cursor()
    
    total_deleted = 0
    for sid, name_pattern, year in targets:
        # First resolve by source ID
        cursor.execute("SELECT meet_db_id FROM Meets WHERE source_meet_id = ?", (sid,))
        res = cursor.fetchall()
        
        # Also resolve by Name + Year logic (Federated logic)
        cursor.execute("SELECT meet_db_id FROM Meets WHERE name LIKE ? AND comp_year = ?", (name_pattern, year))
        res += cursor.fetchall()
        
        m_ids = list(set([r[0] for r in res]))
        if m_ids:
            placeholders = ', '.join(['?'] * len(m_ids))
            cursor.execute(f"DELETE FROM Results WHERE meet_db_id IN ({placeholders})", m_ids)
            total_deleted += cursor.rowcount
            print(f"Deleted {cursor.rowcount} results for federated meet IDs: {m_ids} (Source: {sid})")
        
    conn.commit()
    conn.close()
    print(f"Federated Purge complete. Total records deleted: {total_deleted}")

if __name__ == "__main__":
    federated_purge()
