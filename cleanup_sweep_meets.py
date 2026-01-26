
import sqlite3

def cleanup_db():
    patterns = [
        'Copeland Classic MAG 2020%',
        '2023 MAG Elite Canada%',
        'Spruce Moose Invitational MAG 2019%',
        '2019 Summit Invitational MAG%',
        'Fred Turoff Invitational%'
    ]
    
    conn = sqlite3.connect('gym_data.db')
    cursor = conn.cursor()
    
    total_deleted = 0
    for pattern in patterns:
        cursor.execute("DELETE FROM Gold_Results_MAG WHERE meet_name LIKE ?", (pattern,))
        deleted = cursor.rowcount
        total_deleted += deleted
        print(f"Deleted {deleted} rows for pattern: {pattern}")
        
    conn.commit()
    conn.close()
    print(f"Cleanup complete. Total rows deleted: {total_deleted}")

if __name__ == "__main__":
    cleanup_db()
