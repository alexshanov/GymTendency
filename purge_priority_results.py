import sqlite3
import json
import os

DB_FILE = "gym_data.db"
PRIORITY_FILE = "priority_meets.json"

def main():
    if not os.path.exists(PRIORITY_FILE):
        print(f"Error: {PRIORITY_FILE} not found.")
        return

    with open(PRIORITY_FILE, 'r') as f:
        priority_meets = json.load(f)

    if not os.path.exists(DB_FILE):
        print(f"Error: {DB_FILE} not found.")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    total_purged = 0
    total_files_purged = 0

    print(f"Purging results for {len(priority_meets)} priority meets...")

    for source, m_sid in priority_meets:
        # 1. Find meet_db_id
        cursor.execute("SELECT meet_db_id, name FROM Meets WHERE source = ? AND source_meet_id = ?", (source, str(m_sid)))
        res = cursor.fetchone()
        if res:
            m_db_id, name = res
            print(f"  -> Purging: {name} ({source} {m_sid})")
            
            # 2. Delete Results
            cursor.execute("DELETE FROM Results WHERE meet_db_id = ?", (m_db_id,))
            rows_deleted = cursor.rowcount
            total_purged += rows_deleted
            
            # 3. Delete ProcessedFiles
            # Pattern search in path to catch all CSVs for this meet
            cursor.execute("DELETE FROM ProcessedFiles WHERE file_path LIKE ?", (f"%{m_sid}%",))
            files_deleted = cursor.rowcount
            total_files_purged += files_deleted
            
            print(f"     Deleted {rows_deleted} rows and {files_deleted} file signatures.")
        else:
            print(f"  -> Skip: No DB record for {source} {m_sid}")

    conn.commit()
    conn.close()
    print(f"\nSUCCESS: Purged {total_purged} total result rows and {total_files_purged} file signatures.")

if __name__ == "__main__":
    main()
