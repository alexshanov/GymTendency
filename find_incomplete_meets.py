import sqlite3
import json

def find_incomplete_meets():
    with open('scraped_meets_status.json', 'r') as f:
        status_manifest = json.load(f)
    
    conn = sqlite3.connect('gym_data.db')
    cursor = conn.cursor()
    
    incomplete = []
    
    for key, details in status_manifest.items():
        if isinstance(details, dict) and details.get('status') == 'DONE':
            source, sid = key.split('_', 1)
            
            # Check Results count for this meet
            cursor.execute("""
                SELECT COUNT(*) FROM Results r
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                WHERE m.source = ? AND m.source_meet_id = ?
            """, (source, sid))
            count = cursor.fetchone()[0]
            
            # Threshold: < 5 records might be incomplete for these platforms
            if count < 5:
                incomplete.append({
                    'key': key,
                    'name': details.get('name'),
                    'count': count
                })
                
    conn.close()
    return incomplete

if __name__ == "__main__":
    results = find_incomplete_meets()
    print(f"Found {len(results)} potentially incomplete meets (DONE but < 5 results):")
    for r in results:
        print(f" - {r['key']}: {r['count']} results - {r['name'][:50]}")
