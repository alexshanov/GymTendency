import sqlite3
import json
import os
import glob

DB_FILE = "gym_data.db"
STATUS_FILE = "scraped_meets_status.json"

def find_gaps():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    gaps = []
    
    # 1. WAG Gaps
    cursor.execute("""
        SELECT source, meet_name, COUNT(*) as athletes,
            SUM(CASE WHEN vt_score IS NOT NULL AND vt_score != '' THEN 1 ELSE 0 END) as vt,
            SUM(CASE WHEN ub_score IS NOT NULL AND ub_score != '' THEN 1 ELSE 0 END) as ub,
            SUM(CASE WHEN bb_score IS NOT NULL AND bb_score != '' THEN 1 ELSE 0 END) as bb,
            SUM(CASE WHEN fx_score IS NOT NULL AND fx_score != '' THEN 1 ELSE 0 END) as fx
        FROM Gold_Results_WAG
        GROUP BY source, meet_name
        HAVING athletes > 5 AND (vt = 0 OR ub = 0 OR bb = 0 OR fx = 0)
    """)
    w_gaps = cursor.fetchall()
    for source, name, athletes, vt, ub, bb, fx in w_gaps:
        gaps.append({'source': source, 'name': name, 'gender': 'F', 'athletes': athletes})
        print(f"Gap found (WAG): {name} ({source}) - {athletes} athletes")

    # 2. MAG Gaps
    cursor.execute("""
        SELECT source, meet_name, COUNT(*) as athletes,
            SUM(CASE WHEN fx_score IS NOT NULL AND fx_score != '' THEN 1 ELSE 0 END) as fx,
            SUM(CASE WHEN ph_score IS NOT NULL AND ph_score != '' THEN 1 ELSE 0 END) as ph,
            SUM(CASE WHEN sr_score IS NOT NULL AND sr_score != '' THEN 1 ELSE 0 END) as sr,
            SUM(CASE WHEN vt_score IS NOT NULL AND vt_score != '' THEN 1 ELSE 0 END) as vt,
            SUM(CASE WHEN pb_score IS NOT NULL AND pb_score != '' THEN 1 ELSE 0 END) as pb,
            SUM(CASE WHEN hb_score IS NOT NULL AND hb_score != '' THEN 1 ELSE 0 END) as hb
        FROM Gold_Results_MAG
        GROUP BY source, meet_name
        HAVING athletes > 5 AND (fx = 0 OR ph = 0 OR sr = 0 OR vt = 0 OR pb = 0 OR hb = 0)
    """)
    m_gaps = cursor.fetchall()
    for source, name, athletes, fx, ph, sr, vt, pb, hb in m_gaps:
        gaps.append({'source': source, 'name': name, 'gender': 'M', 'athletes': athletes})
        print(f"Gap found (MAG): {name} ({source}) - {athletes} athletes")
        
    conn.close()
    return gaps

def reset_meets(gaps):
    if not gaps:
        print("No gaps to reset.")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    with open(STATUS_FILE, 'r') as f:
        status_manifest = json.load(f)
    
    for gap in gaps:
        print(f"Resetting: {gap['name']} ({gap['source']})")
        
        # Find Meet IDs in DB
        cursor.execute("SELECT meet_db_id, source_meet_id FROM Meets WHERE name LIKE ? AND source = ?", (f"{gap['name']}%", gap['source']))
        meet_info = cursor.fetchall()
        
        for m_db_id, m_sid in meet_info:
            print(f"  -> DB ID: {m_db_id}, SID: {m_sid}")
            
            # 1. Delete Results
            cursor.execute("DELETE FROM Results WHERE meet_db_id = ?", (m_db_id,))
            
            # 2. Delete ProcessedFiles hashes
            # We look for files starting with the source_meet_id
            cursor.execute("DELETE FROM ProcessedFiles WHERE file_path LIKE ?", (f"%{m_sid}%",))
            
            # 3. Optional: Reset in status manifest if we want to RE-SCRAPE
            # For now, let's only reset if results were 0 or suspicious
            # Actually, most of these are loading issues. 
            # If we want to re-scrape, uncomment:
            # key = f"{gap['source']}_{m_sid}"
            # if key in status_manifest: del status_manifest[key]

    conn.commit()
    conn.close()
    
    # with open(STATUS_FILE, 'w') as f:
    #     json.dump(status_manifest, f, indent=2)

if __name__ == "__main__":
    gaps = find_gaps()
    reset_meets(gaps)
