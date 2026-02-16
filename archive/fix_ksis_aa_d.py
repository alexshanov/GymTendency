
import sqlite3

def fix_ksis_aa_d():
    """
    Populates missing aa_d in Gold_Results_MAG by summing event Ds for KSIS rows.
    """
    conn = sqlite3.connect('gym_data.db')
    cursor = conn.cursor()
    
    # Select KSIS rows where aa_d is missing but event Ds are present
    cursor.execute("""
        SELECT athlete_name, meet_name, year,
               fx_d, ph_d, sr_d, vt_d, pb_d, hb_d
        FROM Gold_Results_MAG
        WHERE source = 'ksis' AND aa_d IS NULL
    """)
    rows = cursor.fetchall()
    
    updated_count = 0
    for row in rows:
        name, meet, year, fx, ph, sr, vt, pb, hb = row
        
        # Convert to float and sum
        ds = [fx, ph, sr, vt, pb, hb]
        numeric_ds = []
        for d in ds:
            try:
                if d and str(d).strip() != '':
                    numeric_ds.append(float(str(d).replace(',', '')))
            except ValueError:
                pass
                
        if numeric_ds:
            calculated_aa_d = round(sum(numeric_ds), 3)
            # Update the GOLD table directly
            cursor.execute("""
                UPDATE Gold_Results_MAG 
                SET aa_d = ? 
                WHERE athlete_name = ? AND meet_name = ? AND year = ?
            """, (str(calculated_aa_d), name, meet, year))
            updated_count += 1
            
    conn.commit()
    conn.close()
    print(f"KSIS AA_D remediation complete. Updated {updated_count} records.")

if __name__ == "__main__":
    fix_ksis_aa_d()
