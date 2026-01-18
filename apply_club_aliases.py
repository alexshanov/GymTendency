
import sqlite3
import json
import os

def apply_club_aliases(db_file="gym_data.db", confirmed_json="club_aliases.json", potential_json="potential_club_aliases.json"):
    """
    Reads the GROUPED potential club aliases JSON.
    Format: {"Canonical Name": ["Alias 1", "Alias 2"]}
    Checks if they are already in confirmed_json. If not, merges them in the DB.
    """
    if not os.path.exists(potential_json):
        print(f"Error: {potential_json} not found.")
        return

    with open(potential_json, 'r') as f:
        potential_grouped = json.load(f)

    if not os.path.exists(db_file):
        print(f"Error: {db_file} not found.")
        return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    aliases_applied = 0
    records_merged = 0

    for canonical, aliases in potential_grouped.items():
        # Find canonical club_id
        cursor.execute("SELECT club_id FROM Clubs WHERE name = ?", (canonical,))
        canon_row = cursor.fetchone()
        
        if not canon_row:
            print(f"  [Skip] Canonical club '{canonical}' not found in Clubs table.")
            continue
            
        canon_id = canon_row[0]

        for alias in aliases:
            # Find alias club_id
            cursor.execute("SELECT club_id FROM Clubs WHERE name = ?", (alias,))
            alias_row = cursor.fetchone()
            
            if alias_row:
                alias_id = alias_row[0]
                
                # a. Add to ClubAliases table
                cursor.execute("INSERT OR IGNORE INTO ClubAliases (club_alias_name, canonical_club_id) VALUES (?, ?)", (alias, canon_id))
                aliases_applied += 1
                
                # b. Merge Athletes: Point all athletes associated with alias_id to canon_id
                # (Same logic as person merge, but for club_id)
                # First, find athletes in the alias club
                cursor.execute("SELECT athlete_id, person_id FROM Athletes WHERE club_id = ?", (alias_id,))
                alias_athletes = cursor.fetchall()
                
                for ath_id, person_id in alias_athletes:
                    # Check if an athlete record already exists for this person at the canonical club
                    cursor.execute("SELECT athlete_id FROM Athletes WHERE person_id = ? AND club_id = ?", (person_id, canon_id))
                    target_ath_row = cursor.fetchone()
                    
                    if target_ath_row:
                        target_ath_id = target_ath_row[0]
                        # Move results
                        cursor.execute("UPDATE Results SET athlete_id = ? WHERE athlete_id = ?", (target_ath_id, ath_id))
                        # Delete the redundant athlete link
                        cursor.execute("DELETE FROM Athletes WHERE athlete_id = ?", (ath_id,))
                    else:
                        # Reassign the club_id for this athlete record
                        cursor.execute("UPDATE Athletes SET club_id = ? WHERE athlete_id = ?", (canon_id, ath_id))
                
                # c. Delete the alias club record
                cursor.execute("DELETE FROM Clubs WHERE club_id = ?", (alias_id,))
                records_merged += 1
            else:
                # Still register the alias for future ETL runs
                cursor.execute("INSERT OR IGNORE INTO ClubAliases (club_alias_name, canonical_club_id) VALUES (?, ?)", (alias, canon_id))
                aliases_applied += 1

    conn.commit()
    conn.close()
    print(f"Applied {aliases_applied} aliases to DB and merged {records_merged} club records.")

if __name__ == "__main__":
    apply_club_aliases()
