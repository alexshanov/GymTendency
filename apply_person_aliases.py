
import sqlite3
import json
import os

def apply_person_aliases(db_file="gym_data.db", confirmed_json="person_aliases.json", potential_json="potential_person_aliases.json"):
    """
    Reads the GROUPED potential aliases JSON.
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
        # Find canonical person_id
        cursor.execute("SELECT person_id FROM Persons WHERE full_name = ?", (canonical,))
        canon_row = cursor.fetchone()
        
        if not canon_row:
            print(f"  [Skip] Canonical name '{canonical}' not found in Persons table.")
            continue
            
        canon_id = canon_row[0]

        for alias in aliases:
            # Find alias person_id
            cursor.execute("SELECT person_id FROM Persons WHERE full_name = ?", (alias,))
            alias_row = cursor.fetchone()
            
            if alias_row:
                alias_id = alias_row[0]
                
                # a. Add to PersonAliases table
                cursor.execute("INSERT OR IGNORE INTO PersonAliases (alias_name, canonical_person_id) VALUES (?, ?)", (alias, canon_id))
                aliases_applied += 1
                
                # b. Merge Athletes: Point all athletes associated with alias_id to canon_id
                cursor.execute("SELECT athlete_id, club_id FROM Athletes WHERE person_id = ?", (alias_id,))
                alias_athletes = cursor.fetchall()
                
                for ath_id, club_id in alias_athletes:
                    # Check if an athlete record already exists for the canonical person at this club
                    if club_id is None:
                        cursor.execute("SELECT athlete_id FROM Athletes WHERE person_id = ? AND club_id IS NULL", (canon_id,))
                    else:
                        cursor.execute("SELECT athlete_id FROM Athletes WHERE person_id = ? AND club_id = ?", (canon_id, club_id))
                    
                    target_ath_row = cursor.fetchone()
                    
                    if target_ath_row:
                        target_ath_id = target_ath_row[0]
                        cursor.execute("UPDATE Results SET athlete_id = ? WHERE athlete_id = ?", (target_ath_id, ath_id))
                        cursor.execute("DELETE FROM Athletes WHERE athlete_id = ?", (ath_id,))
                    else:
                        cursor.execute("UPDATE Athletes SET person_id = ? WHERE athlete_id = ?", (canon_id, ath_id))
                
                # c. Delete the alias person from Persons table
                cursor.execute("DELETE FROM Persons WHERE person_id = ?", (alias_id,))
                records_merged += 1
            else:
                # Even if the person isn't in the DB yet, we record the alias for future ETL runs
                cursor.execute("INSERT OR IGNORE INTO PersonAliases (alias_name, canonical_person_id) VALUES (?, ?)", (alias, canon_id))
                aliases_applied += 1

    conn.commit()
    conn.close()
    print(f"Applied {aliases_applied} aliases to DB and merged {records_merged} person records.")

if __name__ == "__main__":
    apply_person_aliases()
