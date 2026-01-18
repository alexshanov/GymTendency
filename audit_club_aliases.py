
import sqlite3
import json
import os
import re

def find_potential_club_aliases(db_file="gym_data.db"):
    if not os.path.exists(db_file):
        print(f"Error: Database {db_file} not found.")
        return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Get all clubs
    cursor.execute("SELECT club_id, name FROM Clubs")
    clubs = cursor.fetchall()

    club_to_id = {name: cid for cid, name in clubs}
    aliases_found = {}

    print(f"Scanning {len(clubs)} clubs for potential aliases...")

    # Sort by length to compare shorter names against potential long names
    sorted_clubs = sorted([c[1] for c in clubs], key=len)

    for i, name in enumerate(sorted_clubs):
        name_lower = name.lower()
        
        # 1. Check for common suffixes/prefixes (Association, Society, Gymnastics Club, etc.)
        # Remove common words to find core name
        core_name = re.sub(r'\b(Gymnastics|Club|Association|Society|Secondary|School|Elem|High|District|Mag|Wag|Competitive|Team|Association|Secondary School)\b', '', name, flags=re.IGNORECASE).strip()
        core_name = re.sub(r'\s+', ' ', core_name)
        
        if len(core_name) < 3: continue

        for other_name in sorted_clubs[i+1:]:
            other_lower = other_name.lower()
            
            # Simple substring match or core name match
            if core_name.lower() in other_lower and name_lower != other_lower:
                # Potential match
                canonical = min(name, other_name, key=len) # Suggest shorter one as canonical usually
                alias = max(name, other_name, key=len)
                
                # Heuristic: If one contains "Secondary" and other doesn't, they are likely the same school club
                if alias not in aliases_found:
                    aliases_found[alias] = canonical

    conn.close()

    if aliases_found:
        print(f"\nFound {len(aliases_found)} potential club alias pairs:")
        for i, (alias, canon) in enumerate(list(aliases_found.items())[:20]):
            print(f"  - {alias} -> {canon}")
        
        with open("potential_club_aliases.json", "w") as f:
            json.dump(aliases_found, f, indent=2)
        print(f"\nSaved all results to 'potential_club_aliases.json'.")
    else:
        print("No obvious club aliases found.")

if __name__ == "__main__":
    find_potential_club_aliases()
