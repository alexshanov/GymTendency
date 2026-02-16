
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
    aliases_found = {}

    print(f"Scanning {len(clubs)} clubs for potential aliases...")

    # 1. Core Name Matching
    sorted_club_names = sorted([c[1] for c in clubs], key=len)
    for i, name in enumerate(sorted_club_names):
        name_lower = name.lower()
        core_name = re.sub(r'\b(Gymnastics|Club|Association|Society|Secondary|School|Elem|High|District|Mag|Wag|Competitive|Team|Association|Secondary School)\b', '', name, flags=re.IGNORECASE).strip()
        core_name = re.sub(r'\s+', ' ', core_name)
        
        if len(core_name) < 3: continue

        for other_name in sorted_club_names[i+1:]:
            other_lower = other_name.lower()
            if core_name.lower() in other_lower and name_lower != other_lower:
                canonical = min(name, other_name, key=len)
                alias = max(name, other_name, key=len)
                if alias not in aliases_found:
                    aliases_found[alias] = canonical

    # 2. Analyze athlete history (Person-Club associations)
    print("Analyzing athlete history for person-club associations...")
    cursor.execute("""
        SELECT p.person_id, p.full_name, c.club_id, c.name
        FROM Athletes a
        JOIN Persons p ON a.person_id = p.person_id
        JOIN Clubs c ON a.club_id = c.club_id
    """)
    associations = cursor.fetchall()
    
    person_to_clubs = {}
    for pid, pname, cid, cname in associations:
        if pid not in person_to_clubs:
            person_to_clubs[pid] = {'name': pname, 'clubs': set()}
        person_to_clubs[pid]['clubs'].add((cid, cname))
        
    history_aliases_count = 0
    for pid, data in person_to_clubs.items():
        if len(data['clubs']) > 1:
            club_list = sorted(list(data['clubs']), key=lambda x: len(x[1]))
            for i in range(len(club_list)):
                for j in range(i + 1, len(club_list)):
                    cn1 = club_list[i][1]
                    cn2 = club_list[j][1]
                    canon = cn1
                    alias = cn2
                    if alias not in aliases_found:
                        aliases_found[alias] = canon
                        history_aliases_count += 1

    conn.close()

    if aliases_found:
        print(f"\nFound {len(aliases_found)} potential club alias pairs ({history_aliases_count} from history):")
        
        # Group by canonical name for a "nicer" JSON
        grouped_aliases = {}
        for alias, canon in aliases_found.items():
            if canon not in grouped_aliases:
                grouped_aliases[canon] = []
            grouped_aliases[canon].append(alias)
        
        # Sort canonical names and their alias lists
        sorted_grouped = {c: sorted(grouped_aliases[c]) for c in sorted(grouped_aliases.keys())}
        
        with open("potential_club_aliases.json", "w") as f:
            json.dump(sorted_grouped, f, indent=2)
        print(f"\nSaved grouped results to 'potential_club_aliases.json'.")
    else:
        print("No obvious club aliases found.")

if __name__ == "__main__":
    find_potential_club_aliases()
