
import sqlite3
import json
import os

def find_potential_aliases(db_file="gym_data.db"):
    if not os.path.exists(db_file):
        print(f"Error: Database {db_file} not found.")
        return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Get all names
    cursor.execute("SELECT person_id, full_name FROM Persons")
    persons = cursor.fetchall()

    name_to_id = {name: pid for pid, name in persons}
    aliases_found = {}

    print(f"Scanning {len(persons)} persons for potential aliases...")

    for pid, name in persons:
        name_lower = name.lower()
        words = name_lower.split()
        
        # 1. Check for flipped names (First Last vs Last First) - Case Insensitive
        if len(words) == 2:
            flipped = f"{words[1]} {words[0]}"
            # Search for the flipped version
            matches = [n for n in name_to_id.keys() if n.lower() == flipped]
            if matches:
                orig_name = name
                orig_flipped = matches[0]
                if orig_name.lower() != orig_flipped.lower() or orig_name != orig_flipped:
                    canonical = min(orig_name, orig_flipped)
                    alias = max(orig_name, orig_flipped)
                    if alias != canonical:
                        aliases_found[alias] = canonical

        # 2. Check for Middle Initial variations (e.g. "John A Smith" vs "John Smith")
        if len(words) == 3 and len(words[1].replace('.', '')) == 1:
            no_initial = f"{words[0]} {words[2]}"
            matches = [n for n in name_to_id.keys() if n.lower() == no_initial]
            if matches:
                aliases_found[name] = matches[0]

    conn.close()

    if aliases_found:
        print(f"\nFound {len(aliases_found)} potential person alias pairs:")
        
        # Group by canonical name for a "nicer" JSON
        grouped_aliases = {}
        for alias, canon in aliases_found.items():
            if canon not in grouped_aliases:
                grouped_aliases[canon] = []
            grouped_aliases[canon].append(alias)
        
        # Sort canonical names and their alias lists
        sorted_grouped = {c: sorted(grouped_aliases[c]) for c in sorted(grouped_aliases.keys())}
        
        with open("potential_person_aliases.json", "w") as f:
            json.dump(sorted_grouped, f, indent=2)
        print(f"\nSaved grouped results to 'potential_person_aliases.json'.")
    else:
        print("No obvious aliases found.")

if __name__ == "__main__":
    find_potential_aliases()
