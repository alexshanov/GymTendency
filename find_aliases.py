
import sqlite3
import json
import os
import argparse

def find_athlete_aliases(targets_file="alias_research_targets.txt", db_file="gym_data.db"):
    if not os.path.exists(db_file):
        print(f"Error: Database {db_file} not found.")
        return
    
    if not os.path.exists(targets_file):
        print(f"Error: Targets file {targets_file} not found.")
        return

    with open(targets_file, "r") as f:
        targets = [line.strip() for line in f if line.strip()]

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    proposals = {}
    md_report = ["# Targeted Alias Proposals\n"]

    for target in targets:
        print(f"Researching aliases for: {target}")
        words = target.split()
        if not words:
            continue
        
        last_name = words[-1]
        first_name = words[0]
        
        # Query for potential matches
        # Search by last name (case-insensitive)
        cursor.execute("""
            SELECT person_id, full_name FROM Persons 
            WHERE LOWER(full_name) LIKE ?
        """, (f"%{last_name.lower()}%",))
        matches = cursor.fetchall()

        matched_aliases = []
        
        for mid, mname in matches:
            if mname.lower() == target.lower():
                continue
            
            # Context fetch
            cursor.execute("""
                SELECT DISTINCT m.name, c.name, m.comp_year
                FROM Results r
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                JOIN Athletes a ON r.athlete_id = a.athlete_id
                JOIN Clubs c ON a.club_id = c.club_id
                WHERE a.person_id = ?
                LIMIT 5
            """, (mid,))
            context = cursor.fetchall()
            
            context_str = ", ".join([f"{c[0]} ({c[2]}) - {c[1]}" for c in context])
            matched_aliases.append({
                "id": mid,
                "name": mname,
                "context": context_str
            })

        if matched_aliases:
            proposals[target] = [m["name"] for m in matched_aliases]
            md_report.append(f"## Target: {target}")
            md_report.append("| Potential Alias | Context (Meet/Year/Club) |")
            md_report.append("| :--- | :--- |")
            for m in matched_aliases:
                md_report.append(f"| {m['name']} | {m['context']} |")
            md_report.append("\n")

    conn.close()

    # Save outputs
    with open("potential_targeted_aliases.json", "w") as f:
        json.dump(proposals, f, indent=2)
    
    with open("alias_proposals_report.md", "w") as f:
        f.write("\n".join(md_report))

    print(f"\nDone. Saved {len(proposals)} proposals to 'potential_targeted_aliases.json' and 'alias_proposals_report.md'.")

if __name__ == "__main__":
    find_athlete_aliases()
