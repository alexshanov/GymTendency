
import json
import os

CANDIDATES_FILE = "/home/alex-shanov/OneDrive/AnalyticsProjects/GymTendency/gold_alias_candidates.json"
ALIASES_FILE = "/home/alex-shanov/OneDrive/AnalyticsProjects/GymTendency/person_aliases.json"

def process_choices():
    if not os.path.exists(CANDIDATES_FILE):
        print(f"Error: {CANDIDATES_FILE} not found.")
        return

    with open(CANDIDATES_FILE, "r") as f:
        candidates = json.load(f)

    if not os.path.exists(ALIASES_FILE):
        aliases = {}
    else:
        with open(ALIASES_FILE, "r") as f:
            aliases = json.load(f)

    updated_count = 0
    ignored_count = 0
    
    # We will remove choices that are processed
    remaining_candidates = []

    for item in candidates:
        choice = item.get("choice")
        if choice == 1:
            # name_1 is canon, name_2 is alias
            aliases[item["name_2"]] = item["name_1"]
            updated_count += 1
        elif choice == 2:
            # name_2 is canon, name_1 is alias
            aliases[item["name_1"]] = item["name_2"]
            updated_count += 1
        elif choice == 0:
            # Erroneous match, discard from candidates list
            ignored_count += 1
        else:
            remaining_candidates.append(item)

    if updated_count > 0:
        with open(ALIASES_FILE, "w") as f:
            json.dump(aliases, f, indent=4)
        print(f"Updated {ALIASES_FILE} with {updated_count} new aliases.")
    else:
        print("No new aliases selected.")

    if ignored_count > 0:
        print(f"Ignored {ignored_count} erroneous matches.")

    # Save the remaining candidates back to the file
    with open(CANDIDATES_FILE, "w") as f:
        json.dump(remaining_candidates, f, indent=4)
    print(f"Saved remaining {len(remaining_candidates)} candidates to {CANDIDATES_FILE}.")

if __name__ == "__main__":
    process_choices()
