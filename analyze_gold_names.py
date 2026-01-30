
import sqlite3
import json
import itertools
from difflib import SequenceMatcher

DB_FILE = "/home/alex-shanov/OneDrive/AnalyticsProjects/GymTendency/gym_data.db"
OUTPUT_FILE = "/home/alex-shanov/OneDrive/AnalyticsProjects/GymTendency/gold_alias_candidates.json"

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

def analyze_names():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print("Fetching athlete names from Gold_Results_MAG...")
    cursor.execute("SELECT DISTINCT athlete_name FROM Gold_Results_MAG WHERE athlete_name IS NOT NULL AND athlete_name != ''")
    names = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    print(f"Found {len(names)} unique names.")
    
    alias_map = {}
    processed = set()
    
    # 1. Check for Reversals
    print("Checking for reversals...")
    name_set = set(names)
    
    for name in names:
        parts = name.split()
        if len(parts) >= 2:
            reversed_name = " ".join(parts[::-1])
            if reversed_name in name_set and reversed_name != name:
                key = tuple(sorted([name, reversed_name]))
                if key not in alias_map:
                    alias_map[key] = "Reversal"

    # 2. Check for Similarity
    print("Checking for fuzzy matches (this might take a moment)...")
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            n1 = names[i]
            n2 = names[j]
            
            key = tuple(sorted([n1, n2]))
            if key in alias_map:
                continue
                
            sim = similarity(n1.lower(), n2.lower())
            
            # Catch "Last First" vs "First Last" that handle "De Gannes" correctly
            # Actually Reversal covers exact swaps.
            # But "Anthony De Gannes" vs "De Ganes Anthony" might be complex.
            
            if sim > 0.90:
                alias_map[key] = f"Fuzzy Match ({sim:.2f})"
            elif sim > 0.8:
                 # Check word overlap
                 p1 = set(n1.lower().split())
                 p2 = set(n2.lower().split())
                 overlap = len(p1.intersection(p2))
                 # If they share significant words
                 if overlap >= min(len(p1), len(p2)) - 1 and len(p1) > 1:
                      alias_map[key] = f"Word Overlap ({sim:.2f})"
            
            # Specific case: "First Last" vs "Last First" with typo?
            # Maybe too complex for now, stick to basics.

    # Format output
    results = []
    for pair, reason in alias_map.items():
        results.append({
            "name_1": pair[0],
            "name_2": pair[1],
            "reason": reason,
            "choice": None
        })
    
    results.sort(key=lambda x: (x["reason"], x["name_1"]))
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
        
    print(f"Found {len(results)} potential alias pairs. Saved to {OUTPUT_FILE}")
    for res in results[:20]:
        print(f"{res['name_1']} <-> {res['name_2']} : {res['reason']}")

if __name__ == "__main__":
    analyze_names()
