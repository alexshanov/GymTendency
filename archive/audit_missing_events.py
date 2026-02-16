import sqlite3
import pandas as pd

def audit_db():
    conn = sqlite3.connect("gym_data.db")
    
    # Query: Get event count per athlete per meet
    # Excluding 'All Around', 'Team' from the count as they are aggregates
    query = """
    SELECT 
        m.source,
        m.name as meet_name,
        r.level,
        r.gender,
        p.full_name as sample_athlete,
        count(distinct r.apparatus_id) as app_count,
        group_concat(distinct app.name) as apps_present
    FROM Results r
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    WHERE app.name NOT IN ('All Around', 'AllAround', 'AA', 'Team', 'Team Score')
    GROUP BY r.meet_db_id, r.athlete_id
    """
    
    print("Scanning database for athlete event counts...")
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Query failed: {e}")
        return

    # Filter for suspicious counts
    # Standard: MAG=6, WAG=4. 
    # We look for strictly less than standard.
    # Note: Some lower levels might genuinely have fewer, but we flag them for review.
    suspects = df[
        ((df['gender'] == 'M') & (df['app_count'] < 6)) |
        ((df['gender'] == 'F') & (df['app_count'] < 4))
    ].copy()
    
    if suspects.empty:
        print("No matches found for <6 (MAG) or <4 (WAG) events.")
        return

    # Group by Meet+Level to find systemic issues (patterns)
    # We aggregate to count how many athletes are affected in that specific group
    summary = suspects.groupby(['source', 'meet_name', 'level', 'gender', 'app_count']).agg(
        frequency=('sample_athlete', 'count'),
        example_athlete=('sample_athlete', 'first'),
        apps_found=('apps_present', 'first')
    ).reset_index()
    
    # Filter out isolated incidents (e.g. 1 athlete scratched) to focus on patterns
    # Let's say at least 3 athletes must be affected to call it a "pattern"
    summary = summary[summary['frequency'] > 2]
    
    summary = summary.sort_values(by=['source', 'meet_name', 'frequency'], ascending=[True, True, False])
    
    print(f"Found {len(summary)} systemic pattern groups (filtered for >2 occurrences).\n")
    
    current_source = None
    expected_mag = {'Floor', 'Pommel Horse', 'Rings', 'Vault', 'Parallel Bars', 'High Bar'}
    expected_wag = {'Vault', 'Uneven Bars', 'Beam', 'Floor'} # Standard 4
    
    for _, row in summary.iterrows():
        if row['source'] != current_source:
            current_source = row['source']
            print(f"\n{'='*20} SOURCE: {current_source.upper()} {'='*20}")
            
        found_list = str(row['apps_found']).split(',')
        # Normalize names for set comparison
        found_set = set()
        for f in found_list:
            clean = f.strip()
            # Basic mapping if needed, though DB names should be clean
            found_set.add(clean)
            
        missing = set()
        if row['gender'] == 'M':
            # Handle potential mismatch in naming if DB has variations
            # checking broadly
            missing = {e for e in expected_mag if e not in found_set}
        else:
            missing = {e for e in expected_wag if e not in found_set}
            
        missing_str = ", ".join(missing) if missing else "Non-Standard Events?"
        
        print(f"MEET: {row['meet_name']}")
        print(f"  Level: {row['level']} ({row['gender']})")
        print(f"  Pattern: {row['frequency']} athletes have {row['app_count']} events")
        # print(f"  Events Found: {', '.join(found_set)}") 
        if missing:
            print(f"  LIKELY MISSING: {missing_str}")
        print(f"  Sample Athlete: {row['example_athlete']}")
        print("-" * 40)

if __name__ == "__main__":
    audit_db()
