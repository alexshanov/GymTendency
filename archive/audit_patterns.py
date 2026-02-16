import sqlite3
import pandas as pd

def audit_patterns():
    conn = sqlite3.connect("gym_data.db")
    
    print("--- 1. Missing Apparatus Analysis (Systemic) ---")
    query_apps = """
    SELECT 
        m.source,
        m.name as meet_name,
        r.level,
        r.gender,
        p.full_name,
        group_concat(distinct app.name) as apps_present,
        count(distinct app.name) as app_count
    FROM Results r
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    WHERE app.name NOT IN ('All Around', 'AllAround', 'AA', 'Team', 'Team Score')
    GROUP BY r.meet_db_id, r.athlete_id
    HAVING (r.gender = 'M' AND app_count < 6) OR (r.gender = 'F' AND app_count < 4)
    """
    
    df_apps = pd.read_sql_query(query_apps, conn)
    
    # Standard Sets
    std_mag = {'Floor', 'Pommel Horse', 'Rings', 'Vault', 'Parallel Bars', 'High Bar'}
    std_wag = {'Vault', 'Uneven Bars', 'Beam', 'Floor'}
    
    summary = []
    
    for _, row in df_apps.iterrows():
        present = set(row['apps_present'].split(','))
        gender = row['gender']
        
        missing = set()
        if gender == 'M':
            missing = std_mag - present
        elif gender == 'F':
            missing = std_wag - present
            
        if missing:
            row['missing_apps'] = ", ".join(sorted(list(missing)))
            summary.append(row)
            
    df_summary = pd.DataFrame(summary)
    
    if not df_summary.empty:
        # Group by pattern
        patterns = df_summary.groupby(['source', 'meet_name', 'level', 'missing_apps']).size().reset_index(name='count')
        # Filter for systemic issues (> 3 athletes)
        patterns = patterns[patterns['count'] > 3].sort_values('count', ascending=False)
        
        print(f"\nFound {len(patterns)} systemic missing apparatus patterns:")
        for _, p in patterns.iterrows():
            print(f"[{p['source']}] {p['meet_name']} (Lvl {p['level']}): Missing {p['missing_apps']} (Affects {p['count']} athletes)")
    else:
        print("No systemic missing apparatus patterns found.")

    print("\n--- 2. Missing Component Scores (D/E) ---")
    # Check rows where Final Score exists but D or E is missing
    # Excluding 'Vault' sometimes because VT often has only Final in lower levels? 
    # Actually, keep Vault, see what pops up.
    query_components = """
    SELECT 
        m.source,
        m.name as meet_name,
        app.name as apparatus,
        count(*) as count_issues
    FROM Results r
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    WHERE r.score_final > 0 
      AND (r.score_d IS NULL OR r.score_d = 0)
      AND app.name != 'All Around'
    GROUP BY m.source, m.name, app.name
    HAVING count(*) > 5
    ORDER BY count_issues DESC
    """
    
    df_comp = pd.read_sql_query(query_components, conn)
    if not df_comp.empty:
        print(f"\nFound {len(df_comp)} groups with scores but missing D-score:")
        for _, row in df_comp.iterrows():
            print(f"[{row['source']}] {row['meet_name']} - {row['apparatus']}: {row['count_issues']} entries missing D-score")
    else:
        print("No systemic missing D-scores found.")

    print("\n--- 3. Ghost Entries (AA Score but NO Apparatus) ---")
    query_ghost = """
    SELECT 
        m.source,
        m.name as meet_name,
        p.full_name
    FROM Results r
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    WHERE app.name = 'All Around'
    GROUP BY r.meet_db_id, r.athlete_id
    HAVING count(distinct CASE WHEN app.name != 'All Around' THEN r.result_id END) = 0
    """
    df_ghost = pd.read_sql_query(query_ghost, conn)
    if not df_ghost.empty:
        print(f"\nFound {len(df_ghost)} athletes with AA score but ZERO apparatus scores (Ghost Entries).")
        print(df_ghost.head(10))
    else:
        print("No ghost entries found.")

if __name__ == "__main__":
    audit_patterns()
