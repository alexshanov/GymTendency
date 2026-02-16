import sqlite3
import pandas as pd
import numpy as np

def audit_completeness():
    print("--- Comprehensive Data Integrity Audit ---")
    conn = sqlite3.connect("gym_data.db")
    
    # 1. Missing All-Around Scores
    # Athletes who have event scores but NO 'All Around' record
    print("\n[Check 1] Missing All-Around Records...")
    query_missing_aa = """
    SELECT m.source, m.name, p.full_name, COUNT(r.result_id) as event_count
    FROM Results r
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Persons p ON a.person_id = p.person_id
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    GROUP BY r.meet_db_id, r.athlete_id
    HAVING SUM(CASE WHEN app.name = 'All Around' THEN 1 ELSE 0 END) = 0
       AND COUNT(r.result_id) > 2
    """
    try:
        df_no_aa = pd.read_sql_query(query_missing_aa, conn)
        if not df_no_aa.empty:
            print(f"-> Found {len(df_no_aa)} athletes with event scores but NO AA record.")
            print(df_no_aa.head(5))
        else:
            print("-> Clean. All athletes with >2 events have an AA record.")
    except Exception as e:
        print(f"Error: {e}")

    # 2. Improper Zeroes
    # Rank exists but score is 0.0 (implies scraping gathered rank but missed score)
    print("\n[Check 2] Improper Zero Scores (Ranked but 0.0)...")
    query_zeroes = """
    SELECT m.source, m.name, app.name as app, p.full_name
    FROM Results r
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    WHERE r.score_final = 0 
      AND r.rank_numeric IS NOT NULL 
      AND r.rank_numeric < 20
    """
    df_zeroves = pd.read_sql_query(query_zeroes, conn)
    if not df_zeroves.empty:
        print(f"-> Found {len(df_zeroves)} entries with valid rank but 0.0 score.")
        print(df_zeroves.groupby(['source', 'app']).size())
    else:
        print("-> Clean.")

    # 3. Score Anomalies
    # Scores > 20.0 (likely parsing error, e.g. 14.500 -> 145.00 or rank in score col)
    print("\n[Check 3] Score Anomalies (> 20.0)...")
    query_high = """
    SELECT m.source, m.name, p.full_name, app.name, r.score_final
    FROM Results r
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    WHERE r.score_final > 20 AND app.name != 'All Around' AND app.name != 'Team'
    """
    df_high = pd.read_sql_query(query_high, conn)
    if not df_high.empty:
        print(f"-> Found {len(df_high)} impossibly high scores.")
        print(df_high.head(5))
    else:
        print("-> Clean.")

    # 4. Individual Gaps (Non-Systemic)
    # Athletes missing events in meets where MOST peers have them.
    print("\n[Check 4] Individual/Non-Systemic Gaps...")
    # This is complex. We'll look for athletes with significant deviation from the mode event count of their group.
    
    query_counts = """
    SELECT 
        m.name as meet_name,
        r.level,
        r.gender,
        p.full_name,
        count(distinct r.apparatus_id) as app_count
    FROM Results r
    JOIN Meets m ON r.meet_db_id = m.meet_db_id
    JOIN Apparatus app ON r.apparatus_id = app.apparatus_id
    JOIN Athletes a ON r.athlete_id = a.athlete_id
    JOIN Persons p ON a.person_id = p.person_id
    WHERE app.name NOT IN ('All Around', 'Team')
    GROUP BY r.meet_db_id, r.level, r.athlete_id
    """
    df_counts = pd.read_sql_query(query_counts, conn)
    
    # Calc mode per group
    stats = df_counts.groupby(['meet_name', 'level'])['app_count'].agg(lambda x: x.mode().max()).reset_index()
    stats.rename(columns={'app_count': 'mode_count'}, inplace=True)
    
    merged = pd.merge(df_counts, stats, on=['meet_name', 'level'])
    # Flag if count < mode - 1 (allowing for 1 scratch)
    outliers = merged[merged['app_count'] < (merged['mode_count'] - 1)]
    
    if not outliers.empty:
        print(f"-> Found {len(outliers)} individual athletes missing >1 events compared to peers.")
        # Sample
        print(outliers[['meet_name', 'level', 'full_name', 'app_count', 'mode_count']].head(10))
    else:
        print("-> Clean.")

if __name__ == "__main__":
    audit_completeness()
