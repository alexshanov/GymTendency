import sqlite3
import pandas as pd
import os

# --- CONFIGURATION ---
DB_FILE = "gym_data.db"

def create_gold_tables():
    """
    Creates analytical gold tables and views for the gymnastics data.
    """
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file '{DB_FILE}' not found.")
        return

    print("--- Creating Gold Tables and Views ---")
    
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            # --- 1. Normalized Scores View ---
            print("Creating NormalizedScores view...")
            cursor.execute("DROP VIEW IF EXISTS NormalizedScores")
            cursor.execute("""
                CREATE VIEW NormalizedScores AS
                SELECT 
                    r.result_id,
                    p.full_name,
                    c.name as club,
                    m.name as meet_name,
                    m.country,
                    m.comp_year,
                    r.level as original_level,
                    -- Normalized level name for matching
                    CASE 
                        WHEN r.level GLOB '[0-9]*' THEN 'Level ' || r.level
                        WHEN r.level LIKE 'CCP %' THEN 'Level ' || SUBSTR(r.level, 5)
                        WHEN r.level IN ('XB', 'XC') THEN 'Bronze'
                        WHEN r.level = 'XS' THEN 'Silver'
                        WHEN r.level = 'XG' THEN 'Gold'
                        WHEN r.level = 'XP' THEN 'Platinum'
                        WHEN r.level = 'XD' THEN 'Diamond'
                        ELSE r.level
                    END as normalized_level,
                    a.name as apparatus,
                    r.score_final,
                    ss.max_score,
                    CASE 
                        WHEN a.name LIKE '%AllAround%' OR a.name LIKE '%All Around%' THEN NULL
                        WHEN ss.max_score IS NOT NULL AND ss.max_score > 0 
                        THEN ROUND((r.score_final / ss.max_score) * 100, 2)
                        ELSE NULL 
                    END as normalized_score,
                    ss.level_system,
                    ss.has_d_score

                FROM Results r
                JOIN Athletes at ON r.athlete_id = at.athlete_id
                JOIN Persons p ON at.person_id = p.person_id
                JOIN Clubs c ON at.club_id = c.club_id
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                LEFT JOIN ScoringStandards ss ON (
                    m.country = ss.country 
                    AND ss.level_name = CASE 
                        WHEN r.level GLOB '[0-9]*' THEN 'Level ' || r.level
                        WHEN r.level LIKE 'CCP %' THEN 'Level ' || SUBSTR(r.level, 5)
                        WHEN r.level IN ('XB', 'XC') THEN 'Bronze'
                        WHEN r.level = 'XS' THEN 'Silver'
                        WHEN r.level = 'XG' THEN 'Gold'
                        WHEN r.level = 'XP' THEN 'Platinum'
                        WHEN r.level = 'XD' THEN 'Diamond'
                        ELSE r.level
                    END
                )
                WHERE r.score_final IS NOT NULL
            """)

            
            # --- 2. Athlete Progression View (updated with normalized scores) ---
            print("Creating AthleteProgression view...")
            cursor.execute("DROP VIEW IF EXISTS AthleteProgression")
            cursor.execute("""
                CREATE VIEW AthleteProgression AS
                SELECT 
                    p.full_name,
                    c.name as club,
                    r.gender,
                    m.country,
                    m.comp_year,
                    r.level,
                    a.name as apparatus,
                    COUNT(r.result_id) as appearances,
                    ROUND(AVG(r.score_final), 3) as avg_score,
                    MAX(r.score_final) as best_score,
                    ROUND(AVG(r.score_d), 3) as avg_d_score,
                    ss.max_score,
                    CASE 
                        WHEN ss.max_score IS NOT NULL AND ss.max_score > 0 
                        THEN ROUND((AVG(r.score_final) / ss.max_score) * 100, 2)
                        ELSE NULL 
                    END as avg_normalized_score
                FROM Results r
                JOIN Athletes at ON r.athlete_id = at.athlete_id
                JOIN Persons p ON at.person_id = p.person_id
                JOIN Clubs c ON at.club_id = c.club_id
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                LEFT JOIN ScoringStandards ss ON (
                    m.country = ss.country 
                    AND r.level = ss.level_name
                )
                WHERE r.score_final IS NOT NULL
                GROUP BY p.person_id, m.comp_year, r.level, a.apparatus_id
            """)
            
            # --- 3. Athlete Event Summary (updated) ---
            print("Creating Gold_Athlete_Event_Summary table...")
            cursor.execute("DROP TABLE IF EXISTS Gold_Athlete_Event_Summary")
            query = """
                SELECT
                    p.full_name,
                    c.name as club,
                    r.gender,
                    a.name as apparatus,
                    COUNT(r.result_id) AS participation_count,
                    ROUND(AVG(r.score_final), 3) AS average_score,
                    MAX(r.score_final) AS best_score,
                    MIN(r.score_final) AS worst_score,
                    ROUND(AVG(r.score_d), 3) AS average_d_score
                FROM Results r
                JOIN Athletes at ON r.athlete_id = at.athlete_id
                JOIN Persons p ON at.person_id = p.person_id
                JOIN Clubs c ON at.club_id = c.club_id
                JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                WHERE r.score_final IS NOT NULL
                GROUP BY p.person_id, a.apparatus_id
                ORDER BY p.full_name, a.name
            """
            gold_df = pd.read_sql_query(query, conn)
            gold_df.to_sql("Gold_Athlete_Event_Summary", conn, if_exists='replace', index=False)
            print(f"  -> Created {len(gold_df)} athlete-event summary records.")
            
            # --- 4. Meet Quality Index (MQI) ---
            print("Creating Gold_Meet_Quality table...")
            cursor.execute("DROP TABLE IF EXISTS Gold_Meet_Quality")
            mqi_query = """
                SELECT
                    m.meet_db_id,
                    m.source,
                    m.name as meet_name,
                    m.country,
                    m.comp_year,
                    m.competition_type,
                    COUNT(DISTINCT r.athlete_id) as athlete_count,
                    COUNT(DISTINCT r.level) as level_count,
                    COUNT(DISTINCT at.club_id) as club_count,
                    ROUND(AVG(r.score_final), 3) as avg_score,
                    ROUND(
                        (MIN(25, LOG(COUNT(DISTINCT r.athlete_id) + 1) * 10) + 
                         MIN(25, COUNT(DISTINCT r.level) * 5) +
                         MIN(25, COUNT(DISTINCT at.club_id) * 3) +
                         25) 
                    , 1) as mqi_score
                FROM Meets m
                JOIN Results r ON m.meet_db_id = r.meet_db_id
                JOIN Athletes at ON r.athlete_id = at.athlete_id
                WHERE r.score_final IS NOT NULL
                GROUP BY m.meet_db_id
                ORDER BY mqi_score DESC
            """
            mqi_df = pd.read_sql_query(mqi_query, conn)
            mqi_df.to_sql("Gold_Meet_Quality", conn, if_exists='replace', index=False)
            print(f"  -> Created {len(mqi_df)} meet quality records.")
            
            # --- 5. Pipeline Statistics View ---
            print("Creating PipelineStats view...")
            cursor.execute("DROP VIEW IF EXISTS PipelineStats")
            cursor.execute("""
                CREATE VIEW PipelineStats AS
                SELECT
                    (SELECT COUNT(*) FROM Meets) as total_meets,
                    (SELECT COUNT(*) FROM Results) as total_results,
                    (SELECT COUNT(*) FROM Persons) as total_persons,
                    (SELECT COUNT(*) FROM Athletes) as total_athlete_links,
                    (SELECT COUNT(*) FROM Clubs) as total_clubs,
                    (SELECT COUNT(*) FROM ScrapeErrors) as total_errors,
                    (SELECT COUNT(DISTINCT source) FROM Meets) as total_sources,
                    (SELECT COUNT(*) FROM ScoringStandards) as scoring_standards
            """)

            
            conn.commit()
            print("\n--- Gold tables and views created successfully! ---")
            
            # Summary
            print("\nSummary:")
            stats = conn.execute("SELECT * FROM PipelineStats").fetchone()
            print(f"  Meets: {stats[0]}, Results: {stats[1]}, Persons: {stats[2]}")
            print(f"  Clubs: {stats[4]}, Sources: {stats[6]}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_gold_tables()