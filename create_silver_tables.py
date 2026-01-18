import sqlite3
import pandas as pd
import os
import traceback

# --- CONFIGURATION ---
DB_FILE = "gym_data.db"

def create_silver_tables():
    """
    Creates analytical silver tables and views for the gymnastics data.
    These are "lighter" and more flexible than the final gold tables.
    """
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file '{DB_FILE}' not found.")
        return

    print("--- Creating Silver Tables and Views ---")
    
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
                        WHEN a.name LIKE 'All%Around%' THEN NULL
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

            # --- 2. Athlete Event Summary ---
            print("Creating Silver_Athlete_Event_Summary table...")
            cursor.execute("DROP TABLE IF EXISTS Silver_Athlete_Event_Summary")
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
            silver_df = pd.read_sql_query(query, conn)
            silver_df.to_sql("Silver_Athlete_Event_Summary", conn, if_exists='replace', index=False)
            print(f"  -> Created {len(silver_df)} athlete-event summary records.")
            
            # --- 3. Meet Quality Index (MQI) ---
            print("Creating Silver_Meet_Quality table...")
            cursor.execute("DROP TABLE IF EXISTS Silver_Meet_Quality")
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
            mqi_df.to_sql("Silver_Meet_Quality", conn, if_exists='replace', index=False)
            print(f"  -> Created {len(mqi_df)} meet quality records.")
            
            # Get current Results columns to avoid "no such column" errors
            cursor.execute("PRAGMA table_info(Results)")
            results_cols = {row[1] for row in cursor.fetchall()}
            
            # Expanded service columns list
            SERVICE_COLS = [
                ('level', 'level'),
                ('age', 'age'),
                ('age_group', 'age_group'),
                ('session', 'session'),
                ('group', 'group'),
                ('state', 'state'),
                ('province', 'province'),
                ('num', 'num'),
                ('meet', 'raw_meet_name'),
                ('execution_bonus', 'execution_bonus'),
                ('bonus', 'bonus'),
                ('school', 'school'),
                ('zone', 'zone'),
                ('team', 'team')
            ]

            def safe_col(col_name, alias=None):
                target = alias or col_name
                if col_name in results_cols:
                    return f'r_base."{col_name}" as "{target}"'
                else:
                    return f'NULL as "{target}"'

            service_selections = ", ".join([safe_col(c[0], c[1]) for c in SERVICE_COLS])

            # --- 4. Silver MAG Export Table (Wide Format) ---
            print("Creating Silver_MAG_Export table...")
            cursor.execute("DROP TABLE IF EXISTS Silver_MAG_Export")
            
            mag_export_query = f"""
                SELECT 
                    p.full_name as athlete_name,
                    p.gender,
                    c.name as club,
                    m.name as meet_name,
                    m.start_date_iso as meet_date,
                    m.comp_year,
                    m.country,
                    m.source,
                    
                    {service_selections},
                    
                    -- Floor (FX)
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_final END) as fx_score,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_d END) as fx_d,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.rank_numeric END) as fx_rank,
                    
                    -- Pommel Horse (PH)
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.score_final END) as ph_score,
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.score_d END) as ph_d,
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.rank_numeric END) as ph_rank,
                    
                    -- Rings (SR)
                    MAX(CASE WHEN a.name = 'Rings' THEN r.score_final END) as sr_score,
                    MAX(CASE WHEN a.name = 'Rings' THEN r.score_d END) as sr_d,
                    MAX(CASE WHEN a.name = 'Rings' THEN r.rank_numeric END) as sr_rank,
                    
                    -- Vault (VT)
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_final END) as vt_score,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_d END) as vt_d,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.rank_numeric END) as vt_rank,
                    
                    -- Parallel Bars (PB)
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.score_final END) as pb_score,
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.score_d END) as pb_d,
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.rank_numeric END) as pb_rank,
                    
                    -- High Bar (HB)
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.score_final END) as hb_score,
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.score_d END) as hb_d,
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.rank_numeric END) as hb_rank,
                    
                    -- All Around (AA)
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_final END) as aa_score,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_d END) as aa_d,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.rank_numeric END) as aa_rank
                    
                FROM Results r
                JOIN Athletes at ON r.athlete_id = at.athlete_id
                JOIN Persons p ON at.person_id = p.person_id
                JOIN Clubs c ON at.club_id = c.club_id
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                JOIN Disciplines d ON a.discipline_id = d.discipline_id
                LEFT JOIN Results r_base ON r_base.athlete_id = r.athlete_id 
                    AND r_base.meet_db_id = r.meet_db_id 
                    AND r_base.level IS NOT NULL
                WHERE d.discipline_name IN ('MAG', 'Other')
                GROUP BY p.person_id, m.meet_db_id
                ORDER BY m.start_date_iso DESC, p.full_name
            """
            
            mag_df = pd.read_sql_query(mag_export_query, conn)
            mag_df.to_sql("Silver_MAG_Export", conn, if_exists='replace', index=False)
            print(f"  -> Created {len(mag_df)} MAG export records.")
            
            # --- 5. Silver WAG Export Table (Wide Format) ---
            print("Creating Silver_WAG_Export table...")
            cursor.execute("DROP TABLE IF EXISTS Silver_WAG_Export")
            
            wag_export_query = f"""
                SELECT 
                    p.full_name as athlete_name,
                    p.gender,
                    c.name as club,
                    m.name as meet_name,
                    m.start_date_iso as meet_date,
                    m.comp_year,
                    m.country,
                    m.source,
                    
                    {service_selections},
                    
                    -- Vault (VT)
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_final END) as vt_score,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_d END) as vt_d,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.rank_numeric END) as vt_rank,
                    
                    -- Uneven Bars (UB)
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.score_final END) as ub_score,
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.score_d END) as ub_d,
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.rank_numeric END) as ub_rank,
                    
                    -- Beam (BB)
                    MAX(CASE WHEN a.name = 'Beam' THEN r.score_final END) as bb_score,
                    MAX(CASE WHEN a.name = 'Beam' THEN r.score_d END) as bb_d,
                    MAX(CASE WHEN a.name = 'Beam' THEN r.rank_numeric END) as bb_rank,
                    
                    -- Floor (FX)
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_final END) as fx_score,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_d END) as fx_d,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.rank_numeric END) as fx_rank,
                    
                    -- All Around (AA)
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_final END) as aa_score,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_d END) as aa_d,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.rank_numeric END) as aa_rank
                    
                FROM Results r
                JOIN Athletes at ON r.athlete_id = at.athlete_id
                JOIN Persons p ON at.person_id = p.person_id
                JOIN Clubs c ON at.club_id = c.club_id
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                JOIN Disciplines d ON a.discipline_id = d.discipline_id
                LEFT JOIN Results r_base ON r_base.athlete_id = r.athlete_id 
                    AND r_base.meet_db_id = r.meet_db_id 
                    AND r_base.level IS NOT NULL
                WHERE d.discipline_name IN ('WAG', 'Other')
                GROUP BY p.person_id, m.meet_db_id
                ORDER BY m.start_date_iso DESC, p.full_name
            """
            
            wag_df = pd.read_sql_query(wag_export_query, conn)
            wag_df.to_sql("Silver_WAG_Export", conn, if_exists='replace', index=False)
            print(f"  -> Created {len(wag_df)} WAG export records.")
            
            # --- 6. Pipeline Statistics View ---
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
                    (SELECT COUNT(*) FROM ScoringStandards) as scoring_standards,
                    (SELECT COUNT(*) FROM Silver_MAG_Export) as mag_export_records
            """)

            conn.commit()
            print("\n--- Silver tables and views created successfully! ---")
            
            # Summary
            stats = conn.execute("SELECT * FROM PipelineStats").fetchone()
            print(f"Summary: Meets: {stats[0]}, Results: {stats[1]}, Persons: {stats[2]}, Clubs: {stats[4]}")

    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    create_silver_tables()