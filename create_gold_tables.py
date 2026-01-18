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
            
            # Ensure apparatus-specific bonus columns exist to avoid query failures
            from etl_functions import ensure_column_exists
            ensure_column_exists(cursor, 'Results', 'bonus', 'REAL')
            ensure_column_exists(cursor, 'Results', 'execution_bonus', 'REAL')
            
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
            
            # Get current Results columns to avoid "no such column" errors
            cursor.execute("PRAGMA table_info(Results)")
            results_cols = {row[1] for row in cursor.fetchall()}
            
            def safe_col(col_name, alias=None):
                target = alias or col_name
                if col_name in results_cols:
                    return f'r_base."{col_name}" as "{target}"'
                else:
                    return f'NULL as "{target}"'

            # --- 5. Gold MAG Export Table (Wide Format for External DB) ---
            print("Creating Gold_MAG_Export table...")
            cursor.execute("DROP TABLE IF EXISTS Gold_MAG_Export")
            
            # Pivot query to create wide format with 7 apparatus triples
            mag_export_query = f"""
                SELECT 
                    -- Athlete Identity
                    p.full_name as athlete_name,
                    p.gender,
                    c.name as club,
                    
                    -- Meet Information
                    m.name as meet_name,
                    m.start_date_iso as meet_date,
                    m.comp_year,
                    m.country,
                    m.source,
                    
                    -- Service Columns (Safe Selection)
                    {safe_col('level')},
                    {safe_col('age')},
                    {safe_col('age_group')},
                    {safe_col('session')},
                    {safe_col('group')},
                    {safe_col('state')},
                    {safe_col('province')},
                    {safe_col('num')},
                    {safe_col('meet', 'raw_meet_name')},
                    {safe_col('execution_bonus')},
                    {safe_col('bonus')},
                    
                    -- Floor (FX)
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_final END) as fx_score,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_d END) as fx_d,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.rank_numeric END) as fx_rank,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.bonus END) as fx_bonus,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.execution_bonus END) as fx_exec_bonus,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_text END) as fx_score_text,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.rank_text END) as fx_rank_text,
                    
                    -- Pommel Horse (PH)
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.score_final END) as ph_score,
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.score_d END) as ph_d,
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.rank_numeric END) as ph_rank,
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.bonus END) as ph_bonus,
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.execution_bonus END) as ph_exec_bonus,
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.score_text END) as ph_score_text,
                    MAX(CASE WHEN a.name = 'Pommel Horse' THEN r.rank_text END) as ph_rank_text,
                    
                    -- Rings (SR)
                    MAX(CASE WHEN a.name = 'Rings' THEN r.score_final END) as sr_score,
                    MAX(CASE WHEN a.name = 'Rings' THEN r.score_d END) as sr_d,
                    MAX(CASE WHEN a.name = 'Rings' THEN r.rank_numeric END) as sr_rank,
                    MAX(CASE WHEN a.name = 'Rings' THEN r.bonus END) as sr_bonus,
                    MAX(CASE WHEN a.name = 'Rings' THEN r.execution_bonus END) as sr_exec_bonus,
                    MAX(CASE WHEN a.name = 'Rings' THEN r.score_text END) as sr_score_text,
                    MAX(CASE WHEN a.name = 'Rings' THEN r.rank_text END) as sr_rank_text,
                    
                    -- Vault (VT)
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_final END) as vt_score,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_d END) as vt_d,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.rank_numeric END) as vt_rank,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.bonus END) as vt_bonus,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.execution_bonus END) as vt_exec_bonus,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_text END) as vt_score_text,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.rank_text END) as vt_rank_text,
                    
                    -- Parallel Bars (PB)
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.score_final END) as pb_score,
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.score_d END) as pb_d,
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.rank_numeric END) as pb_rank,
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.bonus END) as pb_bonus,
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.execution_bonus END) as pb_exec_bonus,
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.score_text END) as pb_score_text,
                    MAX(CASE WHEN a.name = 'Parallel Bars' THEN r.rank_text END) as pb_rank_text,
                    
                    -- High Bar (HB)
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.score_final END) as hb_score,
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.score_d END) as hb_d,
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.rank_numeric END) as hb_rank,
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.bonus END) as hb_bonus,
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.execution_bonus END) as hb_exec_bonus,
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.score_text END) as hb_score_text,
                    MAX(CASE WHEN a.name = 'High Bar' THEN r.rank_text END) as hb_rank_text,
                    
                    -- All Around (AA)
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_final END) as aa_score,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_d END) as aa_d,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.rank_numeric END) as aa_rank,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.bonus END) as aa_bonus,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.execution_bonus END) as aa_exec_bonus,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_text END) as aa_score_text,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.rank_text END) as aa_rank_text
                    
                FROM Results r
                JOIN Athletes at ON r.athlete_id = at.athlete_id
                JOIN Persons p ON at.person_id = p.person_id
                JOIN Clubs c ON at.club_id = c.club_id
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                JOIN Disciplines d ON a.discipline_id = d.discipline_id
                -- Get a base result for service columns
                LEFT JOIN Results r_base ON r_base.athlete_id = r.athlete_id 
                    AND r_base.meet_db_id = r.meet_db_id 
                    AND r_base.level IS NOT NULL
                WHERE d.discipline_name IN ('MAG', 'Other')
                GROUP BY p.person_id, m.meet_db_id
                ORDER BY m.start_date_iso DESC, p.full_name
            """
            
            mag_df = pd.read_sql_query(mag_export_query, conn)
            mag_df.to_sql("Gold_MAG_Export", conn, if_exists='replace', index=False)
            print(f"  -> Created {len(mag_df)} MAG export records.")
            
            # --- 6. Gold WAG Export Table (Wide Format for External DB) ---
            print("Creating Gold_WAG_Export table...")
            cursor.execute("DROP TABLE IF EXISTS Gold_WAG_Export")
            
            # Pivot query for WAG with 5 apparatus triples
            wag_export_query = f"""
                SELECT 
                    -- Athlete Identity
                    p.full_name as athlete_name,
                    p.gender,
                    c.name as club,
                    
                    -- Meet Information
                    m.name as meet_name,
                    m.start_date_iso as meet_date,
                    m.comp_year,
                    m.country,
                    m.source,
                    
                    -- Service Columns (Safe Selection)
                    {safe_col('level')},
                    {safe_col('age')},
                    {safe_col('age_group')},
                    {safe_col('session')},
                    {safe_col('group')},
                    {safe_col('state')},
                    {safe_col('province')},
                    {safe_col('num')},
                    {safe_col('meet', 'raw_meet_name')},
                    {safe_col('execution_bonus')},
                    {safe_col('bonus')},
                    
                    -- Vault (VT)
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_final END) as vt_score,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_d END) as vt_d,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.rank_numeric END) as vt_rank,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.bonus END) as vt_bonus,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.execution_bonus END) as vt_exec_bonus,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.score_text END) as vt_score_text,
                    MAX(CASE WHEN a.name = 'Vault' THEN r.rank_text END) as vt_rank_text,
                    
                    -- Uneven Bars (UB)
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.score_final END) as ub_score,
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.score_d END) as ub_d,
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.rank_numeric END) as ub_rank,
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.bonus END) as ub_bonus,
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.execution_bonus END) as ub_exec_bonus,
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.score_text END) as ub_score_text,
                    MAX(CASE WHEN a.name = 'Uneven Bars' THEN r.rank_text END) as ub_rank_text,
                    
                    -- Beam (BB)
                    MAX(CASE WHEN a.name = 'Beam' THEN r.score_final END) as bb_score,
                    MAX(CASE WHEN a.name = 'Beam' THEN r.score_d END) as bb_d,
                    MAX(CASE WHEN a.name = 'Beam' THEN r.rank_numeric END) as bb_rank,
                    MAX(CASE WHEN a.name = 'Beam' THEN r.bonus END) as bb_bonus,
                    MAX(CASE WHEN a.name = 'Beam' THEN r.execution_bonus END) as bb_exec_bonus,
                    MAX(CASE WHEN a.name = 'Beam' THEN r.score_text END) as bb_score_text,
                    MAX(CASE WHEN a.name = 'Beam' THEN r.rank_text END) as bb_rank_text,
                    
                    -- Floor (FX)
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_final END) as fx_score,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_d END) as fx_d,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.rank_numeric END) as fx_rank,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.bonus END) as fx_bonus,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.execution_bonus END) as fx_exec_bonus,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.score_text END) as fx_score_text,
                    MAX(CASE WHEN a.name = 'Floor' THEN r.rank_text END) as fx_rank_text,
                    
                    -- All Around (AA)
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_final END) as aa_score,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_d END) as aa_d,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.rank_numeric END) as aa_rank,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.bonus END) as aa_bonus,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.execution_bonus END) as aa_exec_bonus,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.score_text END) as aa_score_text,
                    MAX(CASE WHEN a.name LIKE 'All%Around%' THEN r.rank_text END) as aa_rank_text
                    
                FROM Results r
                JOIN Athletes at ON r.athlete_id = at.athlete_id
                JOIN Persons p ON at.person_id = p.person_id
                JOIN Clubs c ON at.club_id = c.club_id
                JOIN Meets m ON r.meet_db_id = m.meet_db_id
                JOIN Apparatus a ON r.apparatus_id = a.apparatus_id
                JOIN Disciplines d ON a.discipline_id = d.discipline_id
                -- Get a base result for service columns
                LEFT JOIN Results r_base ON r_base.athlete_id = r.athlete_id 
                    AND r_base.meet_db_id = r.meet_db_id 
                    AND r_base.level IS NOT NULL
                WHERE d.discipline_name IN ('WAG', 'Other')
                GROUP BY p.person_id, m.meet_db_id
                ORDER BY m.start_date_iso DESC, p.full_name
            """
            
            wag_df = pd.read_sql_query(wag_export_query, conn)
            wag_df.to_sql("Gold_WAG_Export", conn, if_exists='replace', index=False)
            print(f"  -> Created {len(wag_df)} WAG export records.")
            
            # --- 7. Pipeline Statistics View ---

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
                    (SELECT COUNT(*) FROM Gold_MAG_Export) as mag_export_records
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