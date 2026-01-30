import sqlite3

def apply_user_records():
    db_path = 'gym_data.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # The user provided 4 records. 
    # They are for Alton Paige, Altvater Paityn, Alula Joseph, Alves Corriveau.
    # Note: Altvater Paityn and Alula Joseph seem complete, but user wants them merged.
    
    records = [
        ('Alton Paige', 'ksis', '2025', 'MEET\n                        FULL - OCP/PAIRS/XCEL - WAG 35th Annual Burlington Spring Cup', 'Level 2 age 10 - B WAG (2015)', '', 'Gymnastics Energy', '', '', '', '', '', '', '9.75', '10.0', '2', '', '', '', '38.1', '', '4'),
        ('Altvater Paityn', 'kscore', '2025', 'Ed Vincent Invitational (WAG Session 9A: Level 7 (WAG))', 'CCP 7B', '', 'Gymtastics Gym Club', '9.133', '10.0', '3', '7.666', '10.0', '6', '8.733', '9.5', '4', '9.066', '10.0', '8', '34.598', '39.5', '6'),
        ('Alula Joseph', 'livemeet', '2025', 'SEQ WAG Senior Regional Championships (Level 7 U13 - Combined)', 'L7U13', '12.0', 'Pga', '11.866', '2.5', '3', '12.8', '3.4', '2', '12.0', '3.3', '6', '11.95', '3.5', '6', '48.616', '12.7', '4'),
        ('Alves Corriveau', 'ksis', '2025', 'MEET\n                        FULL - OCP/PAIRS/XCEL - WAG 35th Annual Burlington Spring Cup', 'PAIRS - Level 9 WAG ()', '', 'Aim Gymnastics/Gym-Trm', '', '', '', '', '', '', '8.75', '0.2', '3', '', '', '', '35.3', '', '4')
    ]

    cols = ["athlete_name", "source", "year", "meet_name", "level", "age", "club", "vt_score", "vt_d", "vt_rank", "ub_score", "ub_d", "ub_rank", "bb_score", "bb_d", "bb_rank", "fx_score", "fx_d", "fx_rank", "aa_score", "aa_d", "aa_rank"]
    
    for rec in records:
        print(f"Applying manual record for {rec[0]}...")
        # Clear existing
        cursor.execute("DELETE FROM Gold_Results_WAG WHERE athlete_name = ? AND meet_name = ?", (rec[0], rec[3]))
        
        placeholders = ', '.join(['?'] * len(cols))
        query = f"INSERT INTO Gold_Results_WAG ({', '.join(cols)}) VALUES ({placeholders})"
        cursor.execute(query, rec)

    conn.commit()
    conn.close()
    print("Done applying manual WAG records.")

if __name__ == "__main__":
    apply_user_records()
