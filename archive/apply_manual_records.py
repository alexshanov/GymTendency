
import sqlite3

def apply_records():
    db_path = 'gym_data.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    records = [
        # Sam Smith - Copeland 2025
        {
            "athlete_name": 'Sam Smith', "source": 'livemeet', "year": '2025', 
            "meet_name": 'Copeland Classic MAG 2025 (Senior Next Gen 19-20 - Combined)', 
            "level": 'SNG', "age": '', "club": 'CGC', "fx_rank": '3', "ph_rank": '3', "sr_rank": '2', 
            "vt_rank": '3', "pb_rank": '2', "hb_rank": '2'
        },
        # Jaydon De Silva - Copeland 2022
        {
            "athlete_name": 'Jaydon De Silva', "source": 'livemeet', "year": '2022',
            "meet_name": 'Copeland Classic MAG 2022 (Elite 3 - Combined)', "level": 'E3', "age": '11.0', "club": 'CGC',
            "fx_score": '9.2', "fx_d": '10.0', "fx_rank": '1', "ph_score": '9.4', "ph_d": '9.7', "ph_rank": '2',
            "sr_score": '10.0', "sr_d": '11.0', "sr_rank": '1', "vt_score": '10.3', "vt_d": '11.0', "vt_rank": '2',
            "pb_score": '8.55', "pb_d": '10.0', "pb_rank": '1', "hb_score": '10.1', "hb_d": '10.7', "hb_rank": '1',
            "aa_score": '66.65', "aa_rank": '1'
        },
        # Isaiah Flack - Copeland 2020
        {
            "athlete_name": 'Isaiah Flack', "source": 'livemeet', "year": '2020',
            "meet_name": 'Copeland Classic MAG 2020 (Elite 3 - Combined)', "level": 'E3', "age": '', "club": 'Gymtastics',
            "fx_score": '7.2', "fx_rank": '8', "ph_score": '8.166', "ph_rank": '60', "sr_score": '7.666', "sr_rank": '7',
            "vt_score": '10.55', "vt_rank": '4', "pb_rank": '11', "hb_score": '8.066', "hb_rank": '7', "aa_score": '49.298', "aa_rank": '10'
        },
        # Isaac Hoyem - Copeland 2020
        {
            "athlete_name": 'Isaac Hoyem', "source": 'livemeet', "year": '2020',
            "meet_name": 'Copeland Classic MAG 2020 (Elite 3 - Combined)', "level": 'E3', "age": '', "club": 'CGC',
            "fx_score": '8.45', "fx_rank": '2', "ph_score": '9.066', "ph_rank": '10', "sr_score": '9.3', "sr_rank": '2',
            "vt_score": '10.4', "vt_rank": '2', "pb_score": '6.833', "pb_rank": '6', "hb_score": '9.666', "hb_rank": '3',
            "aa_score": '62.765', "aa_rank": '2'
        },
        # Elian Tong - MAG Provincials 2020
        {
            "athlete_name": 'Elian Tong', "source": 'livemeet', "year": '2020',
            "meet_name": 'MAG Artistic Provincials 2020', "level": 'P1-U10', "age": '8.73', "club": 'CGC'
        },
        # Tong Elian - Ontario Cup 2024
        {
            "athlete_name": 'Tong Elian', "source": 'ksis', "year": '2024',
            "meet_name": 'MAG | 1st Ontario Cup 2024-25', "level": 'A MAG (2011-2012)', "age": '', "club": 'Team Alberta',
            "fx_score": '11.1', "fx_d": '2.5', "fx_rank": '6', "ph_score": '8.367', "ph_d": '1.7', "ph_rank": '11',
            "sr_score": '10.767', "sr_d": '1.8', "sr_rank": '4', "vt_score": '10.6', "vt_d": '1.2', "vt_rank": '7',
            "pb_score": '11.3', "pb_d": '2.2', "pb_rank": '2', "hb_score": '10.9', "hb_d": '2.4', "hb_rank": '2',
            "aa_score": '63.034', "aa_rank": '6'
        },
        # Elian Tong - Copeland 2022
        {
            "athlete_name": 'Elian Tong', "source": 'livemeet', "year": '2022',
            "meet_name": 'Copeland Classic MAG 2022 (Elite 2 - Combined)', "level": 'E2', "age": '9.0', "club": 'CGC',
            "fx_score": '7.2', "fx_d": '10.0', "fx_rank": '2', "ph_score": '7.85', "ph_d": '10.0', "ph_rank": '1',
            "sr_score": '8.65', "sr_d": '10.0', "sr_rank": '1', "vt_score": '8.15', "vt_d": '10.0', "vt_rank": '2',
            "pb_score": '8.3', "pb_d": '10.0', "pb_rank": '2', "hb_score": '8.65', "hb_d": '10.0', "hb_rank": '2',
            "aa_score": '57.3', "aa_rank": '2'
        },
        # Majus Grabliauskas - Copeland 2022
        {
            "athlete_name": 'Majus Grabliauskas', "source": 'livemeet', "year": '2022',
            "meet_name": 'Copeland Classic MAG 2022 (Elite 3 - Combined)', "level": 'E3', "age": '11.0', "club": 'CGC',
            "fx_score": '8.7', "fx_d": '10.0', "fx_rank": '3', "ph_score": '9.0', "ph_d": '9.9', "ph_rank": '3',
            "sr_score": '8.6', "sr_d": '10.0', "sr_rank": '7', "vt_score": '10.1', "vt_d": '11.0', "vt_rank": '3',
            "pb_score": '5.7', "pb_d": '8.5', "pb_rank": '7', "hb_score": '8.7', "hb_d": '10.3', "hb_rank": '5',
            "aa_score": '57.7', "aa_rank": '4'
        },
        # Grabliauskas Majus - KScore 2019
        {
            "athlete_name": 'Grabliauskas Majus', "source": 'kscore', "year": '2019',
            "meet_name": 'Gymtastics Hollywood Classic (Session 2A - E1, E2, E3 and P2 (MAG))', "level": 'Elite 1', "age": '', "club": 'University Of Calgary',
            "fx_score": '8.3', "fx_d": '10.0', "fx_rank": '3', "ph_score": '8.9', "ph_d": '10.0', "ph_rank": '1',
            "sr_score": '5.2', "sr_d": '10.0', "sr_rank": '8', "vt_score": '9.4', "vt_d": '10.0', "vt_rank": '1',
            "pb_score": '8.8', "pb_d": '10.0', "pb_rank": '2', "hb_score": '7.55', "hb_d": '10.0', "hb_rank": '6',
            "aa_score": '57.75', "aa_d": '70.0', "aa_rank": '2'
        },
        # Majus Grabliauskas - Spruce Moose 2019
        {
            "athlete_name": 'Majus Grabliauskas', "source": 'livemeet', "year": '2019',
            "meet_name": 'Spruce Moose Invitational MAG 2019 (Session #4 9:00 AM - 1:00 PM P1, Elite 1 - Combined)', "level": 'Elite 1', "age": '9.0', "club": 'Jurassic',
            "fx_score": '8.6', "fx_d": '10.0', "fx_rank": '11', "ph_score": '6.9', "ph_d": '10.0', "ph_rank": '26',
            "sr_score": '6.9', "sr_d": '10.0', "sr_rank": '23', "vt_score": '8.4', "vt_d": '10.0', "vt_rank": '32',
            "pb_score": '6.8', "pb_d": '10.0', "pb_rank": '26', "hb_score": '7.4', "hb_d": '10.0', "hb_rank": '11',
            "aa_score": '53.5', "aa_d": 'pt', "aa_rank": '15'
        },
        # Daxton Hull - Copeland 2020
        {
            "athlete_name": 'Daxton Hull', "source": 'livemeet', "year": '2020',
            "meet_name": 'Copeland Classic MAG 2020 (Provincial 2 U12 - Combined)', "level": 'P2-U12', "age": '10.0', "club": 'Gymtastics',
            "fx_score": '8.7', "fx_d": '10.0', "fx_rank": '4', "ph_rank": '70',
            "sr_score": '8.2', "sr_d": '10.0', "sr_rank": '4', "vt_score": '9.35', "vt_d": '10.0', "vt_rank": '4',
            "pb_score": '6.7', "pb_rank": '90', "hb_rank": '20', "aa_score": '57.1', "aa_rank": '5'
        },
        # Liam Sutton - Copeland 2023
        {
            "athlete_name": 'Liam Sutton', "source": 'livemeet', "year": '2023',
            "meet_name": 'Copeland Classic MAG 2023 (Open - Combined)', "level": 'O', "age": '18.0', "club": 'CGC',
            "fx_score": '10.8', "fx_d": '3.6', "fx_rank": '6', "ph_score": '11.25', "ph_d": '2.5', "ph_rank": '1',
            "sr_rank": '7', "vt_rank": '6', "pb_score": '11.8', "pb_d": '3.0', "pb_rank": '2', "hb_score": '9.35', "hb_d": '2.2', "hb_rank": '5',
            "aa_score": '43.2', "aa_d": '11.3', "aa_rank": '6'
        },
        # Liam Sutton - Provincials 2023
        {
            "athlete_name": 'Liam Sutton', "source": 'livemeet', "year": '2023',
            "meet_name": 'MAG Artistic Provincials (National Open - Combined)', "level": 'nan', "age": '18.0', "club": 'CGC',
            "fx_rank": '8', "ph_score": '8.366', "ph_d": '2.5', "ph_rank": '7', "sr_rank": '7', "vt_rank": '7',
            "pb_score": '11.533', "pb_d": '3.0', "pb_rank": '4', "hb_rank": '7', "aa_score": '19.899', "aa_d": '5.5', "aa_rank": '8'
        },
        # Cooper Mizera - MAG Provincials 2020
        {
            "athlete_name": 'Cooper Mizera', "source": 'livemeet', "year": '2020',
            "meet_name": 'MAG Artistic Provincials 2020', "level": 'P1-U10', "age": '8.88', "club": 'CGC'
        },
        # Cooper Mizera - Summit 2019
        {
            "athlete_name": 'Cooper Mizera', "source": 'livemeet', "year": '2019',
            "meet_name": '2019 Summit Invitational MAG (Provincial 1 (under 8) - Combined)', "level": 'P1-U8', "age": '', "club": 'CGC',
            "fx_score": '6.5', "fx_d": '9.0', "fx_rank": '4', "ph_score": '6.5', "ph_d": '10.0', "ph_rank": '5',
            "sr_score": '7.4', "sr_d": '10.0', "sr_rank": '2', "vt_score": '7.7', "vt_d": '9.0', "vt_rank": '6',
            "pb_score": '7.6', "pb_d": '10.0', "pb_rank": '3', "hb_score": '5.0', "hb_d": '10.0', "hb_rank": '6',
            "aa_score": '48.7'
        },
        # Thoren Lawrence - Copeland 2020
        {
            "athlete_name": 'Thoren Lawrence', "source": 'livemeet', "year": '2020',
            "meet_name": 'Copeland Classic MAG 2020 (Elite 3 - Combined)', "level": 'E3', "age": '', "club": 'CGC',
            "fx_score": '8.15', "fx_rank": '3', "ph_score": '9.233', "ph_rank": '20', "sr_score": '9.0', "sr_rank": '3',
            "vt_score": '10.5', "vt_rank": '3', "pb_score": '8.4', "pb_rank": '1', "hb_score": '10.233', "hb_rank": '2',
            "aa_score": '62.316', "aa_rank": '3'
        },
        # Thoren Lawrence - Spruce Moose 2019
        {
            "athlete_name": 'Thoren Lawrence', "source": 'livemeet', "year": '2019',
            "meet_name": 'Spruce Moose Invitational MAG 2019 (Session #3 1:30 PM - 6:00 PM Elite 3, Elite 4, P4 - Combined)', "level": 'E3', "age": '11.0', "club": 'CGC',
            "fx_score": '7.45', "fx_d": '10.1', "fx_rank": '37', "ph_score": '7.0', "ph_d": '10.1', "ph_rank": '32',
            "sr_score": '7.8', "sr_d": '10.0', "sr_rank": '33', "vt_score": '9.8', "vt_d": '11.0', "vt_rank": '31',
            "pb_score": '7.2', "pb_d": '10.0', "pb_rank": '31', "hb_score": '8.0', "hb_d": '10.0', "hb_rank": '29',
            "aa_score": '47.25', "aa_d": '61.2', "aa_rank": '30'
        },
        # Drew Pettigrew - Fred Turoff 2021
        {
            "athlete_name": 'Drew Pettigrew', "source": 'livemeet', "year": '2021',
            "meet_name": 'Fred Turoff Invitational (Level 4 Division 2 - Combined)', "level": 'M4D2', "age": '10.0', "club": 'Gymsport Gents',
            "fx_score": '7.6', "fx_rank": '7', "ph_score": '7.5', "ph_rank": '7', "sr_score": '7.8', "sr_rank": '5',
            "vt_score": '9.1', "vt_rank": '5', "pb_score": '7.8', "pb_rank": '6', "hb_score": '8.6', "hb_rank": '5',
            "aa_score": '48.4', "aa_rank": '8'
        },
        # George Pettigrew - Ontario Cup 2024
        {
            "athlete_name": 'George Pettigrew', "source": 'ksis', "year": '2024',
            "meet_name": 'MAG | 1st Ontario Cup 2024-25', "level": 'A MAG (2011-2012)', "age": '', "club": 'Team Alberta',
            "fx_score": '10.033', "fx_d": '2.6', "fx_rank": '10', "ph_score": '9.767', "ph_d": '1.8', "ph_rank": '8',
            "sr_score": '10.567', "sr_d": '1.8', "sr_rank": '5', "vt_score": '10.7', "vt_d": '1.2', "vt_rank": '6',
            "pb_score": '10.767', "pb_d": '2.1', "pb_rank": '5', "hb_score": '9.633', "hb_d": '1.8', "hb_rank": '7',
            "aa_score": '61.467', "aa_rank": '7'
        },
        # Anton Prosolin - Summit 2019
        {
            "athlete_name": 'Anton Prosolin', "source": 'livemeet', "year": '2019',
            "meet_name": '2019 Summit Invitational MAG (Provincial 1 (under 8) - Combined)', "level": 'P1-U8', "age": '', "club": 'CGC',
            "fx_score": '7.7', "fx_d": '10.0', "fx_rank": '3', "ph_score": '5.0', "ph_d": '10.0', "ph_rank": '7',
            "sr_score": '6.0', "sr_d": '10.0', "sr_rank": '4', "vt_score": '7.1', "vt_d": '9.0', "vt_rank": '7',
            "pb_score": '6.5', "pb_d": '10.0', "pb_rank": '7', "hb_score": '6.5', "hb_d": '10.0', "hb_rank": '2',
            "aa_score": '44.8'
        },
        # Anton Prosolin - Ontario Cup 2024
        {
            "athlete_name": 'Anton Prosolin', "source": 'ksis', "year": '2024',
            "meet_name": 'MAG | 1st Ontario Cup 2024-25', "level": 'A MAG (2011-2012)', "age": '', "club": 'Team Alberta',
            "fx_score": '11.667', "fx_d": '3.3', "fx_rank": '5', "ph_score": '10.367', "ph_d": '1.9', "ph_rank": '5',
            "sr_score": '9.9', "sr_d": '1.8', "sr_rank": '8', "vt_score": '10.1', "vt_d": '1.8', "vt_rank": '10',
            "pb_score": '9.333', "pb_d": '2.4', "pb_rank": '9', "hb_score": '9.633', "hb_d": '1.8', "hb_rank": '7',
            "aa_score": '61.0', "aa_rank": '8'
        }
    ]

    for rec in records:
        # First, clear any existing matching records to avoid duplicates
        cursor.execute("DELETE FROM Gold_Results_MAG WHERE athlete_name = ? AND meet_name = ?", (rec['athlete_name'], rec['meet_name']))
        
        columns = rec.keys()
        placeholders = ', '.join(['?'] * len(columns))
        query = f"INSERT INTO Gold_Results_MAG ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.execute(query, list(rec.values()))
        print(f"Applied record for {rec['athlete_name']} at {rec['meet_name']}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    apply_records()
