# create_db.py

from etl_functions import setup_database

DB_FILE = "gym_data.db"

if __name__ == "__main__":
    print(f"--- Initializing Gymnastics Database: {DB_FILE} ---")
    if setup_database(DB_FILE):
        print("Success: Database initialized with the professional schema.")
    else:
        print("Error: Database initialization failed.")