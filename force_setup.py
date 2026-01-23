import sqlite3
from etl_functions import setup_database

DB_FILE = "gym_data.db"
print(f"Forcing setup on {DB_FILE}...")
success = setup_database(DB_FILE)
print(f"Setup success: {success}")

with sqlite3.connect(DB_FILE) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"Tables: {tables}")
