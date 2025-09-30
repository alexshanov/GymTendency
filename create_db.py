import sqlite3
import os

DB_FILE = "gym_data.db"

def create_database():
    """
    Создает или пересоздает базу данных SQLite с необходимой структурой таблиц.
    """
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Существующий файл базы данных '{DB_FILE}' удален.")

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        print(f"База данных '{DB_FILE}' создана, соединение установлено.")

        cursor.execute("""
            CREATE TABLE Meets (
                meet_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                dates TEXT,
                location TEXT,
                year INTEGER
            );
        """)

        cursor.execute("""
            CREATE TABLE Athletes (
                athlete_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                club TEXT,
                UNIQUE (full_name, club)
            );
        """)

        cursor.execute("""
            CREATE TABLE Events (
                event_id INTEGER PRIMARY KEY,
                event_name TEXT NOT NULL UNIQUE
            );
        """)

        cursor.execute("""
            CREATE TABLE Results (
                result_id INTEGER PRIMARY KEY,
                meet_id TEXT,
                athlete_id INTEGER,
                event_id INTEGER,
                group_name TEXT,
                age_group TEXT,
                score_d REAL,
                score_final REAL,
                score_text TEXT,
                rank_numeric INTEGER,
                rank_text TEXT,
                FOREIGN KEY (meet_id) REFERENCES Meets (meet_id),
                FOREIGN KEY (athlete_id) REFERENCES Athletes (athlete_id),
                FOREIGN KEY (event_id) REFERENCES Events (event_id)
            );
        """)

        conn.commit()
        print("Все таблицы ('Meets', 'Athletes', 'Events', 'Results') успешно созданы.")

    except sqlite3.Error as e:
        print(f"Произошла ошибка SQLite: {e}")
    
    finally:
        if conn:
            conn.close()
            print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    create_database()