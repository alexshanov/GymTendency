import sqlite3
import os

DB_FILE = "gym_data.db"

def create_database():
    """
    Создает или пересоздает базу данных SQLite с финальной, правильной архитектурой,
    которая корректно обрабатывает дублирующиеся названия снарядов (Floor, Vault).
    """
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Существующий файл базы данных '{DB_FILE}' удален для создания новой структуры.")

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            print(f"База данных '{DB_FILE}' создана, соединение установлено.")

            # --- Таблицы Disciplines, Meets, Athletes (без изменений) ---
            cursor.execute("""
                CREATE TABLE Disciplines ( discipline_id INTEGER PRIMARY KEY, discipline_name TEXT NOT NULL UNIQUE );
            """)
            cursor.execute("""
                CREATE TABLE Meets ( meet_id TEXT PRIMARY KEY, name TEXT NOT NULL, start_date_iso TEXT, location TEXT, year INTEGER );
            """)
            cursor.execute("""
                CREATE TABLE Athletes ( athlete_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, club TEXT, gender TEXT, UNIQUE (full_name, club) );
            """)

            # --- ОБНОВЛЕННАЯ Таблица Events ---
            # Убрано ограничение UNIQUE с event_name.
            # Добавлено UNIQUE ограничение на КОМБИНАЦИЮ (event_name, discipline_id).
            cursor.execute("""
                CREATE TABLE Events (
                    event_id INTEGER PRIMARY KEY,
                    event_name TEXT NOT NULL,
                    discipline_id INTEGER NOT NULL,
                    sort_order INTEGER,
                    FOREIGN KEY (discipline_id) REFERENCES Disciplines (discipline_id),
                    UNIQUE (event_name, discipline_id)
                );
            """)

            # --- Таблица Results (без изменений в структуре) ---
            cursor.execute("""
                CREATE TABLE Results (
                    result_id INTEGER PRIMARY KEY,
                    meet_id TEXT, athlete_id INTEGER, event_id INTEGER,
                    group_name TEXT, age_group TEXT,
                    score_d REAL, score_final REAL, score_text TEXT,
                    rank_numeric INTEGER, rank_text TEXT,
                    FOREIGN KEY (meet_id) REFERENCES Meets (meet_id),
                    FOREIGN KEY (athlete_id) REFERENCES Athletes (athlete_id),
                    FOREIGN KEY (event_id) REFERENCES Events (event_id)
                );
            """)
            
            conn.commit()
            print("Все таблицы успешно созданы с финальной, корректной структурой.")

    except sqlite3.Error as e:
        print(f"Произошла ошибка SQLite: {e}")
    
    finally:
        print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    create_database()