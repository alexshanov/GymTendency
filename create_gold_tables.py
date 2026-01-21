import sqlite3
import pandas as pd
import os

# --- КОНФИГУРАЦИЯ ---
DB_FILE = "gym_data.db"
GOLD_TABLE_NAME = "Gold_Athlete_Event_Summary"

def create_athlete_summary_table():
    """
    Подключается к базе данных, выполняет агрегирующий SQL-запрос
    и создает новую 'Gold' таблицу со сводной статистикой по спортсменам.
    """
    if not os.path.exists(DB_FILE):
        print(f"Ошибка: Файл базы данных '{DB_FILE}' не найден.")
        return

    print(f"--- Создание Gold-таблицы: {GOLD_TABLE_NAME} ---")
    
    try:
        # Используем 'with' для автоматического закрытия соединения
        with sqlite3.connect(DB_FILE) as conn:
            
            # --- Шаг 1: SQL-запрос для агрегации данных ---
            # Этот запрос - сердце всего процесса. Он объединяет таблицы
            # и вычисляет статистику.
            query = """
                SELECT
                    a.full_name,
                    a.club,
                    e.event_name,
                    COUNT(r.result_id) AS participation_count,
                    AVG(r.score_final) AS average_score,
                    MAX(r.score_final) AS best_score,
                    MIN(r.score_final) AS worst_score
                FROM
                    Results r
                JOIN
                    Athletes a ON r.athlete_id = a.athlete_id
                JOIN
                    Events e ON r.event_id = e.event_id
                WHERE
                    r.score_final IS NOT NULL  -- Считаем статистику только по числовым оценкам
                GROUP BY
                    a.full_name, a.club, e.event_name -- Группируем, чтобы агрегаты считались для каждого спортсмена в каждой дисциплине
                ORDER BY
                    a.full_name, e.event_name;
            """
            
            print("Выполняем SQL-запрос для агрегации данных...")
            # pd.read_sql_query выполняет запрос и сразу загружает результат в DataFrame
            gold_df = pd.read_sql_query(query, conn)
            
            # Округляем результаты для красоты
            gold_df['average_score'] = gold_df['average_score'].round(3)
            
            print(f"Агрегация завершена. Получено {len(gold_df)} сводных записей.")
            
            # --- Шаг 2: Сохранение результата в новую таблицу в БД ---
            # if_exists='replace' означает, что при каждом запуске скрипта
            # эта таблица будет полностью перезаписываться свежими данными.
            print(f"Сохраняем результаты в новую таблицу '{GOLD_TABLE_NAME}'...")
            gold_df.to_sql(GOLD_TABLE_NAME, conn, if_exists='replace', index=False)
            
            print("Gold-таблица успешно создана/обновлена в базе данных!")

            # (Опционально) Сохранить также и в CSV для быстрой проверки
            # gold_df.to_csv("gold_athlete_summary.csv", index=False)

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()

# --- Основной блок для запуска скрипта ---
if __name__ == "__main__":
    create_athlete_summary_table()