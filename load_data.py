import pandas as pd
import sqlite3
import os
import glob
import traceback

# --- КОНФИГУРАЦИЯ ---
DB_FILE = "gym_data.db"
MEETS_CSV_FILE = "discovered_meet_ids.csv"
FINAL_CSVS_DIR = "CSVs_final"

def load_meets_data():
    """
    Читает данные о соревнованиях из CSV и полностью перезаписывает таблицу 'Meets'
    с использованием новой структуры, включающей 'start_date_iso'.
    """
    if not os.path.exists(DB_FILE) or not os.path.exists(MEETS_CSV_FILE):
        print("Ошибка: Не найден DB_FILE или MEETS_CSV_FILE. Проверьте конфигурацию.")
        return False

    print("--- Запуск загрузки данных о соревнованиях (Meets) ---")
    try:
        df = pd.read_csv(MEETS_CSV_FILE)
        
        # <<< ИЗМЕНЕНИЕ 1: Выбираем только нужные столбцы >>>
        # Мы берем только те столбцы, которые соответствуют нашей новой таблице в БД.
        columns_to_load = ['MeetID', 'MeetName', 'start_date_iso', 'Location', 'Year']
        df_for_db = df[columns_to_load]

        # <<< ИЗМЕНЕНИЕ 2: Переименовываем столбцы для соответствия схеме БД >>>
        df_for_db = df_for_db.rename(columns={
            'MeetID': 'meet_id',
            'MeetName': 'name',
            # 'start_date_iso' уже имеет правильное имя, но для ясности оставим
            'start_date_iso': 'start_date_iso', 
            'Location': 'location',
            'Year': 'year'
        })
        
        with sqlite3.connect(DB_FILE) as conn:
            # Загружаем наш подготовленный DataFrame
            df_for_db.to_sql('Meets', conn, if_exists='replace', index=False)
            
        print(f"Успешно перезаписано {len(df_for_db)} записей в таблицу 'Meets'.")
        return True

    except Exception as e:
        print(f"Произошла ошибка при загрузке данных о соревнованиях: {e}")
        return False

# ==============================================================================
# Функция load_results_data остается АБСОЛЮТНО БЕЗ ИЗМЕНЕНИЙ.
# Она уже идеально спроектирована и не зависит от структуры таблицы Meets.
# ==============================================================================
def load_results_data():
    """
    Читает все '*_FINAL_*.csv' файлы и загружает их данные в таблицы
    Athletes, Events и Results, обрабатывая и числовые, и текстовые значения.
    """
    print("\n--- Запуск загрузки данных о результатах (Results) ---")

    if not os.path.isdir(FINAL_CSVS_DIR):
        print(f"ОШИБКА: Папка для CSV файлов '{FINAL_CSVS_DIR}' не найдена!")
        print(f"Скрипт ищет ее здесь: {os.getcwd()}")
        return

    search_pattern = os.path.join(FINAL_CSVS_DIR, "*_FINAL_*.csv")
    final_csv_files = glob.glob(search_pattern)
    
    if not final_csv_files:
        print(f"ОШИБКА: Не найдено ни одного файла по шаблону '{search_pattern}'.")
        return
        
    print(f"Найдено {len(final_csv_files)} файлов с результатами. Начинаю обработку...")

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            print("Загружаем существующих спортсменов и дисциплины в кеш...")
            cursor.execute("SELECT athlete_id, full_name, club FROM Athletes")
            athlete_cache = {(name, club): id for id, name, club in cursor.fetchall()}
            cursor.execute("SELECT event_id, event_name FROM Events")
            event_cache = {name: id for id, name in cursor.fetchall()}
            print(f"В кеше {len(athlete_cache)} спортсменов и {len(event_cache)} дисциплин.")

            for i, filepath in enumerate(final_csv_files):
                filename = os.path.basename(filepath)
                print(f"\n--- Обработка файла {i+1}/{len(final_csv_files)}: {filename} ---")
                
                meet_id = filename.split('_FINAL_')[0]
                
                try:
                    df = pd.read_csv(filepath)
                except pd.errors.EmptyDataError:
                    print(f"Предупреждение: Файл пустой. Пропускаем.")
                    continue

                id_vars = [col for col in ['Name', 'Club', 'Group', 'Meet', 'Age_Group'] if col in df.columns]
                result_vars = [col for col in df.columns if col.startswith('Result_') and len(col.split('_')) == 3]
                
                if not result_vars:
                    print(f"Предупреждение: В файле не найдены корректные столбцы с результатами. Пропускаем.")
                    continue

                long_df = pd.melt(df, id_vars=id_vars, value_vars=result_vars, var_name='result_metric', value_name='value')
                long_df.dropna(subset=['value'], inplace=True)
                
                if long_df.empty:
                    print("В файле не найдено непустых значений результатов. Пропускаем.")
                    continue

                parts = long_df['result_metric'].str.split('_', expand=True)
                long_df['event_name'] = parts[1]
                long_df['metric_type'] = parts[2]

                pivot_df = long_df.pivot_table(
                    index=['Name', 'Club', 'Group', 'Meet', 'Age_Group', 'event_name'],
                    columns='metric_type', values='value', aggfunc='first'
                ).reset_index()

                for _, row in pivot_df.iterrows():
                    athlete_key = (row['Name'], row.get('Club'))
                    if athlete_key not in athlete_cache:
                        cursor.execute("INSERT INTO Athletes (full_name, club) VALUES (?, ?)", (row['Name'], row.get('Club')))
                        new_athlete_id = cursor.lastrowid
                        athlete_cache[athlete_key] = new_athlete_id
                    athlete_id = athlete_cache[athlete_key]

                    event_name = row['event_name']
                    if event_name not in event_cache:
                        cursor.execute("INSERT INTO Events (event_name) VALUES (?)", (event_name,))
                        new_event_id = cursor.lastrowid
                        event_cache[event_name] = new_event_id
                    event_id = event_cache[event_name]
                    
                    score_val = row.get('Score')
                    rank_val = row.get('Rnk')
                    
                    score_numeric = pd.to_numeric(score_val, errors='coerce')
                    score_text = None if pd.notna(score_numeric) else str(score_val)

                    rank_numeric = pd.to_numeric(rank_val, errors='coerce')
                    rank_text = None if pd.notna(rank_numeric) else str(rank_val)

                    cursor.execute("""
                        INSERT INTO Results (meet_id, athlete_id, event_id, group_name, age_group, 
                                             score_d, score_final, score_text, rank_numeric, rank_text)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        meet_id, athlete_id, event_id,
                        row.get('Group'), row.get('Age_Group'),
                        pd.to_numeric(row.get('D'), errors='coerce'),
                        score_numeric, score_text,
                        rank_numeric, rank_text
                    ))

                conn.commit()
                print(f"Обработано и загружено {len(pivot_df)} записей о результатах.")

    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        traceback.print_exc()

    print("\n--- Загрузка данных завершена ---")

if __name__ == "__main__":
    if load_meets_data():
        load_results_data()