import pandas as pd
import sqlite3
import os
import glob
import traceback

# --- КОНФИГУРАЦИЯ ---
DB_FILE = "gym_data.db"
MEETS_CSV_FILE = "discovered_meet_ids.csv"
FINAL_CSVS_DIR = "CSVs_final"

def populate_definitions():
    """
    Заполняет справочные таблицы 'Disciplines' и 'Events' предопределенными значениями.
    Этот процесс идемпотентен: он не добавит дубликатов при повторном запуске.
    """
    print("--- Заполнение справочных таблиц (Disciplines, Events) ---")
    
    disciplines = [
        (1, 'WAG'),  # Women's Artistic Gymnastics
        (2, 'MAG'),  # Men's Artistic Gymnastics
        (99, 'Other') # Для всего остального
    ]
    
    # Снаряды с их принадлежностью к дисциплине и правильным порядком
    events = [
        # WAG Events (discipline_id = 1)
        ('Vault', 1, 1),
        ('Uneven Bars', 1, 2),
        ('Beam', 1, 3),
        # MAG Events (discipline_id = 2)
        ('Floor', 2, 1),
        ('Pommel Horse', 2, 2),
        ('Rings', 2, 3),
        ('Parallel Bars', 2, 5),
        ('High Bar', 2, 6),
        # События, общие для обеих дисциплин, но с разным порядком
        ('Floor', 1, 4), # WAG Floor
        ('Vault', 2, 4), # MAG Vault
        # Special/Other Events (discipline_id = 99)
        ('AllAround', 99, 99),
        ('Physical Preparation', 99, 100)
    ]
    
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.executemany("INSERT OR IGNORE INTO Disciplines (discipline_id, discipline_name) VALUES (?, ?)", disciplines)
            
            # Вставляем события, только если связка (имя + дисциплина) уникальна
            for event_name, disc_id, sort_order in events:
                cursor.execute("SELECT 1 FROM Events WHERE event_name = ? AND discipline_id = ?", (event_name, disc_id))
                if cursor.fetchone() is None:
                    cursor.execute("INSERT INTO Events (event_name, discipline_id, sort_order) VALUES (?, ?, ?)", 
                                   (event_name, disc_id, sort_order))
            
            conn.commit()
        print("Справочные таблицы успешно заполнены.")
        return True
    except Exception as e:
        print(f"Ошибка при заполнении справочных таблиц: {e}")
        return False

def load_meets_data():
    """
    Читает данные о соревнованиях из CSV и полностью перезаписывает таблицу 'Meets'.
    """
    # ... (Этот код остается без изменений из предыдущей версии) ...
    # Он должен выбирать и переименовывать столбцы:
    # 'MeetID', 'MeetName', 'start_date_iso', 'Location', 'Year'
    if not os.path.exists(DB_FILE) or not os.path.exists(MEETS_CSV_FILE):
        return False
    print("--- Запуск загрузки данных о соревнованиях (Meets) ---")
    try:
        df = pd.read_csv(MEETS_CSV_FILE)
        df_for_db = df[['MeetID', 'MeetName', 'start_date_iso', 'Location', 'Year']].rename(columns={
            'MeetID': 'meet_id', 'MeetName': 'name', 'start_date_iso': 'start_date_iso', 
            'Location': 'location', 'Year': 'year'
        })
        with sqlite3.connect(DB_FILE) as conn:
            df_for_db.to_sql('Meets', conn, if_exists='replace', index=False)
        print(f"Успешно перезаписано {len(df_for_db)} записей в таблицу 'Meets'.")
        return True
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных о соревнованиях: {e}")
        return False

def load_results_data():
    """
    Читает все '*_FINAL_*.csv' файлы, определяет дисциплину, переупорядочивает столбцы,
    и загружает данные в таблицы Athletes и Results.
    """
    print("\n--- Запуск загрузки данных о результатах (Results) ---")
    
    search_pattern = os.path.join(FINAL_CSVS_DIR, "*_FINAL_*.csv")
    final_csv_files = glob.glob(search_pattern)
    if not final_csv_files:
        print(f"ОШИБКА: Не найдено файлов по шаблону '{search_pattern}'.")
        return

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            print("Загружаем справочники и кеши...")
            cursor.execute("SELECT athlete_id, full_name, club FROM Athletes")
            athlete_cache = {(name, club): id for id, name, club in cursor.fetchall()}
            
            cursor.execute("SELECT event_id, event_name, discipline_id, sort_order FROM Events")
            event_cache = {}
            for event_id, event_name, disc_id, sort_order in cursor.fetchall():
                event_cache[(event_name, disc_id)] = {'id': event_id, 'sort': sort_order}

            mag_indicators = {'Pommel Horse', 'Rings', 'Parallel Bars', 'High Bar'}
            wag_indicators = {'Uneven Bars', 'Beam'}

            for i, filepath in enumerate(final_csv_files):
                filename = os.path.basename(filepath)
                print(f"\n--- Обработка файла {i+1}/{len(final_csv_files)}: {filename} ---")
                
                try:
                    df = pd.read_csv(filepath)
                    meet_id = filename.split('_FINAL_')[0]
                except (pd.errors.EmptyDataError, IndexError):
                    print("Предупреждение: Файл пустой или имеет неверное имя. Пропускаем.")
                    continue

                # --- Шаг 1: Определяем дисциплину файла и эвристику для пола ---
                column_events = {col.split('_')[1].replace('_', ' ') for col in df.columns if col.startswith('Result_')}
                
                discipline_id, discipline_name, gender_heuristic = 99, 'Other', 'Unknown'
                standard_order_map = {}

                if any(ev in mag_indicators for ev in column_events):
                    discipline_id, discipline_name, gender_heuristic = 2, 'MAG', 'M'
                    standard_order_map = {name: data['sort'] for (name, disc_id), data in event_cache.items() if disc_id == 2}
                elif any(ev in wag_indicators for ev in column_events):
                    discipline_id, discipline_name, gender_heuristic = 1, 'WAG', 'F'
                    standard_order_map = {name: data['sort'] for (name, disc_id), data in event_cache.items() if disc_id == 1}
                
                print(f"Определена дисциплина: {discipline_name}")
                
                # --- Шаг 2: Переупорядочивание столбцов ---
                info_columns = [col for col in df.columns if not col.startswith('Result_')]
                result_columns = [col for col in df.columns if col.startswith('Result_')]
                
                def get_sort_key(col_name):
                    parts = col_name.split('_')
                    event = parts[1].replace('_', ' ')
                    metric = parts[-1]
                    event_order = standard_order_map.get(event, 999)
                    metric_order = {'D': 0, 'Score': 1, 'Rnk': 2}.get(metric, 9)
                    return (event_order, metric_order)
                
                sorted_result_columns = sorted(result_columns, key=get_sort_key)
                df = df[info_columns + sorted_result_columns]

                # --- Шаг 3: Трансформация и загрузка (Melt & Pivot) ---
                long_df = pd.melt(df, id_vars=info_columns, value_vars=sorted_result_columns, var_name='result_metric', value_name='value').dropna(subset=['value'])
                if long_df.empty:
                    print("В файле не найдено непустых значений результатов. Пропускаем.")
                    continue
                
                regex_pattern = r'^(Result)_(.*?)_([^_]+)$'
                parts = long_df['result_metric'].str.extract(regex_pattern)
                parts.columns = ['prefix', 'event_name_raw', 'metric_type']
                long_df = long_df.join(parts).dropna(subset=['event_name_raw'])
                if long_df.empty: continue

                long_df['event_name'] = long_df['event_name_raw'].str.replace('_', ' ')
                
                pivot_index_cols = [col for col in ['Name', 'Club', 'Group', 'Meet', 'Age_Group', 'event_name'] if col in long_df.columns]
                pivot_df = long_df.pivot_table(index=pivot_index_cols, columns='metric_type', values='value', aggfunc='first').reset_index()

                for _, row in pivot_df.iterrows():
                    # --- Обновление данных о спортсмене (с добавлением пола) ---
                    athlete_key = (row['Name'], row.get('Club'))
                    if athlete_key not in athlete_cache:
                        cursor.execute("INSERT INTO Athletes (full_name, club, gender) VALUES (?, ?, ?)", 
                                       (row['Name'], row.get('Club'), gender_heuristic))
                        new_athlete_id = cursor.lastrowid
                        athlete_cache[athlete_key] = new_athlete_id
                    athlete_id = athlete_cache[athlete_key]

                    # --- Получение event_id теперь требует и дисциплину ---
                    event_name = row['event_name']
                    event_key = (event_name, discipline_id)
                    if event_key not in event_cache:
                        event_key = (event_name, 99) # Ищем в 'Other'
                    
                    if event_key not in event_cache:
                        print(f"Критическая ошибка: не удалось найти event_id для '{event_name}' ни в '{discipline_name}', ни в 'Other'. Пропускаем запись.")
                        continue
                        
                    event_id = event_cache[event_key]['id']
                    
                    score_val, rank_val = row.get('Score'), row.get('Rnk')
                    score_numeric, rank_numeric = pd.to_numeric(score_val, errors='coerce'), pd.to_numeric(rank_val, errors='coerce')
                    score_text = None if pd.notna(score_numeric) else str(score_val)
                    rank_text = None if pd.notna(rank_numeric) else str(rank_val)

                    cursor.execute("""
                        INSERT INTO Results (meet_id, athlete_id, event_id, group_name, age_group, 
                                             score_d, score_final, score_text, rank_numeric, rank_text)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (meet_id, athlete_id, event_id, row.get('Group'), row.get('Age_Group'), pd.to_numeric(row.get('D'), errors='coerce'), score_numeric, score_text, rank_numeric, rank_text))

                conn.commit()
                print(f"Обработано и загружено {len(pivot_df)} записей о результатах.")

    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        traceback.print_exc()

    print("\n--- Загрузка данных завершена ---")


if __name__ == "__main__":
    if populate_definitions():
        if load_meets_data():
            load_results_data()