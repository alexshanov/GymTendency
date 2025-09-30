import pandas as pd
import requests
import time
import os
import json
import io
import traceback

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- КОНФИГУРАЦИЯ ---
KSCORE_MEETS_CSV = "discovered_meet_ids_kscore.csv"
OUTPUT_DIR_KSCORE = "CSVs_final_kscore"
DEBUG_LIMIT = 1 # Установите > 0 для отладки (например, 3, чтобы обработать только первые 3 соревнования)

def scrape_kscore_meet(meet_id, meet_name, output_dir):
    """
    Основная функция для скрапинга одного соревнования с сайта Kscore.
    --- ИСПРАВЛЕНА ОШИБКА: Имитирует заголовки браузера, чтобы избежать ошибки 403 Forbidden. ---
    """
    raw_meet_id = meet_id.replace('kscore_', '')
    base_url = f"https://live.kscore.ca/results/{raw_meet_id}"
    
    print(f"--- Начинаю обработку Kscore meet: {meet_name} ({meet_id}) ---")

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = None
    saved_files_count = 0
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        driver.get(base_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#sel-sess option:not([value=''])"))
        )
        
        # Собираем "паспорт" браузера для имитации
        user_agent = driver.execute_script("return navigator.userAgent;")
        headers = {
            'User-Agent': user_agent,
            'Referer': base_url,
            'X-Requested-With': 'XMLHttpRequest'
        }
        cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        
        sessions = [{'id': el.get_attribute('value'), 'name': el.text} for el in driver.find_elements(By.CSS_SELECTOR, "#sel-sess option:not([value=''])")]
        print(f"Найдено {len(sessions)} сессий.")

        for session in sessions:
            print(f"  -- Обработка сессии: {session['name']} (ID: {session['id']}) --")
            
            js_script = f"""
                var callback = arguments[0];
                $.ajax({{
                    url: 'src/query_scoring_groups.php',
                    data: 'sess=["{session['id']}"]',
                    dataType: 'json', type: 'GET',
                    success: function (resultArray) {{ callback(resultArray); }},
                    error: function() {{ callback(null); }}
                }});
            """
            categories_result = driver.execute_async_script(js_script)
            
            if not categories_result:
                print("    Не удалось получить категории для этой сессии.")
                continue

            for cat_id, cat_info in enumerate(categories_result):
                if not cat_info: continue
                group_name = session['name']
                age_group = cat_info['name']
                print(f"    -> Скрапинг категории: {age_group} (ID: {cat_id})")

                results_url = f"https://live.kscore.ca/results/{raw_meet_id}/src/query_custom_results.php"
                params = {
                    'event': 0, 'discip': cat_info.get('discip'),
                    'cat': json.dumps(cat_info.get('members')),
                    'sess': json.dumps(cat_info.get('mSess', cat_info.get('sess')))
                }
                
                response = requests.get(results_url, params=params, cookies=cookies, headers=headers)
                response.raise_for_status()
                
                html_table = response.text
                if not html_table or "There are no results" in html_table:
                    print("       В этой категории нет таблицы с результатами.")
                    continue

                df_list = pd.read_html(io.StringIO(html_table))
                if not df_list: continue
                
                df = df_list[0]
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
                
                df['Meet'] = meet_name
                df['Group'] = group_name
                df['Age_Group'] = age_group
                
                output_filename = f"{meet_id}_FINAL_{session['id']}_{cat_id}.csv"
                output_path = os.path.join(output_dir, output_filename)
                df.to_csv(output_path, index=False)
                saved_files_count += 1
                
        return saved_files_count

    except Exception as e:
        print(f"Произошла критическая ошибка при обработке {meet_id}: {e}")
        traceback.print_exc()
        return saved_files_count
    finally:
        if driver:
            driver.quit()
            
                      
# --- ОСНОВНОЙ БЛОК ЗАПУСКА ---
if __name__ == "__main__":
    
    # --- Настройка ---
    os.makedirs(OUTPUT_DIR_KSCORE, exist_ok=True)
    
    try:
        meets_df = pd.read_csv(KSCORE_MEETS_CSV)
        print(f"Найдено {len(meets_df)} соревнований для обработки из файла '{KSCORE_MEETS_CSV}'.")
    except FileNotFoundError:
        print(f"ОШИБКА: Файл '{KSCORE_MEETS_CSV}' не найден. Сначала запустите скрипт extract_kscore_ids.py.")
        exit()

    # Применяем DEBUG_LIMIT, если он установлен
    if DEBUG_LIMIT > 0:
        meets_df = meets_df.head(DEBUG_LIMIT)
        print(f"--- РЕЖИМ ОТЛАДКИ: Будет обработано только {len(meets_df)} соревнований. ---")

    # --- Основной цикл ---
    total_files_created = 0
    for index, row in meets_df.iterrows():
        # Извлекаем необходимые данные из строки CSV
        meet_id = row['MeetID']
        meet_name = row['MeetName']
        
        files_count = scrape_kscore_meet(
            meet_id=meet_id,
            meet_name=meet_name,
            output_dir=OUTPUT_DIR_KSCORE
        )
        
        if files_count > 0:
            print(f"--- ✅ Успешно сохранено {files_count} файлов для {meet_name} ---")
            total_files_created += files_count
        else:
            print(f"--- ❌ Для {meet_name} не было сохранено ни одного файла. ---")
            
        time.sleep(2) # Небольшая пауза между соревнованиями

    print(f"\n--- ЗАВЕРШЕНО ---")
    print(f"Всего создано {total_files_created} CSV-файлов в папке '{OUTPUT_DIR_KSCORE}'.")