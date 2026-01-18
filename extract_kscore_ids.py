from bs4 import BeautifulSoup
import pandas as pd
import re
from dateutil import parser
import sys
sys.path.insert(0, '.')
from etl_functions import is_tt_meet

def extract_kscore_meets_from_html(filename="meets_kscore.html"):
    """
    Читает HTML-файл от Kscore, извлекает информацию о каждом соревновании
    и сохраняет ее в CSV-файл, добавляя источник данных.
    """
    print(f"--- Чтение и парсинг файла: {filename} ---")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            html_content = f.read()
    except FileNotFoundError:
        print(f"Ошибка: Файл '{filename}' не найден. Сохраните HTML-страницу с live.kscore.ca в этот файл.")
        return None

    soup = BeautifulSoup(html_content, 'html.parser')
    
    meet_containers = soup.select('div.column.one-half')
    
    if not meet_containers:
        print("Не удалось найти ни одного контейнера с соревнованиями в HTML-файле.")
        return None

    print(f"Найдено {len(meet_containers)} соревнований.")
    
    meet_info_list = []
    for container in meet_containers:
        link_element = container.find('a', class_='event-name')
        name_element = container.find('h3')
        date_element = container.find('h4')
        
        if not (link_element and name_element and date_element):
            continue

        href = link_element.get('href', '')
        raw_id = href.split('/')[-1]
        if not raw_id:
            continue
        meet_id = f"kscore_{raw_id}"

        meet_name = name_element.get_text(strip=True)
        
        # --- T&T EXCLUSION ---
        if is_tt_meet(meet_name):
            print(f"  - Skipping T&T meet: {meet_name}")
            continue
        dates_str = date_element.get_text(strip=True)
        start_date_iso = None
        year = None

        try:
            # 1. First, find a 4-digit year in the entire string
            year_match = re.search(r'\b(20\d{2})\b', dates_str)
            if year_match:
                year = year_match.group(1)
            
            # 2. Extract the start part (before range symbols)
            start_date_only_str = dates_str.split('–')[0].split('-')[0].split('&')[0].strip()
            
            # 3. If we have a year, append it to the start part to ensure parser uses it
            if year and year not in start_date_only_str:
                parse_target = f"{start_date_only_str}, {year}"
            else:
                parse_target = start_date_only_str
                
            dt_object = parser.parse(parse_target)
            start_date_iso = dt_object.strftime('%Y-%m-%d')
            
            # Use the year from the object if we didn't find one via regex
            if not year:
                year = dt_object.year
        except (parser.ParserError, ValueError):
            print(f"  - Warning: Could not parse date from: '{dates_str}' for {meet_name}")

        meet_info_list.append({
            "Source": "kscore",
            "MeetID": meet_id,
            "MeetName": meet_name,
            "Dates": dates_str,
            "start_date_iso": start_date_iso,
            "Location": "N/A",
            "Year": year
        })
        
    return meet_info_list

# --- Основной блок для запуска скрипта ---
if __name__ == "__main__":
    
    discovered_meets = extract_kscore_meets_from_html(filename="meets_kscore.html")

    if discovered_meets:
        meets_df = pd.DataFrame(discovered_meets)
        
        column_order = ['Source', 'MeetID', 'MeetName', 'Dates', 'start_date_iso', 'Location', 'Year']
        meets_df = meets_df[column_order]

        output_csv_filename = 'discovered_meet_ids_kscore.csv'
        meets_df.to_csv(output_csv_filename, index=False)
        
        print(f"\n--- УСПЕХ ---")
        print(f"Сохранена информация о {len(meets_df)} соревнованиях в файл '{output_csv_filename}'")
        print("\nПредпросмотр данных:")
        
        # <<< ИСПРАВЛЕНИЕ ЗДЕСЬ >>>
        # Заменяем display() на print() для работы в обычном .py скрипте
        print(meets_df.head())