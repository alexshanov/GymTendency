from bs4 import BeautifulSoup
import pandas as pd
import re 
from dateutil import parser
import sys
sys.path.insert(0, '.')
from etl_functions import is_tt_meet

def extract_meet_ids_from_html(filename="meets_livemeet.html"):
    """
    Reads a saved HTML file from LiveMeet (SportzSoft), parses it,
    and extracts meet info, adding a 'Source' column.
    """
    print(f"--- Reading and parsing {filename} ---")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            html_content = f.read()
    except FileNotFoundError:
        print(f"Warning: The file '{filename}' was not found. LiveMeet discovery requires manual saving of the HTML page.")
        # Create empty CSV to prevent downstream errors
        pd.DataFrame(columns=['Source', 'MeetID', 'MeetName', 'Dates', 'start_date_iso', 'Location', 'Year']).to_csv('discovered_meet_ids_livemeet.csv', index=False)
        return None

    soup = BeautifulSoup(html_content, 'html.parser')
    
    meet_cards = soup.find_all('div', id=lambda x: x and x.startswith('mt'))
    
    if not meet_cards:
        print("Could not find any meet cards in the HTML file.")
        return None

    print(f"Found {len(meet_cards)} meets.")
    
    meet_info_list = []
    for card in meet_cards:
        full_id = card.get('id')
        clean_id = full_id[2:]
        
        meet_name_element = card.find('h4', class_='card-title')
        meet_name = meet_name_element.get_text(strip=True).replace('Register/Login', '').replace('Login', '').replace('Results', '').strip() if meet_name_element else "Unknown Meet Name"
        
        # --- T&T EXCLUSION ---
        if is_tt_meet(meet_name):
            print(f"  - Skipping T&T meet: {meet_name}")
            continue
        dates = "N/A"
        location = "N/A"
        year = "N/A"
        start_date_iso = None
        
        p_tag = card.find('p', class_='card-text')
        if p_tag:
            location_span = p_tag.find('span', class_='float-right')
            if location_span:
                location = location_span.get_text(strip=True)
                location_span.decompose()
            
            dates = p_tag.get_text(strip=True)
            
            year_match = re.search(r'(\d{4})', dates)
            if year_match:
                year = year_match.group(1)
            
            try:
                start_date_str = dates.split('-')[0].strip()
                dt_object = parser.parse(start_date_str, fuzzy=True)
                start_date_iso = dt_object.strftime('%Y-%m-%d')
            except (parser.ParserError, TypeError, ValueError):
                print(f"  - Warning: Could not parse date from string: '{dates}' for MeetID {clean_id}")

        
        meet_info_list.append({
            "Source": "livemeet", # <<< ИЗМЕНЕНИЕ 1: Добавляем источник
            "MeetID": clean_id,
            "MeetName": meet_name,
            "Dates": dates,
            "start_date_iso": start_date_iso,
            "Location": location,
            "Year": year
        })
        
    return meet_info_list

# --- Main script execution ---
if __name__ == "__main__":
    
    # ВАЖНО: Убедитесь, что вы сохранили HTML-код от LiveMeet в файл 'meets.html'
    discovered_meets = extract_meet_ids_from_html(filename="meets_livemeet.html")

    if discovered_meets:
        meets_df = pd.DataFrame(discovered_meets)
        
        # <<< ИЗМЕНЕНИЕ 2: Добавляем 'Source' в начало списка столбцов
        column_order = ['Source', 'MeetID', 'MeetName', 'Dates', 'start_date_iso', 'Location', 'Year']
        meets_df = meets_df[column_order]

        output_csv_filename = 'discovered_meet_ids_livemeet.csv' # Даем файлу уникальное имя
        meets_df.to_csv(output_csv_filename, index=False)
        
        print(f"\n--- SUCCESS ---")
        print(f"Saved meet info (from LiveMeet) to '{output_csv_filename}'")
        print("\nData Preview:")
        print(meets_df.head())