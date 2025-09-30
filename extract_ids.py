from bs4 import BeautifulSoup
import pandas as pd
import re 
from dateutil import parser

def extract_meet_ids_from_html(filename="meets.html"):
    """
    Reads a saved HTML file, parses it to find all meet cards,
    and extracts their ID, name, dates, location, year, and a clean ISO start date.
    --- UPDATED to handle date ranges correctly ---
    """
    print(f"--- Reading and parsing {filename} ---")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            html_content = f.read()
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found. Please save the HTML first.")
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
                # <<< ЭТО И ЕСТЬ ИСПРАВЛЕНИЕ >>>
                # Берем только часть строки до первого дефиса, чтобы отсечь дату окончания
                start_date_str = dates.split('-')[0].strip()

                # Теперь передаем парсеру только чистую дату начала
                dt_object = parser.parse(start_date_str, fuzzy=True)
                start_date_iso = dt_object.strftime('%Y-%m-%d')
                
            except (parser.ParserError, TypeError, ValueError):
                # ValueError добавлен на случай, если после split останется пустая строка
                print(f"  - Warning: Could not parse date from string: '{dates}' for MeetID {clean_id}")

        
        meet_info_list.append({
            "MeetID": clean_id,
            "MeetName": meet_name,
            "Dates": dates,
            "start_date_iso": start_date_iso,
            "Location": location,
            "Year": year
        })
        
    return meet_info_list

# --- Main script execution (остается без изменений) ---
if __name__ == "__main__":
    
    discovered_meets = extract_meet_ids_from_html()

    if discovered_meets:
        meets_df = pd.DataFrame(discovered_meets)
        
        column_order = ['MeetID', 'MeetName', 'Dates', 'start_date_iso', 'Location', 'Year']
        meets_df = meets_df[column_order]

        output_csv_filename = 'discovered_meet_ids.csv'
        meets_df.to_csv(output_csv_filename, index=False)
        
        print(f"\n--- SUCCESS ---")
        print(f"Saved meet info (including 'start_date_iso') to '{output_csv_filename}'")
        print("\nData Preview:")
        print(meets_df.head())