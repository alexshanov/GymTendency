from bs4 import BeautifulSoup
import pandas as pd
import re # Import the regular expression module to find the year

def extract_meet_ids_from_html(filename="meets.html"):
    """
    Reads a saved HTML file, parses it to find all meet cards,
    and extracts their ID, name, dates, location, and year.
    """
    print(f"--- Reading and parsing {filename} ---")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            html_content = f.read()
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found. Please save the HTML first.")
        return None

    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all the div elements that have an id starting with 'mt'
    meet_cards = soup.find_all('div', id=lambda x: x and x.startswith('mt'))
    
    if not meet_cards:
        print("Could not find any meet cards in the HTML file.")
        return None

    print(f"Found {len(meet_cards)} meets.")
    
    meet_info_list = []
    for card in meet_cards:
        # --- Standard ID and Name Extraction ---
        full_id = card.get('id')
        clean_id = full_id[2:] # Slice off "mt"
        
        meet_name_element = card.find('h4', class_='card-title')
        meet_name = meet_name_element.get_text(strip=True).replace('Register/Login', '').replace('Login', '').replace('Results', '').strip() if meet_name_element else "Unknown Meet Name"
        
        # --- NEW Date, Location, and Year Extraction ---
        dates = "N/A"
        location = "N/A"
        year = "N/A"
        
        p_tag = card.find('p', class_='card-text')
        if p_tag:
            # Find location first
            location_span = p_tag.find('span', class_='float-right')
            if location_span:
                location = location_span.get_text(strip=True)
                # Remove the location span to get clean dates
                location_span.decompose()
            
            dates = p_tag.get_text(strip=True)
            
            # Extract the year from the dates string using regex
            year_match = re.search(r'(\d{4})', dates)
            if year_match:
                year = year_match.group(1)
        
        meet_info_list.append({
            "MeetID": clean_id,
            "MeetName": meet_name,
            "Dates": dates,
            "Location": location,
            "Year": year
        })
        
    return meet_info_list

# --- Main script execution ---
if __name__ == "__main__":
    
    discovered_meets = extract_meet_ids_from_html()

    if discovered_meets:
        # Create a DataFrame and save to CSV for your reference
        meets_df = pd.DataFrame(discovered_meets)
        output_csv_filename = 'discovered_meet_ids.csv'
        meets_df.to_csv(output_csv_filename, index=False)
        
        print(f"\n--- SUCCESS ---")
        print(f"Saved meet info to '{output_csv_filename}'")
        print("\nData Preview:")
        print(meets_df.head())
        
        # Extract just the IDs into a Python list
        meet_id_list = [meet['MeetID'] for meet in discovered_meets]
        
        print("\nHere is the Python list of discovered Meet IDs:")
        print("-" * 50)
        print(meet_id_list)
        print("-" * 50)