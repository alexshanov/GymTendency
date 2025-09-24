from bs4 import BeautifulSoup
import pandas as pd

def extract_meet_ids_from_html(filename="meets.html"):
    """
    Reads a saved HTML file, parses it to find all meet cards,
    and extracts their cleaned IDs and names.
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
        full_id = card.get('id')
        clean_id = full_id[2:] # Slice off "mt"
        
        meet_name_element = card.find('h4', class_='card-title')
        meet_name = meet_name_element.get_text(strip=True).replace('Register/Login', '').replace('Login', '').replace('Results', '').strip() if meet_name_element else "Unknown Meet Name"
        
        meet_info_list.append({
            "MeetName": meet_name,
            "MeetID": clean_id
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
        print(f"Saved meet names and IDs to '{output_csv_filename}'")
        
        # Extract just the IDs into a Python list
        meet_id_list = [meet['MeetID'] for meet in discovered_meets]
        
        print("\nHere is the Python list of discovered Meet IDs:")
        print("-" * 50)
        print(meet_id_list)
        print("-" * 50)