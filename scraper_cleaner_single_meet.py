import pandas as pd
import io
import json
from bs4 import BeautifulSoup
import re
import html

# --- Selenium Imports ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def clean_scraped_table(df_raw):
    """
    Takes a raw DataFrame and robustly cleans it by finding the data's
    start position based on the 'Age' column.
    """
    if df_raw.empty:
        return None

    # Find the likely 'Age' column (usually 4th or 5th)
    age_col_idx = -1
    for i in range(min(10, df_raw.shape[1])):
        if pd.to_numeric(df_raw[i], errors='coerce').notna().any():
            age_col_idx = i
            break
    
    if age_col_idx == -1: return None # No numeric age column found

    # Find the first row of actual data
    first_data_row = pd.to_numeric(df_raw[age_col_idx], errors='coerce').first_valid_index()
    if first_data_row is None or first_data_row < 2: return None

    # Based on the data start, identify the header rows
    sub_header_row = first_data_row - 1
    main_header_row = first_data_row - 2

    main_headers = df_raw.iloc[main_header_row].ffill()
    sub_headers = df_raw.iloc[sub_header_row]

    clean_columns = []
    for h1, h2 in zip(main_headers, sub_headers):
        h1 = str(h1).strip()
        h2 = str(h2).strip()
        if h2 in ['SV', 'D', 'Score', 'Rnk'] and h1 != 'nan':
            clean_columns.append(f"{h1}_{h2}")
        else:
            clean_columns.append(h1)
            
    df_data = df_raw.iloc[first_data_row:].copy()
    df_data.columns = clean_columns
    
    df_data = df_data[pd.to_numeric(df_data['Age'], errors='coerce').notna()]
    if '#' in df_data.columns:
        df_data = df_data.drop(columns='#')
        
    return df_data.reset_index(drop=True)


def scrape_and_clean_full_meet(main_page_url):
    """
    Handles the entire scraping and cleaning process.
    """
    print("--- Initializing Selenium Browser ---")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    driver = None
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # --- PHASE 1: DISCOVERY ---
        print("--- Phase 1: Discovering Meet Name, SessionId, and Events ---")
        driver.get(main_page_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "liCategory")))
        print("Event list loaded.")

        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        meet_name_element = soup.select_one("div#thHeader.TournamentHeader div div.TournamentHeading")
        meet_name = meet_name_element.get_text(strip=True) if meet_name_element else "Unknown Meet"
        print(f"Successfully discovered Meet Name: {meet_name}")
        
        session_id_match = re.search(r'SessionId=([a-zA-Z0-9]+)', html_content)
        if not session_id_match:
            print("CRITICAL ERROR: Could not find the active SessionId. Aborting.")
            return None
        active_session_id = session_id_match.group(1)
        print(f"Successfully discovered active SessionId: {active_session_id}")

        event_elements = soup.find_all('li', class_='liCategory')
        events_to_scrape = {el.get_text(strip=True): el.get('id') for el in event_elements if el.get_text(strip=True) and el.get('id')}
        print(f"Discovery complete. Found {len(events_to_scrape)} events.")

        # --- PHASE 2: SCRAPING AND CLEANING ---
        all_clean_dfs = []
        base_data_url = "https://www.sportzsoft.com/meet/meetWeb.dll/TournamentResults"
        print("\n--- Phase 2: Scraping and Cleaning data for each event ---")
        
        for event_name, division_id in events_to_scrape.items():
            try:
                data_url = f"{base_data_url}?DivId={division_id}&SessionId={active_session_id}"
                print(f"Processing: {event_name}")
                driver.get(data_url)

                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
                json_text = driver.find_element(By.TAG_NAME, 'pre').text
                data = json.loads(json_text)
                decoded_html = html.unescape(data['html'])
                
                df_raw_list = pd.read_html(io.StringIO(decoded_html), attrs={'id': 'sessionEventResults'}, header=None)
                if not df_raw_list: continue
                
                # Call our new, robust cleaning function
                df_clean = clean_scraped_table(df_raw_list[0])
                
                if df_clean is not None and not df_clean.empty:
                    df_clean['Event'] = event_name
                    df_clean['Meet'] = meet_name
                    all_clean_dfs.append(df_clean)
                    print(f"--> Success! Processed {len(df_clean)} rows.")
                else:
                    print(f"--> Info: No valid athlete data found for {event_name}.")


            except Exception as e:
                print(f"--> Warning: Could not process data for {event_name}. Reason: {e}")
        
        return all_clean_dfs

    except Exception as e:
        print(f"An unrecoverable error occurred: {e}")
        return None
    finally:
        print("\n--- Finalizing: Closing browser ---")
        if driver:
            driver.quit()

# --- Main script execution ---
MAIN_PAGE_URL = "https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id=C5432FCE37715FF3C29F88080A34FDD6"
list_of_clean_dataframes = scrape_and_clean_full_meet(MAIN_PAGE_URL)

if list_of_clean_dataframes:
    master_df = pd.concat(list_of_clean_dataframes, ignore_index=True)
    
    id_cols = ['Meet', 'Event', 'Name', 'Club', 'Level', 'Prov', 'Age']
    score_cols = [col for col in master_df.columns if col not in id_cols]
    final_cols = id_cols + sorted(score_cols)
    master_df = master_df.reindex(columns=final_cols)
    
    print("\n--- All Events Scraped and Cleaned Successfully ---")
    if not master_df.empty:
        print(f"Meet: {master_df['Meet'].iloc[0]}")
        print("Total athletes found:", len(master_df))
    
    output_filename = 'Gymnastics_Meet_Results_FINAL.csv'
    master_df.to_csv(output_filename, index=False)
    
    print(f"\nSuccessfully saved all combined results to '{output_filename}'")
    print("\nFinal Data Preview:")
    print(master_df.head())
else:
    print("\nScraping process finished, but no data was retrieved.")