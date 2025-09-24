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

def scrape_raw_data(main_page_url):
    """
    Scrapes all event data into a single, raw, messy CSV file.
    Includes the Meet Name in the raw data.
    """
    print("--- Initializing Selenium Browser ---")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    driver = None
    all_raw_dfs = []
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # --- PHASE 1: DISCOVERY ---
        print("--- Phase 1: Discovering Meet Name, SessionId, and Events ---")
        driver.get(main_page_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "liCategory")))
        
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Discover Meet Name
        meet_name_element = soup.select_one("div#thHeader.TournamentHeader div div.TournamentHeading")
        meet_name = meet_name_element.get_text(strip=True) if meet_name_element else "Unknown Meet"
        print(f"Discovered Meet Name: {meet_name}")

        session_id_match = re.search(r'SessionId=([a-zA-Z0-9]+)', html_content)
        active_session_id = session_id_match.group(1)
        
        event_elements = soup.find_all('li', class_='liCategory')
        events_to_scrape = {el.get_text(strip=True): el.get('id') for el in event_elements if el.get_text(strip=True) and el.get('id')}
        print(f"Found {len(events_to_scrape)} events.")

        # --- PHASE 2: SCRAPING ---
        base_data_url = "https://www.sportzsoft.com/meet/meetWeb.dll/TournamentResults"
        print("\n--- Phase 2: Scraping raw data for each event ---")
        
        for event_name, division_id in events_to_scrape.items():
            try:
                data_url = f"{base_data_url}?DivId={division_id}&SessionId={active_session_id}"
                print(f"Processing: {event_name}")
                driver.get(data_url)

                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
                json_text = driver.find_element(By.TAG_NAME, 'pre').text
                data = json.loads(json_text)
                decoded_html = html.unescape(data['html'])
                
                df_list = pd.read_html(io.StringIO(decoded_html), attrs={'id': 'sessionEventResults'})
                if df_list:
                    df = df_list[0]
                    # Add metadata to the raw data before appending
                    df['Event'] = event_name
                    df['Meet'] = meet_name
                    all_raw_dfs.append(df)
                    print(f"--> Success! Scraped {event_name}")

            except Exception as e:
                print(f"--> Warning: Could not process {event_name}. Reason: {e}")
        
        return all_raw_dfs

    finally:
        print("\n--- Finalizing: Closing browser ---")
        if driver:
            driver.quit()

# --- Main script execution ---
MAIN_PAGE_URL = "https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id=C5432FCE37715FF3C29F88080A34FDD6"
list_of_raw_dataframes = scrape_raw_data(MAIN_PAGE_URL)

if list_of_raw_dataframes:
    messy_df = pd.concat(list_of_raw_dataframes)
    output_filename = 'Gymnastics_Meet_Results_MESSY.csv'
    messy_df.to_csv(output_filename, index=False)
    print(f"\n--- Scraping complete. All raw data saved to '{output_filename}' ---")
else:
    print("\nScraping process finished, but no data was retrieved.")