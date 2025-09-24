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

def scrape_full_meet(main_page_url):
    """
    Handles the entire scraping process.
    Correctly handles HTML-encoded JSON API responses and pandas logic.
    """
    print("--- Initializing Selenium Browser ---")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless') # Re-enabling headless mode as it's more convenient
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    driver = None
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # --- PHASE 1: DISCOVERY ---
        print("--- Phase 1: Discovering all event IDs and the active SessionId ---")
        driver.get(main_page_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "liCategory")))
        print("Event list loaded.")

        html_content = driver.page_source
        session_id_match = re.search(r'SessionId=([a-zA-Z0-9]+)', html_content)
        if not session_id_match:
            print("CRITICAL ERROR: Could not find the active SessionId. Aborting.")
            return None
        active_session_id = session_id_match.group(1)
        print(f"Successfully discovered active SessionId: {active_session_id}")

        soup = BeautifulSoup(html_content, 'html.parser')
        event_elements = soup.find_all('li', class_='liCategory')
        events_to_scrape = {el.get_text(strip=True): el.get('id') for el in event_elements if el.get_text(strip=True) and el.get('id')}
        print(f"Discovery complete. Found {len(events_to_scrape)} events.")

        # --- PHASE 2: SCRAPING ---
        all_results = []
        base_data_url = "https://www.sportzsoft.com/meet/meetWeb.dll/TournamentResults"
        print("\n--- Phase 2: Scraping data for each event ---")
        
        for event_name, division_id in events_to_scrape.items():
            try:
                data_url = f"{base_data_url}?DivId={division_id}&SessionId={active_session_id}"
                print(f"Navigating to: {event_name}")
                driver.get(data_url)

                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
                json_text = driver.find_element(By.TAG_NAME, 'pre').text
                data = json.loads(json_text)
                html_from_json = data['html']
                decoded_html = html.unescape(html_from_json)
                
                df = pd.read_html(io.StringIO(decoded_html), 
                                  attrs={'id': 'sessionEventResults'}, 
                                  header=[0, 1])[0]
                
                # Data Cleaning
                new_columns = [f"{c1}_{c2}" if 'Unnamed' not in str(c1) and 'Unnamed' not in str(c2) else c1 for c1, c2 in df.columns]
                df.columns = new_columns
                
                # --- THIS IS THE CORRECTED LINE ---
                if not df.columns.empty and df.columns[0].strip().lower() == 'pl.':
                    df = df.drop(columns=df.columns[0])
                
                df.dropna(how='all', inplace=True)
                df['Event'] = event_name
                
                if not df.empty:
                    all_results.append(df)
                    print(f"--> Success! Scraped {len(df)} rows.")

            except Exception as e:
                print(f"--> Warning: Could not process data for {event_name}. Reason: {e}")
        
        return all_results

    except Exception as e:
        print(f"An unrecoverable error occurred: {e}")
        return None
    finally:
        print("\n--- Finalizing: Closing browser ---")
        if driver:
            driver.quit()

# --- Main script execution ---
MAIN_PAGE_URL = "https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id=C5432FCE37715FF3C29F88080A34FDD6"
final_data = scrape_full_meet(MAIN_PAGE_URL)

if final_data:
    master_df = pd.concat(final_data, ignore_index=True)
    print("\n--- All Events Scraped Successfully ---")
    print("Total athletes found:", len(master_df))
    output_filename = 'Gymnastics_Meet_Results_SUCCESS.csv'
    master_df.to_csv(output_filename, index=False)
    print(f"\nSuccessfully saved all combined results to '{output_filename}'")
else:
    print("\nScraping process finished, but no data was retrieved.")