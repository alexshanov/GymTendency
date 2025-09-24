import pandas as pd
import io
import json
from bs4 import BeautifulSoup
import re
import html

import numpy as np


# --- Selenium Imports ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ==============================================================================
#  LIBRARY OF FUNCTIONS (The "Tool")
# ==============================================================================

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

def fix_csv_headers(input_filename, output_filename):
    """
    Reads a CSV with repeated double headers, merges each pair into a single,
    standardized header row, and removes the redundant second row.
    - Standardizes 'SV' and 'D' to '_D'.
    - Replaces all spaces with underscores.
    """
    print(f"--- Starting header fixing for '{input_filename}' ---")

    try:
        df = pd.read_csv(input_filename, header=None)
    except FileNotFoundError:
        print(f"Error: The input file '{input_filename}' was not found.")
        return

    rows_to_drop = []
    
    for i in range(len(df) - 1):
        current_row = df.iloc[i].astype(str).values
        next_row = df.iloc[i + 1].astype(str).values

        if 'Name' in current_row:
            main_header = pd.Series(current_row).ffill()
            sub_header = pd.Series(next_row)
            
            new_header = []
            for h1, h2 in zip(main_header, sub_header):
                h1_clean = str(h1).strip().replace(' ', '_')
                h2_clean = str(h2).strip()
                
                # Standardize 'SV' and 'D' to '_D'
                if h2_clean in ['SV', 'D']:
                    h2_clean = 'D'
                
                # Combine headers
                if h2_clean in ['D', 'Score', 'Rnk'] and 'nan' not in h1_clean and 'Provincial' not in h1_clean:
                    new_header.append(f"{h1_clean}_{h2_clean}")
                else:
                    new_header.append(h1_clean)
            
            df.iloc[i] = new_header
            rows_to_drop.append(i + 1)

    df_cleaned = df.drop(rows_to_drop).reset_index(drop=True)
    
    df_cleaned.to_csv(output_filename, index=False, header=False)
    
    print("\n--- Header Fixing Complete ---")
    print(f"Processed and standardized {len(rows_to_drop)} header pairs.")
    print(f"Output saved to '{output_filename}'")

def unify_and_clean_data(input_filename, output_filename):
    """
    Reads a file with cleaned-but-repeated headers, verifies their uniformity
    (ignoring the last two columns), and produces a final clean CSV.
    """
    print(f"--- Starting final cleaning and unification for '{input_filename}' ---")

    try:
        df = pd.read_csv(input_filename, header=None, dtype=str)
    except FileNotFoundError:
        print(f"Error: The input file '{input_filename}' was not found.")
        return

    # 1. Identify all header rows and the master header
    header_rows = df[df[0].astype(str).str.strip() == '#']
    if header_rows.empty:
        print("Error: Could not find any header rows (containing '#' in the first column).")
        return
        
    master_header = header_rows.iloc[0].tolist()
    print(f"Master header identified with {len(master_header)} columns.")

    # 2. Verify all other header rows are identical (with your requested slicing)
    all_headers_match = True
    
    # --- THIS IS YOUR MODIFIED LOGIC ---
    # We will compare all columns EXCEPT the last two.
    num_cols_to_compare = len(master_header) - 2
    master_header_slice = master_header[:num_cols_to_compare]
    print(f"Verifying the first {num_cols_to_compare} columns of all headers...")
    # --- END MODIFICATION ---

    for index, row in header_rows.iloc[1:].iterrows():
        # Slice the current row's header to match the master slice
        current_header_slice = row.tolist()[:num_cols_to_compare]
        
        if current_header_slice != master_header_slice:
            print(f"Warning: Header mismatch found at row {index}. This may cause issues.")
            all_headers_match = False
            # For debugging:
            # print("Master Slice:", master_header_slice)
            # print("Current Slice:", current_header_slice)

    if all_headers_match:
        print("Verification complete: All core headers are identical.")

    # 3. Clean and Combine
    data_rows = df[df[0].astype(str).str.strip() != '#']
    
    clean_df = pd.DataFrame(data_rows.values)
    
    num_data_cols = clean_df.shape[1]
    clean_df.columns = master_header[:num_data_cols]

    clean_df = clean_df[pd.to_numeric(clean_df.get('Age'), errors='coerce').notna()]
    
    if '#' in clean_df.columns:
        clean_df = clean_df.drop(columns=['#'])
    
    clean_df = clean_df.reset_index(drop=True)
    
    clean_df.to_csv(output_filename, index=False)
    
    print("\n--- Final Cleaning Complete ---")
    print(f"Processed {len(clean_df)} athlete data rows.")
    print(f"Final clean data saved to '{output_filename}'")
    print("\nFinal Data Preview:")
    print(clean_df.head())

# ==============================================================================
#  MAIN EXECUTION BLOCK (The "Application")
# ==============================================================================
# --- Main script execution for scraping ---
