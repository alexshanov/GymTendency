import pandas as pd
import io
import json
from bs4 import BeautifulSoup
import re
import html
import os
import time

import numpy as np


# --- Selenium Imports ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, UnexpectedAlertPresentException

# ==============================================================================
#  LIBRARY OF FUNCTIONS (The "Tools")
# ==============================================================================

def scrape_raw_data_to_separate_files(main_page_url, meet_id_for_filename, output_directory="raw_data"):
    """
    Scrapes all event data, saving each table into its own CSV file.
    --- CORRECTED to accept the Meet ID as an argument for reliable file naming ---
    Returns (file_count, meet_id) on success.
    """
    print(f"--- STEP 1: Scraping Raw Data for {main_page_url} ---")
    
    os.makedirs(output_directory, exist_ok=True)
    
    if not meet_id_for_filename:
        print("--> FATAL ERROR: A valid Meet ID was not provided to the scraper function.")
        return 0, None

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    driver = None
    table_counter = 1
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        driver.get(main_page_url)
        
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "liCategory")))
            
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            meet_name_element = soup.select_one("div#thHeader.TournamentHeader div div.TournamentHeading")
            meet_name = meet_name_element.get_text(strip=True) if meet_name_element else "Unknown Meet"
            
            session_id_match = re.search(r'SessionId=([a-zA-Z0-9]+)', html_content)
            if not session_id_match:
                print("--> ERROR: Could not find SessionId needed for data requests.")
                return 0, None
            active_session_id = session_id_match.group(1)
            
            event_elements = soup.find_all('li', class_='liCategory')
            events_to_scrape = {el.get_text(strip=True): el.get('id') for el in event_elements if el.get_text(strip=True) and el.get('id')}
            
            base_data_url = "https://www.sportzsoft.com/meet/meetWeb.dll/TournamentResults"
            
            print(f"Found {len(events_to_scrape)} event groups for meet '{meet_name}'. Processing...")
            
            for group_name, division_id in events_to_scrape.items():
                try:
                    # (The inner logic for scraping each table is unchanged)
                    data_url = f"{base_data_url}?DivId={division_id}&SessionId={active_session_id}"
                    driver.get(data_url)
                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
                    json_text = driver.find_element(By.TAG_NAME, 'pre').text
                    data = json.loads(json_text)
                    decoded_html = html.unescape(data['html'])
                    results_soup = BeautifulSoup(decoded_html, 'html.parser')
                    table_wrappers = results_soup.find_all('div', class_='resultsTableWrapper')

                    for wrapper in table_wrappers:
                        age_group = "N/A"
                        title_element = wrapper.select_one(".resultsTitle .rpSubTitle")
                        if title_element and 'Age Group:' in title_element.get_text():
                            age_group_text = title_element.get_text(strip=True)
                            age_group = age_group_text.replace('(Age Group:', '').replace(')', '').strip()

                        table_element = wrapper.find('table', id='sessionEventResults')
                        if table_element:
                            df_list = pd.read_html(io.StringIO(str(table_element)))
                            if df_list:
                                df = df_list[0].copy()
                                df['Group'] = group_name
                                df['Meet'] = meet_name
                                df['Age Group'] = age_group
                                
                                filename = f"{meet_id_for_filename}_MESSY_{table_counter}.csv"
                                full_path = os.path.join(output_directory, filename)
                                
                                df.to_csv(full_path, index=False)
                                print(f"  -> Saved table {table_counter} to '{full_path}'")
                                table_counter += 1
                except Exception as e:
                    print(f"  -> Skipping a section in '{group_name}' due to an error: {e}")
                    continue
            
            if table_counter > 1:
                files_saved_count = table_counter - 1
                print(f"\n--> Success! Saved a total of {files_saved_count} tables for '{meet_name}' to the '{output_directory}' directory.")
                return files_saved_count, meet_id_for_filename
            else:
                print(f"--> No data tables were found or saved for {main_page_url}.")
                return 0, None

        except (TimeoutException, UnexpectedAlertPresentException) as e:
            print(f"--> SKIPPING MEET: The page at {main_page_url} failed to load correctly. Error: {e}")
            return 0, None
            
    finally:
        if driver:
            driver.quit()

def fix_and_standardize_headers(input_filename, output_filename):
    """
    Reads a raw/messy CSV, builds a single perfect header by combining the two
    header rows, cleans the data, and saves the final file.
    --- FINAL VERSION: Forcibly renames the last 3 columns to guarantee consistency. ---
    """
    print(f"--- Processing and finalizing '{input_filename}' ---")

    try:
        df = pd.read_csv(input_filename, header=None, dtype=str, keep_default_na=False)
    except (FileNotFoundError, pd.errors.EmptyDataError) as e:
        print(f"Error: Could not read '{input_filename}'. It may be missing or empty. Details: {e}")
        return False

    if df.empty:
        print(f"Warning: Input file is empty. Nothing to process.")
        return True
        
    # --- Step 1: Discard the first column ---
    df = df.iloc[:, 1:].copy()

    # --- Step 2: Find the main header row (using 'Name' as the anchor) ---
    header_row_index = -1
    for i, row in df.iterrows():
        if 'Name' in row.values:
            header_row_index = i
            break
            
    if header_row_index == -1:
        print(f"Error: Could not find the main header row (containing 'Name') in '{input_filename}'.")
        return False

    # --- Step 3: Isolate and clean the TRUE data rows ---
    data_df = df.iloc[:header_row_index].copy()
    data_df = data_df[~data_df.iloc[:, 0].str.contains('Unnamed', na=False)].copy()

    if data_df.empty:
        print(f"Warning: No valid data rows found in '{input_filename}'. Skipping.")
        return True

    # --- Step 4: Build the single, standardized header ---
    main_header_row = df.iloc[header_row_index]
    sub_header_row = df.iloc[header_row_index + 1]
    main_header = pd.Series(main_header_row).ffill()
    sub_header = pd.Series(sub_header_row)
    clean_header = []
    j = 0
    while j < len(main_header):
        is_event_block = False
        if j + 2 < len(main_header):
            h1 = str(main_header.iloc[j]).strip()
            if h1 and h1 == str(main_header.iloc[j+1]).strip() and h1 == str(main_header.iloc[j+2]).strip():
                if str(sub_header.iloc[j+1]).strip() == 'Score' and str(sub_header.iloc[j+2]).strip() == 'Rnk':
                    event_name = h1.replace(' ', '_')
                    clean_header.extend([f"Result_{event_name}_D", f"Result_{event_name}_Score", f"Result_{event_name}_Rnk"])
                    is_event_block = True
                    j += 3
        if not is_event_block:
            name_from_main = str(main_header.iloc[j]).strip()
            name_from_sub = str(sub_header.iloc[j]).strip()
            final_name = name_from_main if name_from_main and 'Unnamed' not in name_from_main else name_from_sub
            clean_header.append(final_name.replace(' ', '_'))
            j += 1

    # --- Step 5: Apply the new header and Save ---
    if len(clean_header) != data_df.shape[1]:
        print(f"Error: Final header length ({len(clean_header)}) doesn't match data columns ({data_df.shape[1]}).")
        return False

    data_df.columns = clean_header
    
    # <<< THIS IS THE NEW LINE YOU REQUESTED >>>
    # Forcibly rename the last three columns to ensure they are always correct.
    data_df.columns.values[-3:] = ['Group', 'Meet', 'Age_Group']

    # Reorder for final readability (this now works reliably)
    cols_to_move = ['Name', 'Club', 'Level', 'Prov', 'Age', 'Meet', 'Group', 'Age_Group']
    existing_cols = [col for col in cols_to_move if col in data_df.columns]
    other_cols = [col for col in data_df.columns if col not in existing_cols]
    final_df = data_df[existing_cols + other_cols]

    final_df.to_csv(output_filename, index=False)
    print(f"-> Success! Final clean file saved to '{output_filename}'")
    return True

# ==============================================================================
#  MAIN EXECUTION BLOCK (The "Application")
# ==============================================================================

if __name__ == "__main__":
    
    # --- CONFIGURATION ---
    MEET_IDS_CSV = "discovered_meet_ids.csv"
    MESSY_FOLDER = "CSVs_messy"
    FINAL_FOLDER = "CSVs_final" # The destination for clean, finished files
    BASE_URL = "https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id="
    
    DEBUG_LIMIT = 0

    # --- SETUP ---
    os.makedirs(MESSY_FOLDER, exist_ok=True)
    os.makedirs(FINAL_FOLDER, exist_ok=True)
    print(f"Ensured output directories exist: '{MESSY_FOLDER}' and '{FINAL_FOLDER}'")

    try:
        meet_ids_df = pd.read_csv(MEET_IDS_CSV)
        meet_id_column_name = [col for col in meet_ids_df.columns if 'MeetID' in col][0]
        meet_ids_to_process = meet_ids_df[meet_id_column_name].tolist()
        print(f"Found {len(meet_ids_to_process)} meet IDs to process from '{MEET_IDS_CSV}'")
    except (FileNotFoundError, IndexError) as e:
        print(f"FATAL ERROR: Could not read '{MEET_IDS_CSV}'. Please create it first. Details: {e}")
        exit()

    if DEBUG_LIMIT and DEBUG_LIMIT > 0:
        print(f"--- DEBUG MODE ON: Processing only the first {DEBUG_LIMIT} meet(s). ---")
        meet_ids_to_process = meet_ids_to_process[:DEBUG_LIMIT]

    # --- EXECUTION PIPELINE ---
    for meet_id in meet_ids_to_process:
        print(f"\n{'='*20} PROCESSING MEET ID: {meet_id} {'='*20}")
        
        meet_url = f"{BASE_URL}{meet_id}"
        
        # --- STEP 1: Scrape messy files ---
        files_saved, file_base_id = scrape_raw_data_to_separate_files(meet_url, meet_id, MESSY_FOLDER)
        
        if files_saved > 0:
            print(f"Scraping complete. Found {files_saved} tables for Meet ID {file_base_id}.")
            print("--- Starting Step 2: Finalizing Files ---")
            
            all_successful = True
            for i in range(1, files_saved + 1):
                messy_file_path = os.path.join(MESSY_FOLDER, f"{file_base_id}_MESSY_{i}.csv")
                final_file_path = os.path.join(FINAL_FOLDER, f"{file_base_id}_FINAL_{i}.csv")

                # --- STEP 2: Call the all-in-one fixer function ---
                if not fix_and_standardize_headers(messy_file_path, final_file_path):
                    print(f"--- ❌ FAILED at Step 2 (Finalizing) for: {messy_file_path} ---")
                    all_successful = False
                    break 

            if all_successful:
                print(f"--- ✅ Successfully processed all {files_saved} tables for Meet ID: {meet_id} ---")
        else:
            print(f"--- ❌ FAILED or SKIPPED at Step 1 (Scraping) for Meet ID: {meet_id} ---")
        
        time.sleep(3)

    print("\n--- ALL MEETS PROCESSED ---")