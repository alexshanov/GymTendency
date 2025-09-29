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


def scrape_raw_data(main_page_url, output_filename):
    """
    Scrapes all event data for a single meet into one raw, messy CSV file.
    Now handles both Timeouts and Unexpected Alerts gracefully.
    
    --- UPDATED to capture multiple tables (e.g., age groups) on a single page. ---
    """
    print(f"--- STEP 1: Scraping Raw Data for {main_page_url} ---")
    
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
        driver.get(main_page_url)
        
        # --- THIS IS THE ROBUST ERROR HANDLING BLOCK ---
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "liCategory")))
            
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            meet_name_element = soup.select_one("div#thHeader.TournamentHeader div div.TournamentHeading")
            meet_name = meet_name_element.get_text(strip=True) if meet_name_element else "Unknown Meet"
            
            session_id_match = re.search(r'SessionId=([a-zA-Z0-9]+)', html_content)
            active_session_id = session_id_match.group(1)
            
            event_elements = soup.find_all('li', class_='liCategory')
            events_to_scrape = {el.get_text(strip=True): el.get('id') for el in event_elements if el.get_text(strip=True) and el.get('id')}
            
            base_data_url = "https://www.sportzsoft.com/meet/meetWeb.dll/TournamentResults"
            
            for group_name, division_id in events_to_scrape.items():
                try:
                    data_url = f"{base_data_url}?DivId={division_id}&SessionId={active_session_id}"
                    driver.get(data_url)

                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
                    json_text = driver.find_element(By.TAG_NAME, 'pre').text
                    data = json.loads(json_text)
                    decoded_html = html.unescape(data['html'])
                    
                    # =================================================================
                    # START: MODIFIED BLOCK TO CAPTURE ALL TABLES
                    # =================================================================
                    
                    # Parse the decoded HTML with BeautifulSoup to find each table's container
                    results_soup = BeautifulSoup(decoded_html, 'html.parser')

                    # Find all the wrapper divs, each containing one results table and its title
                    table_wrappers = results_soup.find_all('div', class_='resultsTableWrapper')

                    # Loop through each wrapper to process the table and its title individually
                    for wrapper in table_wrappers:
                        # --- Extract the Age Group for this specific table ---
                        age_group = "N/A"  # Set a default value
                        title_element = wrapper.select_one(".resultsTitle .rpSubTitle")
                        if title_element and 'Age Group:' in title_element.get_text():
                            age_group_text = title_element.get_text(strip=True)
                            age_group = age_group_text.replace('(Age Group:', '').replace(')', '').strip()

                        # --- Read the HTML table within this wrapper into a DataFrame ---
                        table_element = wrapper.find('table', id='sessionEventResults')
                        
                        if table_element:
                            # pd.read_html returns a list, so we take the first item
                            df_list = pd.read_html(io.StringIO(str(table_element)))
                            if df_list:
                                df = df_list[0]
                                
                                # --- Add all the metadata to the DataFrame ---
                                df['Group'] = group_name      # The main category, e.g., "Level 6"
                                df['Meet'] = meet_name        # The name of the meet
                                df['Age Group'] = age_group   # The specific age group for this table
                                
                                # Append the processed DataFrame to your master list
                                all_raw_dfs.append(df)

                    # =================================================================
                    # END: MODIFIED BLOCK
                    # =================================================================

                except Exception:
                    # If fetching or processing a single division/group fails, just skip it and continue
                    continue
            
            if all_raw_dfs:
                messy_df = pd.concat(all_raw_dfs, ignore_index=True)
                messy_df.to_csv(output_filename, index=False)
                print(f"--> Success! Raw data for '{meet_name}' saved to '{output_filename}'")
                return True
            
            print(f"--> No dataframes were created for {main_page_url}. Check the structure.")
            return False

        except TimeoutException:
            print(f"--> SKIPPING MEET (Timeout): The page at {main_page_url} does not have the expected structure or failed to load.")
            return False
            
        except UnexpectedAlertPresentException as e:
            alert_text = e.alert_text
            print(f"--> SKIPPING MEET (Unexpected Alert): The page produced a server-side error.")
            print(f"    Alert Text: {alert_text}")
            return False
            
    finally:
        if driver:
            driver.quit()
            
def fix_csv_headers(input_filename, output_filename):
    """
    Reads a CSV with repeated double headers and merges them using a robust,
    pattern-based approach.

    The logic is:
    1. Find any group of 3 consecutive columns with the same main header.
    2. If the 2nd sub-header is 'Score' and the 3rd is 'Rnk',
    3. Then the 1st sub-header is forced to be 'D'.
    4. All other columns are treated as single-name columns.
    """
    print(f"--- Starting header fixing for '{input_filename}' ---")

    try:
        df = pd.read_csv(input_filename, header=None)
    except FileNotFoundError:
        print(f"Error: The input file '{input_filename}' was not found.")
        return False

    rows_to_drop = []
    
    for i in range(len(df) - 1):
        current_row = df.iloc[i].astype(str).values
        next_row = df.iloc[i + 1].astype(str).values

        if 'Name' in current_row:
            main_header = pd.Series(current_row).ffill()
            sub_header = pd.Series(next_row)
            
            new_header = []
            j = 0
            while j < len(main_header):
                # --- THIS IS THE NEW, ROBUST LOGIC YOU REQUESTED ---
                
                # Check if a 3-column pattern is possible from the current position
                if j + 2 < len(main_header):
                    h1_base = str(main_header[j]).strip().replace(' ', '_')
                    h1_next1 = str(main_header[j+1]).strip().replace(' ', '_')
                    h1_next2 = str(main_header[j+2]).strip().replace(' ', '_')

                    h2_next1 = str(sub_header[j+1]).strip()
                    h2_next2 = str(sub_header[j+2]).strip()

                    # Apply the rule: 3 same headers, with 'Score' and 'Rnk' in positions 2 and 3
                    if (h1_base == h1_next1 and h1_base == h1_next2 and
                        h2_next1 == 'Score' and h2_next2 == 'Rnk'):
                        
                        # If the pattern matches, create the three corrected column names
                        new_header.append(f"{h1_base}_D")
                        new_header.append(f"{h1_base}_Score")
                        new_header.append(f"{h1_base}_Rnk")
                        
                        # Advance the loop counter by 3 to skip past this processed group
                        j += 3
                        continue # Move to the next iteration of the while loop

                # --- Fallback for all other columns ---
                # If the 3-column pattern is not found, process this as a single column
                h1_clean = str(main_header[j]).strip().replace(' ', '_')
                new_header.append(h1_clean)
                j += 1 # Advance the loop counter by 1
            
            df.iloc[i] = new_header
            rows_to_drop.append(i + 1)

    df_cleaned = df.drop(rows_to_drop).reset_index(drop=True)
    
    df_cleaned.to_csv(output_filename, index=False, header=False)
    
    print("\n--- Header Fixing Complete ---")
    print(f"Processed and standardized {len(rows_to_drop)} header pairs.")
    print(f"Output saved to '{output_filename}'")
    return True

def unify_and_clean_data(input_filename, output_filename):
    """
    Reads a file with cleaned-but-repeated headers, identifies the true header,
    selects only the athlete data rows, and builds a final clean CSV.
    This version uses a robust method to prevent blank row issues.
    """
    print(f"--- Starting final cleaning and unification for '{input_filename}' ---")

    try:
        # Read the entire file as raw data with no header
        df = pd.read_csv(input_filename, header=None, dtype=str)
    except FileNotFoundError:
        print(f"Error: The input file '{input_filename}' was not found.")
        return False

    # 1. Identify and extract the master header row
    header_rows = df[df[0].astype(str).str.strip() == '#']
    if header_rows.empty:
        print("Error: Could not find any header rows (containing '#' in the first column).")
        return False
        
    master_header = header_rows.iloc[0].tolist()
    print(f"Master header identified with {len(master_header)} columns.")

    # 2. Select ONLY the data rows (where the first column is NOT '#')
    data_df = df[df[0].astype(str).str.strip() != '#'].copy()
    
    # 3. Assign the correct headers to the data
    # This is the most critical step. We align the master header to the data rows.
    # If data rows have fewer columns, we only use the first part of the header.
    num_data_cols = data_df.shape[1]
    data_df.columns = master_header[:num_data_cols]
    
    # 4. Filter out any remaining junk rows by keeping only rows with a valid Age
    # This also removes any all-blank rows that might exist
    clean_df = data_df[pd.to_numeric(data_df['Age'], errors='coerce').notna()]

    # 5. Final Polishing
    if '#' in clean_df.columns:
        clean_df = clean_df.drop(columns=['#'])
    
    clean_df = clean_df.reset_index(drop=True)
    
    clean_df.to_csv(output_filename, index=False)
    
    print("\n--- Final Cleaning Complete ---")
    if not clean_df.empty:
        print(f"Processed {len(clean_df)} athlete data rows.")
        print(f"Final clean data saved to '{output_filename}'")
        print("\nFinal Data Preview:")
        print(clean_df.head())
        return True
    else:
        print("Warning: No valid athlete data was found after cleaning.")
        return False
  
# ==============================================================================
#  MAIN EXECUTION BLOCK (The "Application")
# ==============================================================================

if __name__ == "__main__":
    
    # --- CONFIGURATION ---
    MEET_IDS_CSV = "discovered_meet_ids.csv"
    OUTPUT_SUBFOLDER = "CSVs"
    BASE_URL = "https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id="
    
    # --- THIS IS THE NEW DEBUG LINE ---
    # Set this to a number (e.g., 3) to process only the first N meets.
    # Set to 0 or None to process all meets in the CSV.
    DEBUG_LIMIT = 13
    # --- END OF NEW DEBUG LINE ---

    # Create the output subfolder if it doesn't exist
    if not os.path.exists(OUTPUT_SUBFOLDER):
        os.makedirs(OUTPUT_SUBFOLDER)
        print(f"Created subfolder: '{OUTPUT_SUBFOLDER}'")

    # Read the list of Meet IDs from the CSV
    try:
        meet_ids_df = pd.read_csv(MEET_IDS_CSV)
        meet_id_column_name = [col for col in meet_ids_df.columns if 'MeetID' in col][0]
        meet_ids_to_process = meet_ids_df[meet_id_column_name].tolist()
        print(f"Found {len(meet_ids_to_process)} meet IDs to process from '{MEET_IDS_CSV}'")
    except (FileNotFoundError, IndexError) as e:
        print(f"FATAL ERROR: Could not read '{MEET_IDS_CSV}'. Please create it first. Details: {e}")
        exit()

    # Apply the debug limit if it's set
    if DEBUG_LIMIT and DEBUG_LIMIT > 0:
        print(f"--- DEBUG MODE ON: Processing only the first {DEBUG_LIMIT} meet(s). ---")
        meet_ids_to_process = meet_ids_to_process[:DEBUG_LIMIT]

    # --- EXECUTION PIPELINE ---
    for meet_id in meet_ids_to_process:
        print(f"\n{'='*20} PROCESSING MEET ID: {meet_id} {'='*20}")
        
        meet_url = f"{BASE_URL}{meet_id}"
        messy_output = os.path.join(OUTPUT_SUBFOLDER, f"{meet_id}_messy.csv")
        headers_fixed_output = os.path.join(OUTPUT_SUBFOLDER, f"{meet_id}_headers_fixed.csv")
        final_output = os.path.join(OUTPUT_SUBFOLDER, f"{meet_id}_FINAL.csv")
        
        # Run the 3-step pipeline
        if scrape_raw_data(meet_url, messy_output):
            if fix_csv_headers(messy_output, headers_fixed_output):
                if unify_and_clean_data(headers_fixed_output, final_output):
                    print(f"--- ✅ Successfully processed Meet ID: {meet_id} ---")
                    try:
                        print("Cleaning up intermediate files...")
                        #os.remove(messy_output)
                        #os.remove(headers_fixed_output)
                        print("Cleanup complete.")
                    except OSError as e:
                        print(f"Warning: Could not remove intermediate files. Reason: {e}")
                else:
                    print(f"--- ❌ FAILED at Step 3 (Unifying) for Meet ID: {meet_id} ---")
            else:
                print(f"--- ❌ FAILED at Step 2 (Header Fixing) for Meet ID: {meet_id} ---")
        else:
            print(f"--- ❌ FAILED or SKIPPED at Step 1 (Scraping) for Meet ID: {meet_id} ---")
        
        time.sleep(3)

    print("\n--- ALL MEETS PROCESSED ---")