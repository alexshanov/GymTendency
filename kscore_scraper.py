import pandas as pd
import requests
import time
import os
import json
import io
import traceback
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURATION ---
KSCORE_MEETS_CSV = "discovered_meet_ids_kscore.csv"
OUTPUT_DIR_KSCORE = "CSVs_final_kscore"
DEBUG_LIMIT = 0

# ==============================================================================
#  THE PROVEN, WORKING HELPER FUNCTION
# ==============================================================================
def standardize_kscore_columns(html_content):
    """
    This function takes the raw HTML content of a Kscore results table
    and correctly constructs a pandas DataFrame with the proper headers.
    This is the definitive, correct logic.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # --- Step 1: Reliably extract header components from the <thead> ---
    header_rows = soup.select('thead > tr')
    if len(header_rows) < 2:
        print("Warning: Expected two header rows, but found fewer.")
        return pd.DataFrame()

    # Part A: Get event names from the <img> alt text in the first header row.
    event_names = [img.get('alt', 'Unknown') for img in header_rows[0].select('img.apparatuslogo')]
    
    # Part B: Get VISIBLE info column names from the second header row.
    sub_header_cells = header_rows[1].find_all(['th', 'td'])
    info_headers_raw = [
        cell.get_text(strip=True) for cell in sub_header_cells 
        if 'display: none;' not in cell.get('style', '') and cell.get_text(strip=True) not in ['D', 'Score', 'Rk']
    ]
    
    # --- Step 2: Build the final, correct header list ---
    final_columns = []
    
    # Standardize and add the info columns
    info_column_rename_map = {'Athlete': 'Name', '#': 'Rank'}
    final_columns.extend([info_column_rename_map.get(name, name) for name in info_headers_raw])
    
    # Add the event triples
    for event in event_names:
        clean_event = event.replace(' ', '_').replace('-', '_')
        final_columns.extend([f"Result_{clean_event}_D", f"Result_{clean_event}_Score", f"Result_{clean_event}_Rnk"])

    # --- Step 3: Extract VISIBLE data cells from the <tbody> ---
    data_rows = soup.select('tbody > tr')
    all_row_data = []
    for row in data_rows:
        cells = row.find_all('td')
        # This logic correctly takes only the visible cells
        row_data = [cell.get_text(strip=True) for cell in cells if 'display: none;' not in cell.get('style', '')]
        all_row_data.append(row_data)

    if not all_row_data:
        print("No data rows found.")
        return pd.DataFrame()

    # --- Step 4: Create the DataFrame and assign headers ---
    df = pd.DataFrame(all_row_data)

    # Final check: The number of constructed headers must match the number of visible data columns
    if len(final_columns) == df.shape[1]:
        df.columns = final_columns
        return df
    else:
        print(f"FATAL: Mismatch after parsing. Header count: {len(final_columns)}, Data column count: {df.shape[1]}")
        print("Constructed Header:", final_columns)
        return None

# ==============================================================================
#  MAIN SCRAPING FUNCTION
# ==============================================================================
def scrape_kscore_meet(meet_id, meet_name, output_dir):
    """
    Main function to scrape a single competition from the Kscore website.
    """
    raw_meet_id = meet_id.replace('kscore_', '')
    base_url = f"https://live.kscore.ca/results/{raw_meet_id}"
    
    print(f"--- Processing Kscore meet: {meet_name} ({meet_id}) ---")

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = None
    saved_files_count = 0
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        driver.get(base_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#sel-sess option:not([value=''])"))
        )
        
        user_agent = driver.execute_script("return navigator.userAgent;")
        headers = { 'User-Agent': user_agent, 'Referer': base_url, 'X-Requested-With': 'XMLHttpRequest' }
        cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        
        sessions = [{'id': el.get_attribute('value'), 'name': el.text} for el in driver.find_elements(By.CSS_SELECTOR, "#sel-sess option:not([value=''])")]
        print(f"Found {len(sessions)} sessions.")

        for session in sessions:
            print(f"  -- Processing session: {session['name']} (ID: {session['id']}) --")
            
            js_script = f"""
                var callback = arguments[0];
                $.ajax({{
                    url: 'src/query_scoring_groups.php',
                    data: 'sess=["{session['id']}"]',
                    dataType: 'json', type: 'GET',
                    success: function (resultArray) {{ callback(resultArray); }},
                    error: function() {{ callback(null); }}
                }});
            """
            categories_result = driver.execute_async_script(js_script)
            
            if not categories_result:
                print("    Could not retrieve categories for this session.")
                continue

            for cat_id, cat_info in enumerate(categories_result):
                if not cat_info: continue
                group_name = session['name']
                age_group = cat_info['name']
                print(f"    -> Scraping category: {age_group} (ID: {cat_id})")

                results_url = f"https://live.kscore.ca/results/{raw_meet_id}/src/query_custom_results.php"
                params = {
                    'event': 0, 'discip': cat_info.get('discip'),
                    'cat': json.dumps(cat_info.get('members')),
                    'sess': json.dumps(cat_info.get('mSess', cat_info.get('sess')))
                }
                
                response = requests.get(results_url, params=params, cookies=cookies, headers=headers)
                response.raise_for_status()
                
                html_content = response.text
                if not html_content or "There are no results" in html_content:
                    print("       No results table found in this category.")
                    continue

                df = standardize_kscore_columns(html_content)

                if df is None or df.empty:
                    print("       Failed to create a DataFrame from the table.")
                    continue
                
                # The first column from the raw HTML is the Gymnast ID. We don't need it.
                # Let's drop the 'Rank' column as it's the ID, not the placement rank.
                # The real ranks are in the Result_*_Rnk columns.
                if 'Rank' in df.columns:
                    df = df.drop(columns=['Rank'])
                
                df['Meet'] = meet_name
                df['Group'] = group_name
                df['Age_Group'] = age_group

                cols_to_move = ['Name', 'Club', 'Meet', 'Group', 'Age_Group']
                existing_info_cols = [col for col in cols_to_move if col in df.columns]
                result_cols = [col for col in df.columns if col not in existing_info_cols]
                df = df[existing_info_cols + result_cols]
                
                output_filename = f"{meet_id}_FINAL_{session['id']}_{cat_id}.csv"
                output_path = os.path.join(output_dir, output_filename)
                df.to_csv(output_path, index=False)
                saved_files_count += 1
                
        return saved_files_count

    except Exception as e:
        print(f"A critical error occurred while processing {meet_id}: {e}")
        traceback.print_exc()
        return saved_files_count
    finally:
        if driver:
            driver.quit()

# ==============================================================================
#  MAIN EXECUTION BLOCK
# ==============================================================================
def main():
    """
    The main execution function for the Kscore scraper script.
    """
    print("--- Kscore Meet Scraper Initializing ---")
    os.makedirs(OUTPUT_DIR_KSCORE, exist_ok=True)
    print(f"Output directory '{OUTPUT_DIR_KSCORE}' is ready.")

    try:
        meets_df = pd.read_csv(KSCORE_MEETS_CSV)
        if 'MeetID' not in meets_df.columns or 'MeetName' not in meets_df.columns:
            print(f"FATAL ERROR: The file '{KSCORE_MEETS_CSV}' is missing 'MeetID' or 'MeetName' columns.")
            return
        print(f"Found {len(meets_df)} meets to process from '{KSCORE_MEETS_CSV}'.")
    except FileNotFoundError:
        print(f"FATAL ERROR: Input file '{KSCORE_MEETS_CSV}' not found.")
        return
    except Exception as e:
        print(f"FATAL ERROR: Could not read '{KSCORE_MEETS_CSV}'. Details: {e}")
        return

    if DEBUG_LIMIT > 0:
        meets_df = meets_df.head(DEBUG_LIMIT)
        print(f"--- DEBUG MODE: Processing a maximum of {len(meets_df)} meet(s). ---")
    
    total_files_created = 0
    total_meets_processed = 0
    total_meets_in_queue = len(meets_df)

    for index, row in meets_df.iterrows():
        total_meets_processed += 1
        meet_id = str(row['MeetID'])
        meet_name = str(row['MeetName'])

        print("\n" + "="*70)
        print(f"Processing Meet {total_meets_processed}/{total_meets_in_queue}: {meet_name} (ID: {meet_id})")
        print("="*70)

        files_count = scrape_kscore_meet(
            meet_id=meet_id,
            meet_name=meet_name,
            output_dir=OUTPUT_DIR_KSCORE
        )

        if files_count > 0:
            print(f"--- ✅ SUCCESS: Saved {files_count} files for '{meet_name}' ---")
            total_files_created += files_count
        else:
            print(f"--- ⚠️  NOTE: No result files were saved for '{meet_name}'. ---")

        if total_meets_processed < total_meets_in_queue:
             print("Pausing for 2 seconds before the next meet...")
             time.sleep(2)

    print("\n" + "="*70)
    print("--- SCRIPT EXECUTION FINISHED ---")
    print(f"Total meets processed: {total_meets_processed}")
    print(f"Total CSV files created: {total_files_created}")
    print(f"All files are located in the '{OUTPUT_DIR_KSCORE}' directory.")
    print("="*70)


if __name__ == "__main__":
    main()