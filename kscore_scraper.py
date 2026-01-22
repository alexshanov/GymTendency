import pandas as pd
import time
import os
import json
import io
import traceback
import glob
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURATION ---
KSCORE_MEETS_CSV = "discovered_meet_ids_kscore.csv"
OUTPUT_DIR_KSCORE = "CSVs_kscore_final"
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
    
    # Just take the raw headers. No renaming.
    # User Request: "Scrape as is"
    final_columns.extend(info_headers_raw)
    
    # Add the event triples (D/Score/Rnk)
    # K-Score structure is consistent, so we can infer the structure but keep names close to raw if possible.
    # However, the events come from alt tags.
    # We will keep the 'Result_Event_Type' structure for the triples as it's structural, 
    # but we won't rename the event names themselves (e.g. keep 'Balance Beam').
    
    for event in event_names:
        # Sanitize slightly for CSV header safety (no spaces) but don't normalize to "Beam"
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
        # --- EXTRACT RICH METADATA (Service Columns) ---
        raw_meet_name = ""
        raw_year = ""
        try:
            raw_meet_name_el = driver.find_element(By.ID, "event-title")
            raw_meet_name = raw_meet_name_el.text.strip()
            
            # Extract Year from header-text
            header_el = driver.find_element(By.CLASS_NAME, "header-text")
            header_text = header_el.text
            import re
            year_match = re.search(r'\b(20\d{2})\b', header_text)
            if year_match:
                raw_year = year_match.group(1)
        except:
            pass

        sessions = []
        for el in driver.find_elements(By.CSS_SELECTOR, "#sel-sess option:not([value=''])"):
            sessions.append({
                'id': el.get_attribute('value'),
                'name': el.text.strip()
            })
        print(f"Found {len(sessions)} sessions.")

        for session in sessions:
            print(f"  -- Processing session: {session['name']} (ID: {session['id']}) --")
            
            # 1. Select the session in the UI
            try:
                sess_select = driver.find_element(By.ID, "sel-sess")
                for option in sess_select.find_elements(By.TAG_NAME, "option"):
                    if option.get_attribute("value") == session['id']:
                        option.click()
                        time.sleep(2) # Wait for category dropdown to populate
                        break
            except Exception as e:
                print(f"    Warning: Could not select session {session['name']}: {e}")
                continue

            # 2. Identify Categories from the Level/Category Dropdown (#sel-cat)
            categories = []
            try:
                cat_select_el = driver.find_element(By.ID, "sel-cat")
                for el in cat_select_el.find_elements(By.CSS_SELECTOR, "option:not([value=''])"):
                    name = el.text.strip()
                    # Skip placeholders like "Select Category" or "All Categories"
                    if any(x in name for x in ["Select", "Category", "All ", "---"]):
                        continue
                    categories.append({
                        'value': el.get_attribute('value'),
                        'name': name
                    })
            except Exception as e:
                print(f"    Warning: Could not find categories for session {session['name']}: {e}")
                continue

            print(f"    Found {len(categories)} categories (levels).")

            for cat_idx, cat in enumerate(categories):
                level_name = cat['name']
                print(f"    -> Scraping level: {level_name} (ID: {cat['value']})")

                # 3. Select the Category/Level in the UI
                try:
                    # Re-fetch the select element to avoid stale reference
                    cat_select_el = driver.find_element(By.ID, "sel-cat")
                    for option in cat_select_el.find_elements(By.TAG_NAME, "option"):
                        if option.get_attribute("value") == cat['value']:
                            option.click()
                            # Wait for the table to refresh or update. 
                            # A simple sleep is often most reliable for K-Score's older AJAX.
                            time.sleep(2) 
                            break
                except Exception as e:
                    print(f"       Warning: Could not select level {level_name}: {e}")
                    continue

                # 4. Grab the HTML from the live DOM
                try:
                    # Ensure #results-name has updated to match expectation or just grab what's there
                    results_name_el = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, "results-name"))
                    )
                    group_label = results_name_el.text.strip()
                    
                    # Find the table. Class 'a-results' is the standard results table.
                    table_el = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table.a-results"))
                    )
                    html_content = table_el.get_attribute('outerHTML')
                    
                    # Wrap in the headers/thead/tbody if outerHTML doesn't include it (it should)
                    # Use a full HTML snippet for Standardization function
                    full_html = f"<html><body>{html_content}</body></html>"
                except Exception as e:
                    print(f"       Warning: Could not find results table for {level_name}: {e}")
                    continue

                if not html_content or "There are no results" in html_content:
                    print("       No results table found in this category.")
                    continue

                df = standardize_kscore_columns(full_html)

                if df is None or df.empty:
                    print("       Failed to create a DataFrame from the table.")
                    continue
                
                if 'Rank' in df.columns:
                    df = df.drop(columns=['Rank'])
                
                # --- APPLY SERVICE COLUMN STANDARDIZATION ---
                df['Meet'] = meet_name
                df['Raw_Meet_Name'] = raw_meet_name
                df['Session'] = session['name']
                df['Group'] = group_label # e.g. Provincial 2A
                df['Age_Group'] = level_name
                # User Feedback: "Ignore age group as they do not give it". Use Group (HTML Header) as Level.
                # Prioritize group_label for Level, fallback to level_name if empty.
                df['Level'] = group_label if group_label else level_name 
                df['Age'] = ""   
                df['Prov'] = ""  
                df['Year'] = raw_year

                # Drop 'Category' if it was picked up from the table headers
                if 'Category' in df.columns:
                    df = df.drop(columns=['Category'])

                # 2. Enforce the Standard Column Order
                standard_info_cols = ['Name', 'Club', 'Level', 'Age', 'Prov', 'Age_Group', 'Meet', 'Raw_Meet_Name', 'Session', 'Group']
                # filter for columns that actually exist or were just created
                existing_info_cols = [col for col in standard_info_cols if col in df.columns]
                result_cols = [col for col in df.columns if col not in existing_info_cols]
                
                df = df[existing_info_cols + result_cols]
                
                output_filename = f"{meet_id}_FINAL_{session['id']}_{cat_idx}.csv"
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

        # --- SKIP LOGIC ---
        # Note: KScore files have naming pattern: {meet_id}_FINAL_{session['id']}_{cat_idx}.csv
        existing_files = glob.glob(os.path.join(OUTPUT_DIR_KSCORE, f"{meet_id}_FINAL_*.csv"))
        if existing_files:
            print(f"Skipping already scraped meet: {meet_name} (ID: {meet_id}). Found {len(existing_files)} files.")
            continue
        # ------------------

        print("\n" + "="*70)
        print(f"[{total_meets_processed}/{total_meets_in_queue}] Processing: {meet_name} (ID: {meet_id})")
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