import glob
import pandas as pd
import io
import json
import requests
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
    This version uses a more robust file naming scheme to handle multiple
    tables per page correctly.
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
    total_files_saved = 0
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        driver.get(main_page_url)
        
        try:
            # Step 1: Wait for page content and check if results are available
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Check for "disabled results" variants
            page_text = driver.page_source
            disabled_markers = [
                "Host has disabled public viewing of Results",
                "Results Reporting to the public are disabled",
                "Results are not available to the public"
            ]
            if any(marker in page_text for marker in disabled_markers):
                print(f"--> SKIPPING MEET: Results are disabled by the host for ID {meet_id_for_filename}.")
                return 0, None

            # Step 1.1: Switch to "Results by Session" tab if possible
            print("  -> Attempting to switch to 'Results by Session' tab...")
            
            # Check if the transition is possible (if the form and function exist)
            can_switch = driver.execute_script("return typeof gotoSubTab === 'function' && typeof document.Tournament !== 'undefined';")
            if can_switch:
                driver.execute_script("gotoSubTab('Z')")
                time.sleep(5) # Wait for page to refresh
            else:
                print("  -> Warning: 'gotoSubTab' not found. Results might be formatted differently.")
            
            # Step 1.2: Identify all sessions in the "FilterPanel"
            # We look for <li> elements that have the 'reportOnSession' class
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Get Meet Name from the page before it updates
            meet_name_element = soup.select_one("div#thHeader.TournamentHeader div div.TournamentHeading")
            meet_name = meet_name_element.get_text(strip=True) if meet_name_element else "Unknown Meet Name"
            
            # --- TNT SKIP LOGIC ---
            # Check for TNT/T&T keywords in meet name or page content
            tnt_keywords = ["TNT", "T&T", "TG ", " TG", "T G ", "T.G.", "TUMBLING", "TRAMPOLINE", "T & T"]
            # Look at both meet name and page body for common TNT markers
            page_text_upper = page_text.upper() # Use already captured page_text from line 62
            if any(k in meet_name.upper() for k in tnt_keywords) or \
               "DOUBLE MINI" in page_text_upper or \
               ("TRAMPOLINE" in page_text_upper and "TUMBLING" in page_text_upper):
                print(f"--> SKIPPING TNT MEET: '{meet_name}' (ID: {meet_id_for_filename})")
                return 0, None
            # ----------------------
            
            # Initial SessionId identification (web session ID)
            active_session_id = ""
            session_id_match = re.search(r'SessionId=([a-zA-Z0-9]+)', html_content)
            if session_id_match:
                active_session_id = session_id_match.group(1)

            session_elements = soup.select("#FilterPanel li.reportOnSession")
            sessions_to_scrape = {}
            for el in session_elements:
                session_id = el.get('id')
                session_name_el = el.select_one(".repSessionShortName")
                if session_id and session_name_el:
                    sessions_to_scrape[session_name_el.get_text(strip=True)] = session_id
            
            if not sessions_to_scrape:
                print("--> INFO: No session-based results found in 'FilterPanel'. Falling back to level-based search (Plan B).")
                # Switch back to "Results by Level" tab if possible
                can_switch_level = driver.execute_script("return typeof gotoSubTab === 'function' && typeof document.Tournament !== 'undefined';")
                if can_switch_level:
                    driver.execute_script("gotoSubTab('D')")
                    time.sleep(3) # Wait for sidebar to refresh
                else:
                    print("  -> Skipping fallback SubTab switch: 'gotoSubTab' undefined.")
                
                html_content = driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')
                event_elements = soup.find_all('li', class_='liCategory')
                
                for el in event_elements:
                    name = el.get_text(strip=True)
                    div_id = el.get('id')
                    if name and div_id:
                        sessions_to_scrape[name] = div_id
                
                base_data_url_param = "DivId"
                print(f"  -> Found {len(sessions_to_scrape)} level/category groups via Plan B.")
            else:
                base_data_url_param = "SelectSession"
                print(f"  -> Found {len(sessions_to_scrape)} competitive sessions via Plan A.")

            # Re-get the web session ID from the URL or state
            session_id_match = re.search(r'SessionId=([a-zA-Z0-9]+)', driver.current_url)
            web_session_id = session_id_match.group(1) if session_id_match else active_session_id
            
            # Prepare requests session for fast fetching
            s = requests.Session()
            # Pass cookies from selenium to requests
            for cookie in driver.get_cookies():
                s.cookies.set(cookie['name'], cookie['value'])

            base_data_url = "https://www.sportzsoft.com/meet/meetWeb.dll/TournamentResults"
            
            for group_name, comp_session_id in sessions_to_scrape.items():
                try:
                    print(f"  -> Selecting group/session: {group_name}")
                    
                    # Ensure we are on the main meet page and Session tab is active
                    if driver.current_url != main_page_url:
                        driver.get(main_page_url)
                        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#FilterPanel")))
                    
                    # Ensure the correct tab is selected (Safe execution)
                    # 'Z' is Results by Session, 'D' is Results by Level/Category
                    target_tab = 'Z' if base_data_url_param == "SelectSession" else 'D'
                    try:
                        if driver.execute_script(f"return typeof gotoSubTab === 'function' && typeof document.Tournament !== 'undefined';"):
                            driver.execute_script(f"gotoSubTab('{target_tab}');")
                            time.sleep(2)
                            # Wait for the sidebar to populate
                            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#FilterPanel li")))
                        else:
                            print(f"    -> Skipping SubTab switch ({target_tab}): 'gotoSubTab' or 'Tournament' undefined.")
                    except Exception as tab_err:
                        print(f"    -> Warning: Could not ensure {target_tab} tab: {tab_err}")
                    
                    # Robust Switching Logic with Retries
                    switch_success = False
                    
                    # DYNAMIC FUNCTION DETECTION:
                    # Some meets use reportOnLv/reportOnSession, others use ReportDivResults/ReportOnSessionResults
                    actual_report_func = None
                    if base_data_url_param == "SelectSession":
                        # Variants for Session
                        for func_name in ["reportOnSession", "ReportOnSessionResults"]:
                            if driver.execute_script(f"return typeof {func_name} === 'function';"):
                                actual_report_func = func_name
                                break
                    else:
                        # Variants for Level/Category
                        for func_name in ["reportOnLv", "ReportDivResults"]:
                            if driver.execute_script(f"return typeof {func_name} === 'function';"):
                                actual_report_func = func_name
                                break
                    
                    if not actual_report_func:
                        print(f"    -> Warning: No known report function found for {base_data_url_param}. Falling back to click only.")

                    start_time = time.time()
                    while time.time() - start_time < 30: # Try for up to 30 seconds
                        try:
                            # 1. Standard Selenium Click
                            try:
                                # Ensure element is present and visible
                                elem = WebDriverWait(driver, 5).until(
                                    EC.element_to_be_clickable((By.ID, comp_session_id))
                                )
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", elem)
                                time.sleep(0.5)
                                elem.click()
                                print(f"    -> Switched via element click ({comp_session_id})")
                                switch_success = True
                            except Exception as click_err:
                                # 2. JS-based Click (Triggers the element's onclick in browser context)
                                # This is safer as it bypasses Selenium's visibility/interactability checks
                                try:
                                    is_present = driver.execute_script(f"return document.getElementById('{comp_session_id}') !== null;")
                                    if is_present:
                                        driver.execute_script(f"document.getElementById('{comp_session_id}').click();")
                                        print(f"    -> Switched via JS element click ({comp_session_id})")
                                        switch_success = True
                                except Exception:
                                    pass
                            
                            if switch_success:
                                # IMPORTANT: Wait for the results to actually start loading
                                # The class often changes to 'working' or 'active'
                                time.sleep(6) # Critical wait for server state update
                                break
                        except Exception as e:
                            # Outer loop retry
                            pass
                        time.sleep(2)

                    if not switch_success:
                        print(f"    -> Failed to switch to session {comp_session_id}. Skipping.")
                        continue

                    # UPDATE COOKIES: Ensure requests session has latest state/cookies from browser
                    s.cookies.clear()
                    for cookie in driver.get_cookies():
                        s.cookies.set(cookie['name'], cookie['value'])
                    print(f"    -> Cookies re-synced for requests session. Current URL: {driver.current_url}")
                    
                    # Re-capture Web Session ID if it changed
                    match = re.search(r"SessionId=([a-zA-Z0-9]+)", driver.current_url)
                    if match:
                         web_session_id = match.group(1)
                         print(f"    -> Web Session ID re-captured: {web_session_id}")
                    else:
                         print(f"    -> Warning: Could not re-capture Web Session ID from URL after switch.")

                    # Discovery of Reporting Categories via JS - AGGRESSIVE
                    js_discovery = """
                    var results = [];
                    // Look for ANY label that might be a category
                    var labels = document.querySelectorAll('label.ssRadioLabel, label[for^="rcReportingCategory"]');
                    labels.forEach(function(l) {
                        var inp = document.getElementById(l.getAttribute('for')) || document.querySelector('input[name="rcRC"][id="' + l.getAttribute('for') + '"]');
                        if (inp && !results.some(x => x.value === inp.value)) {
                            results.push({label: l.innerText.trim(), value: inp.value});
                        }
                    });
                    
                    // Fallback: search all inputs directly
                    if (results.length === 0) {
                        var radios = document.querySelectorAll('input[name="rcRC"], input[id^="rcReportingCategory"]');
                        radios.forEach(function(r) {
                            var label = document.querySelector('label[for="' + r.id + '"]');
                            results.push({label: label ? label.innerText.trim() : "Unknown", value: r.value});
                        });
                    }
                    return results;
                    """
                    js_results = driver.execute_script(js_discovery)
                    
                    scrape_targets = []
                    if js_results:
                        print(f"    -> Discovered {len(js_results)} categories via JS")
                        for item in js_results:
                            rc_label = item['label']
                            rc_id = item['value']
                            if rc_label.upper() == "ALL" and len(js_results) > 1:
                                continue
                            scrape_targets.append((rc_label, rc_id))
                    else:
                        print(f"    -> Info: No category filters found.")
                        if "Provincial 2" in group_name:
                            ss_name = f"debug_p2_failure_{comp_session_id}.png"
                            driver.save_screenshot(ss_name)
                            print(f"    -> Saved debug screenshot for Provincial 2: {ss_name}")
                        scrape_targets = [("Session_Overall", "0")]

                    for rc_label, rc_id in scrape_targets:
                        # Normalization
                        clean_rc_label = rc_label.replace('AllAround', 'All Around')
                        safe_rc_name = re.sub(r'[\s/\\:*?"<>|]+', '_', clean_rc_label).strip().replace('__', '_')
                        
                        print(f"    -> Processing: {rc_label} (ID: {rc_id})")
                        
                        # FETCH DATA VIA REQUESTS
                        # CRITICAL FIX: Do NOT include SelectSession/ResultsForSessionId when fetching a specific category.
                        # Including it resets the filter to "All". The session is already active from the switching step.
                        if rc_id != "0":
                            ajax_url = f"{base_data_url}?ReportingCategory={rc_id}&SessionId={web_session_id}&ReportOnly=1"
                        else:
                            # For "Session Overall" (ID 0) or if we need to force the session view
                            ajax_url = f"{base_data_url}?{base_data_url_param}={comp_session_id}&SessionId={web_session_id}&ReportingCategory={rc_id}&ReportOnly=1"
                        
                        try:
                            resp = s.get(ajax_url, timeout=15)
                            data = resp.json()
                            decoded_html = html.unescape(data['html'])
                            results_soup = BeautifulSoup(decoded_html, 'html.parser')
                            
                            table_wrappers = results_soup.find_all('div', class_='resultsTableWrapper')
                            if not table_wrappers:
                                 single_table = results_soup.find('table', id='sessionEventResults')
                                 if single_table:
                                     dummy_wrapper = results_soup.new_tag("div")
                                     dummy_wrapper.append(single_table)
                                     table_wrappers = [dummy_wrapper]

                            tables_in_group_counter = 1
                            for wrapper in table_wrappers:
                                age_group = "N/A"
                                title_element = wrapper.select_one(".resultsTitle .rpSubTitle")
                                if title_element:
                                    title_text = title_element.get_text(strip=True)
                                    age_group = title_text.replace('(Age Group:', '').replace(')', '').strip()

                                table_element = wrapper.find('table', id='sessionEventResults') or wrapper.find('table')
                                if table_element:
                                    df_list = pd.read_html(io.StringIO(str(table_element)))
                                    if df_list:
                                        df = df_list[0].copy()
                                        df['Group'] = group_name
                                        df['Meet'] = meet_name
                                        df['Age_Group'] = age_group
                                        df['Reporting_Category'] = rc_label
                                        
                                        safe_group_name = re.sub(r'[\s/\\:*?"<>|]+', '_', group_name)
                                        safe_age_group = re.sub(r'[\s/\\:*?"<>|]+', '_', age_group)
                                        
                                        filename = f"{meet_id_for_filename}_MESSY_{safe_group_name}_{safe_age_group}_{safe_rc_name}_{tables_in_group_counter}.csv"
                                        df.to_csv(os.path.join(output_directory, filename), index=False)
                                        print(f"    -> Saved: {filename}")
                                        total_files_saved += 1
                                        tables_in_group_counter += 1
                        except Exception as ajax_err:
                            print(f"    -> AJAX Error for {rc_label}: {ajax_err}")

                except Exception as e:
                    print(f"  -> Error processing '{group_name}': {e}")
                    continue
            
            if total_files_saved > 0:
                print(f"\n--> Success! Saved {total_files_saved} tables for '{meet_name}'.")
                return total_files_saved, meet_id_for_filename
            else:
                print(f"--> No data tables saved for {main_page_url}.")
                return 0, None

        except (TimeoutException, UnexpectedAlertPresentException) as e:
            print(f"--> SKIPPING MEET: The page at {main_page_url} failed to load correctly. Error: {e}")
            return 0, None
            
    finally:
        if driver:
            driver.quit()

def fix_and_standardize_headers(input_filename, output_filename):
    """
    Reads a raw/messy CSV, builds a single perfect header by correctly
    identifying triples (D/Score/Rnk), doubles (D/Score), and singles,
    and saves the final file.
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
        
    df = df.iloc[:, 1:].copy()

    header_row_index = -1
    for i, row in df.iterrows():
        if 'Name' in row.values:
            header_row_index = i
            break
            
    if header_row_index == -1:
        print(f"Error: Could not find the main header row (containing 'Name') in '{input_filename}'.")
        return False

    data_df = df.iloc[:header_row_index].copy()
    data_df = data_df[~data_df.iloc[:, 0].str.contains('Unnamed', na=False)].copy()

    if data_df.empty:
        print(f"Warning: No valid data rows found in '{input_filename}'. Skipping.")
        return True

    # --- THIS IS THE NEW, CORRECTED LOGIC ---
    main_header_row = df.iloc[header_row_index]
    sub_header_row = df.iloc[header_row_index + 1]
    main_header = pd.Series(main_header_row).ffill() # Forward-fill is still correct
    sub_header = pd.Series(sub_header_row)
    
    clean_header = []
    j = 0
    while j < len(main_header):
        event_name_raw = str(main_header.iloc[j]).strip()
        event_name = event_name_raw.replace(' ', '_')
        
        # --- Check for a TRIPLE (e.g., Uneven Bars) ---
        if (j + 2 < len(main_header) and 
            main_header.iloc[j] == main_header.iloc[j+1] == main_header.iloc[j+2] and
            str(sub_header.iloc[j+1]).strip() == 'Score' and 
            str(sub_header.iloc[j+2]).strip() == 'Rnk'):
            
            # The first sub-header could be 'D' or 'JO'. We'll call it 'D' for simplicity.
            clean_header.extend([f"Result_{event_name}_D", f"Result_{event_name}_Score", f"Result_{event_name}_Rnk"])
            j += 3
            
        # --- Check for a DOUBLE (e.g., AllAround) ---
        elif (j + 1 < len(main_header) and
              main_header.iloc[j] == main_header.iloc[j+1] and
              str(sub_header.iloc[j+1]).strip() == 'Score'):
              
            clean_header.extend([f"Result_{event_name}_D", f"Result_{event_name}_Score"])
            j += 2
            
        # --- Handle SINGLE columns (e.g., Name, Club, Level) ---
        else:
            name_from_main = event_name_raw
            name_from_sub = str(sub_header.iloc[j]).strip()
            final_name = name_from_main if name_from_main and 'Unnamed' not in name_from_main else name_from_sub
            clean_header.append(final_name.replace(' ', '_'))
            j += 1

    # --- APPARATUS NAME MAPPING ---
    # User Request: "Scrape as is". 
    # Do not normalize 'Balance_Beam' to 'Beam'.
    mapped_header = clean_header # Pass through logic removed

    if len(mapped_header) != data_df.shape[1]:
        print(f"Error: Final header length ({len(mapped_header)}) doesn't match data columns ({data_df.shape[1]}).")
        print("Constructed Header:", mapped_header)
        return False

    data_df.columns = mapped_header

    # Rename the trailing standard columns added during scraping
    # We now have 4 extra columns: Group, Meet, Age_Group, Reporting_Category
    data_df.rename(columns={
        data_df.columns[-4]: 'Group',
        data_df.columns[-3]: 'Meet',
        data_df.columns[-2]: 'Age_Group',
        data_df.columns[-1]: 'Reporting_Category'
    }, inplace=True)

    # --- APPLY SERVICE COLUMN STANDARDIZATION ---
    standard_info_cols = ['Name', 'Club', 'Level', 'Age', 'Prov', 'Age_Group', 'Reporting_Category', 'Meet', 'Group']
    
    # 1. Ensure all columns exist
    for col in standard_info_cols:
        if col not in data_df.columns:
            data_df[col] = ""
    
    # 2. Extract Level from Group if Level is empty
    def extract_level(row):
        if row['Level'] and str(row['Level']).strip():
            return row['Level']
        group = str(row['Group'])
        # Common patterns: "Level 4", "P1", "CCP 6", "CPP 1"
        match = re.search(r'(Level\s*\d+|CCP\s*\d+|P\d+|CPP\s*\d+|Provincial\s*\d+|Junior\s*[A-Z]|Senior\s*[A-Z]|Xcel\s*[a-zA-Z]+)', group, re.I)
        return match.group(0) if match else ""

    data_df['Level'] = data_df.apply(extract_level, axis=1)

    # 3. Consolidate Province/Prov
    if 'Province' in data_df.columns:
         data_df['Prov'] = data_df.apply(lambda r: r['Province'] if not str(r['Prov']).strip() else r['Prov'], axis=1)
         data_df.drop(columns=['Province'], inplace=True)

    # 4. Enforce standard order for info columns, then results
    other_cols = [col for col in data_df.columns if col not in standard_info_cols]
    final_df = data_df[standard_info_cols + other_cols]

    final_df.to_csv(output_filename, index=False)
    print(f"-> Success! Final clean file saved to '{output_filename}'")
    return True

  
# ==============================================================================
#  MAIN EXECUTION BLOCK (The "Application")
# ==============================================================================

if __name__ == "__main__":
    
    # --- CONFIGURATION ---
    MEET_IDS_CSV = "discovered_meet_ids_livemeet.csv"
    MESSY_FOLDER = "CSVs_Livemeet_messy"
    FINAL_FOLDER = "CSVs_Livemeet_final" 
    BASE_URL = "https://www.sportzsoft.com/meet/meetWeb.dll/MeetResults?Id="
    
    DEBUG_LIMIT = 0 # Set to 0 to run all

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
    total_meets = len(meet_ids_to_process)
    for i, meet_id in enumerate(meet_ids_to_process, 1):
        print(f"\n[{i}/{total_meets}] {'='*20} PROCESSING MEET ID: {meet_id} {'='*20}")
        
        meet_url = f"{BASE_URL}{meet_id}"
        
        # --- STEP 1: Scrape messy files ---
        files_saved, file_base_id = scrape_raw_data_to_separate_files(meet_url, meet_id, MESSY_FOLDER)
        
        if files_saved > 0:
            print(f"Scraping complete. Found {files_saved} tables for Meet ID {file_base_id}.")
            print("--- Starting Step 2: Finalizing Files ---")
            
            # --- THIS IS THE KEY FIX ---
            # Instead of guessing filenames with a loop from 1 to N,
            # we find all the messy files that were actually created for this meet.
            search_pattern = os.path.join(MESSY_FOLDER, f"{file_base_id}_MESSY_*.csv")
            messy_files_to_process = glob.glob(search_pattern)
            
            all_successful = True
            for messy_file_path in messy_files_to_process:
                # Construct the final filename from the messy one
                messy_filename = os.path.basename(messy_file_path)
                final_filename = messy_filename.replace('_MESSY_', '_FINAL_')
                final_file_path = os.path.join(FINAL_FOLDER, final_filename)

                if not fix_and_standardize_headers(messy_file_path, final_file_path):
                    print(f"--- ❌ FAILED at Step 2 (Finalizing) for: {messy_file_path} ---")
                    all_successful = False
                    # We can choose to break or continue with other files
                    # break 

            if all_successful:
                print(f"--- ✅ Successfully processed all {len(messy_files_to_process)} tables for Meet ID: {meet_id} ---")
        else:
            print(f"--- ❌ FAILED or SKIPPED at Step 1 (Scraping) for Meet ID: {meet_id} ---")
        
        time.sleep(3)

    print("\n--- ALL MEETS PROCESSED ---")