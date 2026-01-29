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
import logging
import traceback

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

def wait_for_results_to_load(driver, timeout=30):
    """
    Polls the page until the sportzsoft results table is fully rendered
    and not in a 'Loading...' or empty state.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        html = driver.page_source
        if "Loading..." not in html and ("resultsTable" in html or "gridData" in html or "sessionEventResults" in html):
            # Check if there are actual data rows in the tables
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.select("table.resultsTable, table.gridData, table#sessionEventResults")
            for t in tables:
                rows = t.find_all('tr')
                if len(rows) > 1: # Header + at least one data row
                    return True
        time.sleep(1)
    print("  -> Warning: Wait for results timed out or no data found.")
    return False


def scrape_raw_data_to_separate_files(main_page_url, meet_id_for_filename, output_directory="raw_data", driver_path=None, target_level_name=None):
    """
    Scrapes all event data, saving each table into its own CSV file.
    This version uses a more robust file naming scheme to handle multiple
    tables per page correctly.
    """
    print(f"--- STEP 1: Scraping Raw Data for {main_page_url} ---")
    
    os.makedirs(output_directory, exist_ok=True)
    
    if not meet_id_for_filename:
        print("--> FATAL ERROR: A valid Meet ID was not provided to the scraper function.")
        return False, 0, None

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

    driver = None
    total_files_saved = 0
    full_success = True
    
    try:
        if driver_path:
            service = Service(driver_path)
        else:
            service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        driver.get(main_page_url)
        
        try:
            # Step 1: Wait for page content and check if results are available
            time.sleep(5)
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
                return False, 0, None

            # --- GENDER TOGGLE DETECTION ---
            # Try multiple methods to detect the gender toggle (Female/Male)
            toggle_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'ChangeDivGender')]")
            if len(toggle_links) == 0:
                toggle_links = driver.find_elements(By.CSS_SELECTOR, ".firstradioTab, .lastradioTab, .radioTab")
            if len(toggle_links) == 0:
                toggle_links = driver.find_elements(By.XPATH, "//span[text()='Male' or text()='Female']")
            
            gender_toggle_available = len(toggle_links) > 0 or "ChangeDivGender" in page_text
            genders_to_scrape = ['F', 'M'] if gender_toggle_available else [None]
            
            if gender_toggle_available:
                print(f"  -> Gender toggle DETECTED ({len(toggle_links)} elements). Will scrape both Female and Male results.")

            for current_gender in genders_to_scrape:
                gender_label = {'F': 'WAG', 'M': 'MAG'}.get(current_gender, '')
                gender_suffix = f"_{gender_label}" if gender_label else ""

                # Switch gender if toggle is available
                if current_gender is not None:
                    print(f"  -> Switching to gender: {current_gender} ({gender_label})")
                    try:
                        driver.execute_script(f"ChangeDivGender('{current_gender}');")
                        time.sleep(5)
                    except Exception as e:
                        print(f"    -> Warning: Failed to switch gender to {current_gender}: {e}")
                        continue

                # Step 1.1: Switch to "Results by Session" tab if possible
                print(f"  -> Attempting to switch to 'Results by Session' tab ({gender_label})...")
                
                # Check if the transition is possible (if the form and function exist)
                can_switch = driver.execute_script("return typeof gotoSubTab === 'function' && typeof document.Tournament !== 'undefined';")
                if can_switch:
                    driver.execute_script("gotoSubTab('Z')")
                    time.sleep(5) # Wait for page to refresh
                
                # Step 1.2: Identify all sessions in the "FilterPanel"
                html_content = driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Get Meet Name from the page
                meet_name_element = soup.select_one("div#thHeader.TournamentHeader div div.TournamentHeading")
                meet_name = meet_name_element.get_text(strip=True) if meet_name_element else "Unknown Meet Name"
                
                # --- TNT SKIP LOGIC (Only once if possible, but safe here) ---
                tnt_keywords = ["TNT", "T&T", "TG ", " TG", "T G ", "T.G.", "TUMBLING", "TRAMPOLINE", "T & T"]
                page_text_upper = html_content.upper()
                if any(k in meet_name.upper() for k in tnt_keywords) or \
                   "DOUBLE MINI" in page_text_upper or \
                   ("TRAMPOLINE" in page_text_upper and "TUMBLING" in page_text_upper):
                    print(f"--> SKIPPING TNT MEET SIDE: '{meet_name}' ({gender_label})")
                    continue # Skip this gender side
                # ----------------------
                
                # === LEVEL-BASED EXTRACTION (PRIORITY) ===
                # The user prefers consistent "Level" columns. We attempt to extract by Level first.
                
                # 1. Attempt using 'reportOnLv' / 'ReportDivResults' directly or via 'Results by Level' tab
                print("  -> Attempting Level-Based Extraction (Priority)...")
                level_targets = {}
            
                # Try switching to 'Results by Level' tab ('D')
                can_switch_level = driver.execute_script("return typeof gotoSubTab === 'function' && typeof document.Tournament !== 'undefined';")

                if can_switch_level:
                    try:
                        driver.execute_script("gotoSubTab('D')")
                        time.sleep(3) # Wait for sidebar
                    except Exception as e:
                        print(f"    -> Error switching to Level tab: {e}")
    
                # Scrape available levels from sidebar
                html_content = driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Standard Sportzsoft Level List
                level_elements = soup.select("#FilterPanel li.liCategory") 
                # Some meets use a different class or ID structure for levels
                if not level_elements:
                     level_elements = soup.select("#FilterPanel li[onclick^='reportOnLv']")
    
                for el in level_elements:
                    div_id = el.get('id')
                    # If onclick is explicit, extracted ID might differ, but usually ID is enough for click/JS
                    name_el = el.select_one(".repSessionShortName") or el
                    name = name_el.get_text(strip=True)
                    
                    if div_id and name:
                         level_targets[name] = div_id
                
                if level_targets:
                    print(f"  -> Found {len(level_targets)} Levels. Using Level-Based Extraction.")
                    sessions_to_scrape = level_targets
                    base_data_url_param = "DivId" # Or "ReportingCategory" depending on context, but usually DivId for 'D' tab
                else:
                    print("  -> No Levels found. Falling back to Session-Based Extraction.")
                    
                    # Switch back to 'Results by Session' tab ('Z')
                    if can_switch_level:
                        driver.execute_script("gotoSubTab('Z')")
                        time.sleep(3)
                    
                    html_content = driver.page_source
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    session_elements = soup.select("#FilterPanel li.reportOnSession")
                    sessions_to_scrape = {}
                    for el in session_elements:
                        session_id = el.get('id')
                        session_name_el = el.select_one(".repSessionShortName")
                        if session_id and session_name_el:
                            sessions_to_scrape[session_name_el.get_text(strip=True)] = session_id
                    
                    if not sessions_to_scrape:
                        print("--> ERROR: No Sessions OR Levels found. This meet structure is unrecognized.")
                        return False, 0, None
                    
                    base_data_url_param = "SelectSession"
                    print(f"  -> Found {len(sessions_to_scrape)} competitive sessions via Fallback.")
    
                # Re-get the web session ID from the URL or state
                session_id_match = re.search(r'SessionId=([a-zA-Z0-9]+)', driver.current_url)
                web_session_id = session_id_match.group(1) if session_id_match else None
                
                # Prepare requests session for fast fetching
                s = requests.Session()
                # Pass cookies from selenium to requests
                for cookie in driver.get_cookies():
                    s.cookies.set(cookie['name'], cookie['value'])
    
                base_data_url = "https://www.sportzsoft.com/meet/meetWeb.dll/TournamentResults"
                
                for group_name, comp_session_id in sessions_to_scrape.items():
                    try:
                        # Filter by target level if specified
                        if target_level_name and target_level_name.lower() not in group_name.lower():
                             continue
    
                        print(f"  -> Selecting group/session: {group_name}")
                        
                        # Ensure we are on the main meet page and Session tab is active
                        if driver.current_url != main_page_url:
                            driver.get(main_page_url)
                            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#FilterPanel")))
                            
                            # CRITICAL: Re-apply gender switch if we executed a reload, 
                            # because the page likely defaulted back to 'Female' (or meet default).
                            # The toggle is usually visible on the default 'Results by Level' tab.
                            if gender_toggle_available and current_gender:
                                try:
                                    # We invoke the function directly. 
                                    # Use a short sleep to allow the AJAX to update the internal state/URL
                                    driver.execute_script(f"ChangeDivGender('{current_gender}');")
                                    time.sleep(2) 
                                except Exception as e:
                                    print(f"    -> Warning: Failed to re-apply gender {current_gender} after reload: {e}")
                        
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
                                    wait_for_results_to_load(driver) # Use our new robust wait
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
                        
                        # --- NEW: APPARATUS DISCOVERY & PER-EVENT AGGREGATION ---
                        # 1. Switch to 'Per Event' View
                        print(f"    -> Switching to 'Per Event' view (Detailed mode)")
                        try:
                            driver.execute_script("ChangeReportType('P');")
                            time.sleep(3)
                        except:
                            pass
                        # Discover Sub-Sessions (Session tabs in the middle)
                        sub_session_discovery_js = r"""
                        var subs = [];
                        var sub_links = document.querySelectorAll('a[href*="gotoSessionTab"]');
                        sub_links.forEach(function(a) {
                            var val = a.getAttribute('href') || "";
                            var match = val.match(/gotoSessionTab\('(\d+)'\)/i);
                            if (match) {
                                subs.push({ label: a.innerText.trim(), id: match[1] });
                            }
                        });
                        return subs;
                        """
                        sub_sessions = driver.execute_script(sub_session_discovery_js) or []
                        if not sub_sessions:
                            # Also check for already loaded view if no tabs discovered
                            sub_sessions = [{'label': 'Combined', 'id': '99'}]
    
                        for sub_info in sub_sessions:
                            sub_label = sub_info['label']
                            sub_id = sub_info['id']
                            print(f"    -> Entering Sub-Session: {sub_label} (ID: {sub_id})")
                            
                            if sub_id:
                                try:
                                    driver.execute_script(f"gotoSessionTab('{sub_id}');")
                                    time.sleep(3)
                                except:
                                    pass
    
                            # 2. Discover Apparatuses available for THIS sub-session
                            apparatus_discovery_js = r"""
                            var apps = [];
                            var app_links = document.querySelectorAll('a');
                            app_links.forEach(function(a) {
                                var val = a.getAttribute('onclick') || a.getAttribute('href') || "";
                                var match = val.match(/ChangeApparatus\('(\d+)'\)/i);
                                if (match) {
                                    var lbl = a.innerText.trim();
                                    if (!lbl && a.querySelector('span')) lbl = a.querySelector('span').innerText.trim();
                                    apps.push({ label: lbl, id: match[1] });
                                }
                            });
                            var uniq = [];
                            var seen = {};
                            apps.forEach(function(app) {
                                if (!seen[app.id]) {
                                    uniq.push(app);
                                    seen[app.id] = true;
                                }
                            });
                            return uniq;
                            """
                            session_apparatuses = driver.execute_script(apparatus_discovery_js) or []
                            
                            # NOTE: AA data cannot be scraped from per-event view.
                            # Do NOT add fake ID '0' here - it just re-scrapes the last apparatus.
                            # AA totals must come from a separate scrape of the Combined/All-Around view.
                            
                            # 3. Aggregation Buffer for THIS Sub-Session
                            athlete_data_wide = {}
    
                            for app_info in session_apparatuses:
                                app_label = app_info['label']
                                app_id = app_info['id']
                                print(f"    -> Fetching apparatus: {app_label} (ID: {app_id})")
                                
                                try:
                                    # Trigger ChangeApparatus in Selenium
                                    retry_count = 0
                                    max_retries = 3
                                    while retry_count < max_retries:
                                        try:
                                            driver.execute_script(f"ChangeApparatus('{app_id}');")
                                            wait_for_results_to_load(driver) # Use robust wait
                                            break
                                        except Exception as e:
                                            retry_count += 1
                                            print(f"      -> Retry {retry_count}/{max_retries} switching to {app_label}: {e}")
                                            time.sleep(2)
                                    
                                    if retry_count == max_retries:
                                        print(f"      -> Failed to switch to {app_label} after retries. Skipping.")
                                        continue
    
                                    
                                    # Extract HTML directly from DOM
                                    page_html = driver.page_source
                                    results_soup = BeautifulSoup(page_html, 'html.parser')
                                    
                                    # Find ALL tables that look like results
                                    table_elements = results_soup.find_all('table', class_='resultsTable') or \
                                                     results_soup.find_all('table', id='sessionEventResults') or \
                                                     results_soup.find_all('table', id='PerEventScores')
                                    
                                    # Fallback to wrappers if no explicit tables found
                                    if not table_elements:
                                        wrappers = results_soup.find_all('div', class_='resultsTableWrapper')
                                        table_elements = [w.find('table') for w in wrappers if w.find('table')]
                                    
                                    print(f"      -> Found {len(table_elements)} tables in DOM for {app_label}")
                                    athletes_found_this_app = 0
    
                                    for table_element in table_elements:
                                        if not table_element: continue
                                        
                                        # Find Age Group for THIS specific table
                                        # It's usually in a .rpSubTitle DIV right before the table
                                        age_group = "N/A"
                                        prev_sib = table_element.find_previous(['div', 'span'], class_='resultsTitle') or \
                                                   table_element.find_previous('div', class_='resultsTableWrapper')
                                        
                                        if prev_sib:
                                            sub_titles = prev_sib.select(".rpSubTitle")
                                            for st in sub_titles:
                                                st_text = st.get_text(strip=True)
                                                if "(Age Group:" in st_text:
                                                    age_group = st_text.replace('(Age Group:', '').replace(')', '').strip()
                                                    break
                                        
                                        # Final sanity check for table element
                                        if table_element:
                                            table_html = str(table_element)
                                            df_list = pd.read_html(io.StringIO(table_html))
                                            if df_list:
                                                df = df_list[0].copy()
                                                suffix = app_label.replace(' ', '_')
                                                
                                                # Identify Identity Columns (Fuzzy match)
                                                name_col = None
                                                for c in df.columns:
                                                    if str(c).lower() in ['name', 'athlete', 'competitor']:
                                                        name_col = c
                                                        break
                                                if not name_col and len(df.columns) > 0:
                                                    name_col = df.columns[0]
                                                
                                                club_col = None
                                                for c in df.columns:
                                                    if str(c).lower() in ['club', 'team', 'org', 'province', 'prov']:
                                                        club_col = c
                                                        if str(c).lower() == 'club': break
                                                
                                                print(f"      -> Table shape: {df.shape}, Columns: {list(df.columns)}")
                                                if len(df) > 0:
                                                    print(f"      -> First row Name: {df.iloc[0].get(name_col)}")
                                                else:
                                                    print(f"      -> WARNING: DataFrame is empty for {app_label}. Snippet: {table_html[:200]}")
                                                
                                                info_cols = [name_col, club_col, 'Prov', 'Level', '#', 'Age', 'Age Group', 'Age_Group', 'Qual']
                                                
                                                # --- Fallback: Manual TR Parsing if df is empty or Name is missing ---
                                                if len(df) == 0 or not any(str(df.iloc[i].get(name_col)).strip() for i in range(min(len(df), 5))):
                                                    print("      -> Attempting manual TR parsing fallback...")
                                                    rows = table_element.find_all('tr')
                                                    # Try to find headers manually too? No, just use indices
                                                    for tr in rows:
                                                        tds = tr.find_all(['td', 'th'])
                                                        if len(tds) < 3: continue
                                                        
                                                        # Assume second or third col is Name
                                                        potential_name = tds[2].get_text(strip=True) if len(tds) > 2 else ""
                                                        potential_club = tds[3].get_text(strip=True) if len(tds) > 3 else ""
                                                        
                                                        if potential_name.lower() in ['name', 'athlete', 'competitor', 'nan', '']:
                                                            potential_name = tds[1].get_text(strip=True) # Try col 1
                                                            potential_club = tds[2].get_text(strip=True)
                                                            
                                                        if not potential_name or potential_name.lower() in ['name', 'athlete', 'competitor', 'nan']: continue
                                                        
                                                        athletes_found_this_app += 1
                                                        key = (potential_name, potential_club, age_group)
                                                        if key not in athlete_data_wide:
                                                            athlete_data_wide[key] = {
                                                                'Name': potential_name,
                                                                'Club': potential_club,
                                                                'Age_Group': age_group,
                                                                'Group': f"{group_name} - {sub_label}",
                                                                'Meet': meet_name,
                                                                'Prov': "",
                                                                'Level': ""
                                                            }
                                                        # Manual score extraction is too complex here, 
                                                        # but we can at least see if athletes are found.
                                                
                                                # STANDARD PATH
                                                for _, row_data in df.iterrows():
                                                    athlete_name = str(row_data.get(name_col, '')).strip()
                                                    athlete_club = str(row_data.get(club_col, '')).strip() if club_col else ""
                                                    if not athlete_name or athlete_name.lower() in ['name', 'nan', 'athlete']: continue
                                                    
                                                    athletes_found_this_app += 1
                                                    key = (athlete_name, athlete_club, age_group)
                                                    if key not in athlete_data_wide:
                                                        athlete_data_wide[key] = {
                                                            'Name': athlete_name,
                                                            'Club': athlete_club,
                                                            'Age_Group': age_group,
                                                            'Group': f"{group_name} - {sub_label}",
                                                            'Meet': meet_name,
                                                            'Prov': str(row_data.get('Prov', '')),
                                                            'Level': str(row_data.get('Level', ''))
                                                        }
                                                    
                                                    # Map columns
                                                    for col in df.columns:
                                                        if col in info_cols or 'Unnamed' in str(col): continue
                                                        
                                                        val = row_data.get(col)
                                                        if pd.isna(val): continue
    
                                                        target_suffix = str(col)
                                                        if col == 'D-Score': target_suffix = 'D'
                                                        elif col == 'E Score': target_suffix = 'E'
                                                        elif col == 'Rank': target_suffix = 'Rnk'
                                                        elif col == 'Final' or col == app_label: target_suffix = 'Score'
                                                        
                                                        clean_suffix = re.sub(r'[^a-zA-Z0-9]+', '_', target_suffix)
                                                        athlete_data_wide[key][f'Result_{suffix}_{clean_suffix}'] = val
    
                                                    # NOTE: AllAround is NOT scraped here via ID '0' - it doesn't work.
                                                    # AA data is scraped separately from the dedicated All-Around view.
    
                                    print(f"      -> Processed {athletes_found_this_app} athlete records for {app_label}")
                                except Exception as driver_err:
                                    print(f"      -> Driver Extraction Error: {driver_err}")
                                    full_success = False
    
                            # 4. Save Per-Event CSV (apparatus data)
                            if athlete_data_wide:
                                final_df = pd.DataFrame(athlete_data_wide.values())
                                info_cols_order = ['Name', 'Club', 'Level', 'Prov', 'Age_Group', 'Meet', 'Group']
                                res_cols = [c for c in final_df.columns if c not in info_cols_order]
                                final_df = final_df[info_cols_order + res_cols]
                                
                                safe_name = re.sub(r'[\s/\\:*?"<>|]+', '_', f"{group_name}_{sub_label}")
                                filename = f"{meet_id_for_filename}_PEREVENT_{safe_name}{gender_suffix}_DETAILED.csv"
                                final_df.to_csv(os.path.join(output_directory, filename), index=False)
                                print(f"    -> Saved Per-Event: {filename}")
                                total_files_saved += 1
    
                            # 5. Switch to By-Event view and save full table (for AA data)
                            print("    -> Switching to By-Event view for AA data...")
                            try:
                                driver.execute_script("ChangeReportType('E');")  # 'E' = By Event view
                                if not wait_for_results_to_load(driver, timeout=45):
                                     time.sleep(5) # Final fallback
                                
                                # Scrape all tables from By-Event view (try multiple selectors)
                                be_html = driver.page_source
                                be_soup = BeautifulSoup(be_html, 'lxml')
                                # Try gridData first, then any table with id containing 'scores'
                                be_tables = be_soup.find_all('table', class_='gridData')
                                if not be_tables:
                                    be_tables = be_soup.find_all('table', id=lambda x: x and 'score' in x.lower())
                                if not be_tables:
                                    be_tables = be_soup.find_all('table')
                                
                                all_be_rows = []
                                for be_table in be_tables:
                                    try:
                                        be_df = pd.read_html(io.StringIO(str(be_table)), header=0)[0]
                                        if be_df.empty:
                                            continue
                                        # Add age group from table attribute if present
                                        age_part = be_table.get('data-agepartitioncd', '')
                                        if age_part:
                                            be_df['Age_Group_Code'] = age_part
                                        # Add metadata
                                        be_df['Meet'] = meet_name
                                        be_df['Group'] = f"{group_name} - {sub_label}"
                                        be_df['Level'] = group_name
                                        all_be_rows.append(be_df)
                                    except Exception:
                                        continue
                                
                                if all_be_rows:
                                    be_final_df = pd.concat(all_be_rows, ignore_index=True)
                                    
                                    # Reorder columns: service columns first, then result columns
                                    service_cols = ['Name', 'Club', 'Level', 'Prov', 'Age', 'Age_Group_Code', 'Meet', 'Group']
                                    service_cols = [c for c in service_cols if c in be_final_df.columns]
                                    result_cols = [c for c in be_final_df.columns if c not in service_cols]
                                    be_final_df = be_final_df[service_cols + result_cols]
                                    
                                    safe_name = re.sub(r'[\s/\\:*?"<>|]+', '_', f"{group_name}_{sub_label}")
                                    
                                    # Check if this By-Event file is actually just a single event (common with bad headers)
                                    # If it has mostly empty columns except for one apparatus, we might want to name it differently
                                    # But for now, let's just save it.
                                    
                                    # DE-DUPLICATE COLUMNS before saving to avoid issues in step 2
                                    cols = pd.Series(be_final_df.columns)
                                    for dup in cols[cols.duplicated()].unique():
                                        cols[cols[cols == dup].index.values.tolist()] = [f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))]
                                    be_final_df.columns = cols
    
                                    be_filename = f"{meet_id_for_filename}_MESSY_BYEVENT_{safe_name}{gender_suffix}.csv"
                                    be_final_df.to_csv(os.path.join(output_directory, be_filename), index=False)
                                    print(f"      -> Saved By-Event (Messy): {be_filename} ({len(be_final_df)} rows)")
                                    total_files_saved += 1
                                else:
                                    print("      -> No By-Event data found")
                                
                                # Switch back to Per-Event for next sub-session
                                driver.execute_script("ChangeReportType('P');")
                                time.sleep(2)
                                
                            except Exception as be_err:
                                print(f"      -> Error scraping By-Event view: {be_err}")
                                full_success = False
    
                    except Exception as e:
                        print(f"  -> Error processing '{group_name}': {e}")
                        full_success = False
                        continue
            
        except (TimeoutException, UnexpectedAlertPresentException) as e:
            print(f"--> SKIPPING MEET: The page at {main_page_url} failed to load correctly. Error: {e}")
            return False, 0, None
        except Exception as e:
            print(f"--> FATAL ERROR in scrape_raw_data_to_separate_files: {e}")
            traceback.print_exc()
            return False, 0, None
            
    finally:
        if driver:
            driver.quit()
    
    if total_files_saved > 0:
        return full_success, total_files_saved, meet_id_for_filename
    return False, 0, None

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
        
    # Check if first column is an index (Unnamed 0)
    if 'Unnamed' in str(df.iloc[0, 0]) or str(df.iloc[0, 0]) == '0':
        # Historically, many scrapers saved with an index. If so, drop it.
        # But only if it looks like an index.
        try:
             first_col_is_index = all(str(x).isdigit() for x in df.iloc[1:10, 0])
             if first_col_is_index:
                 df = df.iloc[:, 1:].copy()
        except:
             pass

    header_row_index = -1
    header_row_index = -1
    
    # Robust Header Detection
    HEADER_CANDIDATES = ['name', 'athlete', 'gymnast', 'competitor']
    
    for i, row in df.iterrows():
        # Check if any cell in the row matches one of our candidates (case-insensitive)
        row_values = [str(val).strip().lower() for val in row.values]
        if any(candidate in row_values for candidate in HEADER_CANDIDATES):
            header_row_index = i
            break
            
    if header_row_index == -1:
        # Check for known "junk" patterns (e.g. Privacy/Security footer only)
        first_rows_tex = df.head(10).to_string()
        if "Privacy" in first_rows_tex and "Security" in first_rows_tex:
            print(f"  -> Skipping file '{os.path.basename(input_filename)}': Contains only Privacy/Security placeholders (No data).")
            return False
            
        print(f"Error: Could not find the main header row (checked for: {HEADER_CANDIDATES}) in '{input_filename}'.")
        # Debug: Print first few rows to help identify the issue
        print("--- Debug: First 5 rows of file ---")
        print(df.head(5))
        return False

    # --- THIS IS THE NEW, CORRECTED LOGIC ---
    main_header_row = df.iloc[header_row_index]
    # Check if we have a sub-header row
    has_sub_header = (header_row_index + 1 < len(df)) and any(x in ['Score', 'Rnk', 'D', 'SV'] for x in df.iloc[header_row_index+1].values)
    
    # Data starts AFTER the header row(s)
    start_idx = header_row_index + 2 if has_sub_header else header_row_index + 1
    data_df = df.iloc[start_idx:].copy()

    sub_header_row = df.iloc[header_row_index + 1] if has_sub_header else pd.Series([""] * len(main_header_row))
    
    main_header = pd.Series(main_header_row).ffill() 
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
            clean_name = final_name.replace(' ', '_')
            
            # Avoid duplicate column names by appending index
            if clean_name in clean_header:
                count = sum(1 for c in clean_header if c.startswith(clean_name))
                clean_name = f"{clean_name}_{count}"
                
            clean_header.append(clean_name)
            j += 1

    # --- APPARATUS NAME MAPPING ---
    # User Request: "Scrape as is". 
    # Do not normalize 'Balance_Beam' to 'Beam'.
    mapped_header = clean_header # Pass through logic removed

    # --- UNIFY HEADERS BEFORE ASSIGNMENT ---
    unique_headers = []
    _counts = {}
    for h in mapped_header:
        candidate = str(h)
        if candidate in _counts:
            _counts[candidate] += 1
            unique_headers.append(f"{candidate}_{_counts[candidate]}")
        else:
            _counts[candidate] = 0
            unique_headers.append(candidate)
    
    data_df.columns = unique_headers
    data_df = data_df.copy() # Ensure unique index/columns are baked in

    # Robust trailing column identifies: only rename if we don't already have them
    # AND if the trailing columns are 'Unnamed' or blank.
    # This prevents overwriting result columns in By-Event files.
    trailing_count = 0
    if not any(c in data_df.columns for c in ['Group', 'Meet', 'Age_Group', 'Reporting_Category']):
        # If we don't have them at all, we might be using an old scraper version
        # Check if the last columns look like metadata (generic names)
        potential_meta = []
        for i in range(1, 5):
            col_name = str(data_df.columns[-i])
            if 'Unnamed' in col_name or col_name == '' or col_name.isdigit():
                 potential_meta.append(i)
        
        if len(potential_meta) >= 3: # If at least 3 are unnamed/generic
            data_df.rename(columns={
                data_df.columns[-4]: 'Group',
                data_df.columns[-3]: 'Meet',
                data_df.columns[-2]: 'Age_Group',
                data_df.columns[-1]: 'Reporting_Category'
            }, inplace=True)

    # --- APPLY SERVICE COLUMN STANDARDIZATION ---
    standard_info_cols = ['Name', 'Club', 'Level', 'Age', 'Prov', 'Age_Group', 'Reporting_Category', 'Meet', 'Group']
    
    # 1. Ensure all standard columns exist as unique targets
    # (data_df.columns was already unified above)
    for col in standard_info_cols:
        if col not in data_df.columns:
            data_df[col] = "" # Permissive assignment
    
    # 2. Extract Level from Group if Level is empty
    def extract_level(row):
        try:
            val = row['Level']
            if val and str(val).strip():
                return val
        except Exception:
            pass
        group = str(row['Group'])
        # Common patterns: "Level 4", "P1", "CCP 6", "CPP 1"
        match = re.search(r'(Level\s*\d+|CCP\s*\d+|P\d+|CPP\s*\d+|Provincial\s*\d+|Junior\s*[A-Z]|Senior\s*[A-Z]|Xcel\s*[a-zA-Z]+)', group, re.I)
        return match.group(0) if match else ""

    data_df.loc[:, 'Level'] = data_df.apply(extract_level, axis=1)

    # 3. Consolidate Province/Prov
    if 'Province' in data_df.columns:
         data_df.loc[:, 'Prov'] = data_df.apply(lambda r: r['Province'] if not str(r['Prov']).strip() else r['Prov'], axis=1)
         data_df.drop(columns=['Province'], inplace=True)

    # 4. Enforce standard order for info columns, then results
    # Drop any trailing rows that are mostly empty (common in messy Sportzsoft exports)
    data_df = data_df[data_df.iloc[:, 0].astype(str).str.strip() != ''].copy()
    
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
    MEET_IDS_CSV = "verification_manifest.csv"
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
        
        # --- SKIP LOGIC ---
        # Check if any final CSVs already exist for this meet ID
        final_files_found = glob.glob(os.path.join(FINAL_FOLDER, f"{meet_id}_FINAL_*.csv"))
        if final_files_found:
            print(f"Skipping already scraped meet: {meet_id} ({len(final_files_found)} final files exist)")
            continue
        # ------------------
        
        meet_url = f"{BASE_URL}{meet_id}"
        
        # --- STEP 1: Scrape messy files ---
        success, files_saved, file_base_id = scrape_raw_data_to_separate_files(meet_url, meet_id, MESSY_FOLDER)
        
        if files_saved > 0:
            print(f"Scraping complete. Found {files_saved} tables for Meet ID {file_base_id}.")
            print("--- Starting Step 2: Finalizing Files ---")
            
            # 1. Process Messy files (legacy or fallback)
            search_pattern_messy = os.path.join(MESSY_FOLDER, f"{file_base_id}_MESSY_*.csv")
            messy_files_to_process = glob.glob(search_pattern_messy)
            
            for messy_file_path in messy_files_to_process:
                messy_filename = os.path.basename(messy_file_path)
                final_filename = messy_filename.replace('_MESSY_', '_FINAL_').replace('_BYEVENT_', '_AA_')
                final_file_path = os.path.join(FINAL_FOLDER, final_filename)
                if not fix_and_standardize_headers(messy_file_path, final_file_path):
                    print(f"--- ❌ FAILED at Step 2 (Finalizing) for: {messy_file_path} ---")

            # 2. Process already Finalized files (from new Detailed mode)
            search_pattern_final = os.path.join(MESSY_FOLDER, f"{file_base_id}_FINAL_*_DETAILED.csv")
            already_finalized = glob.glob(search_pattern_final)
            
            for finalized_path in already_finalized:
                final_filename = os.path.basename(finalized_path)
                target_path = os.path.join(FINAL_FOLDER, final_filename)
                # Move or Copy (Move is better to keep MESSY_FOLDER clean)
                try:
                    import shutil
                    shutil.move(finalized_path, target_path)
                    print(f"--- ✅ Acknowledged and moved finalized file: {final_filename} ---")
                except Exception as e:
                    print(f"--- ❌ Failed to move finalized file {final_filename}: {e} ---")

            print(f"--- ✅ Successfully processed all generated tables for Meet ID: {meet_id} ---")
        else:
            print(f"--- ❌ FAILED or SKIPPED at Step 1 (Scraping) for Meet ID: {meet_id} ---")
        
        time.sleep(3)

    print("\n--- ALL MEETS PROCESSED ---")