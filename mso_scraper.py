import os
import time
import pandas as pd
import re
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
DEBUG_LIMIT = 0
OUTPUT_FOLDER = "CSVs_mso_final"
INPUT_MANIFEST = "discovered_meet_ids_mso.csv"

# MSO 'All' Option Value
ALL_OPTION_VALUE = "---All"

# Ensure output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def setup_driver(driver_path=None):
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.page_load_strategy = 'eager'  # Don't wait for all images/ads to load
    
    try:
        if driver_path:
            service = Service(driver_path)
        else:
            service = Service(ChromeDriverManager().install())
    except Exception as e:
        print(f"  -> Warning: ChromeDriverManager failed ({e}), trying cached fallback...")
        fallback_path = "/home/alex-shanov/.wdm/drivers/chromedriver/linux64/144.0.7559.96/chromedriver-linux64/chromedriver"
        if os.path.exists(fallback_path):
             service = Service(fallback_path)
             print(f"  -> Using cached driver at: {fallback_path}")
        else:
             raise e

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(20) # Strict timeout for initial page load
    driver.set_script_timeout(20)
    return driver

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text)).strip()

def random_sleep(min_seconds=2, max_seconds=5):
    """Sleeps for a random amount of time to mimic human behavior."""
    time.sleep(random.uniform(min_seconds, max_seconds))

def select_combined_filter(driver, element_id):
    """
    Selects the '---All' (Combined) option in a select element using JavaScript.
    This is the primary method because these filters are often hidden behind
    a menu or labeled as 'non-interactable' by Selenium.
    """
    try:
        # Check if the element exists in the DOM first
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, element_id))
        )
        
        js_code = f"""
        var select = document.getElementById('{element_id}');
        if (select) {{
            var opts = Array.from(select.options);
            var allOpt = opts.find(o => o.value == '{ALL_OPTION_VALUE}');
            if (allOpt) {{
                select.value = '{ALL_OPTION_VALUE}';
                select.dispatchEvent(new Event('change', {{ 'bubbles': true }}));
                return true;
            }}
        }}
        return false;
        """
        result = driver.execute_script(js_code)
        
        if result:
            print(f"  -> Successfully selected 'Combined' (---All) for #{element_id} via JS")
            time.sleep(1.5) # Wait for AJAX reload
            return True
        else:
            # Not an error if the meet simply doesn't have multiple levels/sessions
            return False
            
    except Exception:
        # Silently fail if the element doesn't exist; some meets don't have all filters
        return False

def extract_table_raw(driver, meet_name):
    """
    Extracts the updated table data as-is, preserving original headers.
    """
    try:
        # Wait for the table to have rows
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.table tbody tr"))
        )
    except:
        print("  -> DEBUG: Timeout waiting for table rows.")
        # proceed to try extraction anyway, maybe it's just slow or different selector
        
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    # Try multiple selectors for the table
    table = soup.find("table", class_="table") 
    if not table:
        table = soup.find("table") # Fallback to any table
        
    if not table:
        print("  -> DEBUG: No <table> tag found in page source.")
        return None

    rows = table.find_all("tr")
    if not rows: 
        print("  -> DEBUG: Table found but no <tr> rows.")
        return None
        
    # Extract headers
    header_row = rows[0]
    cols = header_row.find_all(["th", "td"])
    headers = [clean_text(col.get_text()) for col in cols]
    
    # Extract data rows
    data = []
    for tr in rows[1:]:
        cells = tr.find_all("td")
        if not cells: continue
        
        row_dict = {"Meet": meet_name} # Add meet name metadata
        
        for i, cell in enumerate(cells):
            if i < len(headers):
                cell_text = cell.get_text(" ", strip=True) 
                row_dict[headers[i]] = clean_text(cell_text)
                
        data.append(row_dict)
        
    return pd.DataFrame(data)

def process_meet(driver, meet_id, meet_name, index, total):
    url = f"https://www.meetscoresonline.com/Results/{meet_id}"
    print(f"[{index}/{total}] Processing Meet: {meet_name} ({meet_id}) -> {url}")
    
    max_retries = 3
    for attempt in range(max_retries + 1):
        try:
            # Random jitter before request
            random_sleep(2, 5)
            
            driver.get(url)
            # time.sleep(3) # Let page load (handled by wait+jitter now)
            
            # 1. Handle Popups (MSO ALL ACCESS)
            try:
                close_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'modal')]//button[@class='close'] | //a[contains(text(), 'Close')] | //i[contains(@class, 'fa-times')]"))
                )
                close_btn.click()
                print("  -> Closed popup")
                time.sleep(1)
            except:
                pass 
                
            # 2. Select "Combined" filters
            # Session first, then Level (order might matter for AJAX)
            select_combined_filter(driver, "session_dd")
            select_combined_filter(driver, "level_dd")
            select_combined_filter(driver, "division_dd") # Try division too just in case
            
            time.sleep(2) # Final wait for table update
            
            # 3. Extract Raw Table
            # soup = BeautifulSoup(driver.page_source, "html.parser") # Handled inside extract_table_raw
            df = extract_table_raw(driver, meet_name)
            
            if df is None or df.empty:
                msg = "No results found"
                print(f"  -> {msg}")
                return False, msg
                
            print(f"  -> Extracted {len(df)} rows. Columns: {list(df.columns)}")
            
            # 4. Save Raw CSV
            filename = f"{meet_id}_mso.csv"
            os.makedirs(OUTPUT_FOLDER, exist_ok=True)
            filepath = os.path.join(OUTPUT_FOLDER, filename)
            df.to_csv(filepath, index=False)
            print(f"  -> Saved raw data to {filepath}")
            return True, f"Saved {len(df)} rows"

        except Exception as e:
            if attempt < max_retries:
                wait_time = 10 * (attempt + 1)
                print(f"  -> Error: {e}. Retrying in {wait_time}s (Attempt {attempt+1}/{max_retries})...")
                time.sleep(wait_time)
            else:
                msg = f"Error: {e} (Max retries reached)"
                print(f"  -> {msg}")
                return False, msg

def main():
    if not os.path.exists(INPUT_MANIFEST):
        print(f"Manifest {INPUT_MANIFEST} not found.")
        return

    manifest = pd.read_csv(INPUT_MANIFEST)
    total = len(manifest)
    driver = setup_driver()
    
    for i, (_, row) in enumerate(manifest.iterrows(), 1):
        if DEBUG_LIMIT > 0 and i > DEBUG_LIMIT:
            break
            
        meet_id = str(row['MeetID'])
        meet_name = row['MeetName']
        
        # --- SKIP LOGIC ---
        filename = f"{meet_id}_mso.csv"
        filepath = os.path.join(OUTPUT_FOLDER, filename)
        if os.path.exists(filepath):
            print(f"[{i}/{total}] Skipping already scraped meet: {meet_name} ({meet_id})")
            continue
        # ------------------
        
        process_meet(driver, meet_id, meet_name, i, total)
            
    driver.quit()

if __name__ == "__main__":
    main()
