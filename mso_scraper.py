
import os
import time
import pandas as pd
import re
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

def setup_driver():
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    # Anti-detection
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text)).strip()

def select_combined_filter(driver, element_id):
    """
    Selects the '---All' (Combined) option in a select element if it exists.
    Expected to work on hidden selects that sync with the UI.
    """
    try:
        # First try to find and click the hamburger menu if it's the first interaction
        # Browser check confirmed selector is 'a[href="#nav"]' or class based
        try:
            # Wait for hamburger to be present
            hamburger = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href="#nav"], .fa-bars, .navbar-toggler'))
            )
            # hamburger = driver.find_element(By.CSS_SELECTOR, 'a[href="#nav"], .fa-bars, .navbar-toggler')
            if hamburger.is_displayed():
                hamburger.click()
                time.sleep(1.0) # Short wait for menu expansion
                print("  -> Clicked hamburger menu")
        except:
            # print("  -> Hamburger menu not found or not clickable (might be open)")
            pass 
            
        # Locate the select element (hidden or visible)
        # Use simple ID presence as it might be hidden
        # Wait for it to be present in DOM
        select_elem = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, element_id))
        )
        
        # We need to interact with it differently if it's hidden or not
        # But dispatching 'change' event to the element usually works for MSO
        
        select = Select(select_elem)
        
        # Check if "---All" exists
        options = [o.get_attribute("value") for o in select.options]
        # print(f"  -> Options for {element_id}: {options}") 
        
        if ALL_OPTION_VALUE in options:
            select.select_by_value(ALL_OPTION_VALUE)
            # Critical: Dispatch change event for MSO ajax to trigger
            driver.execute_script("arguments[0].dispatchEvent(new Event('change', { 'bubbles': true }));", select_elem)
            print(f"  -> Selected 'Combined' (---All) for #{element_id}")
            time.sleep(2) # Wait for AJAX reload
            return True
        else:
            print(f"  -> '---All' option not found in #{element_id}")
            return False
            
    except Exception as e:
        print(f"  -> Filter error for {element_id}: {e}")
        # Try JS Fallback
        try:
            print(f"  -> Attempting JS fallback for {element_id}")
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
                print(f"  -> JS Fallback: Selected 'Combined' for #{element_id}")
                time.sleep(2)
                return True
            else:
                print(f"  -> JS Fallback: Failed (Element or Option not found)")
        except Exception as js_e:
            print(f"  -> JS Fallback Exception: {js_e}")

        # Save screenshot for debugging if both failed
        driver.save_screenshot(f"debug_error_{element_id}.png")
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

def process_meet(driver, meet_id, meet_name):
    url = f"https://www.meetscoresonline.com/Results/{meet_id}"
    print(f"Processing Meet: {meet_name} ({meet_id}) -> {url}")
    
    try:
        driver.get(url)
        time.sleep(3) # Let page load
        
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
            print("  -> No results found.")
            return False
            
        print(f"  -> Extracted {len(df)} rows. Columns: {list(df.columns)}")
        
        # 4. Save Raw CSV
        filename = f"{meet_id}_mso.csv"
        filepath = os.path.join(OUTPUT_FOLDER, filename)
        df.to_csv(filepath, index=False)
        print(f"  -> Saved raw data to {filepath}")
        return True

    except Exception as e:
        print(f"  -> Error: {e}")
        return False

def main():
    if not os.path.exists(INPUT_MANIFEST):
        print(f"Manifest {INPUT_MANIFEST} not found.")
        return

    manifest = pd.read_csv(INPUT_MANIFEST)
    driver = setup_driver()
    
    count = 0
    for _, row in manifest.iterrows():
        if DEBUG_LIMIT > 0 and count >= DEBUG_LIMIT:
            break
            
        meet_id = str(row['MeetID'])
        meet_name = row['MeetName']
        
        if process_meet(driver, meet_id, meet_name):
            count += 1
            
    driver.quit()

if __name__ == "__main__":
    main()
