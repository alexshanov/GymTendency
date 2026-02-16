
import pandas as pd
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

def standardize_kscore_columns(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    header_rows = soup.select('thead > tr')
    if len(header_rows) < 2: return pd.DataFrame()
    event_names = [img.get('alt', 'Unknown') for img in header_rows[0].select('img.apparatuslogo')]
    sub_header_cells = header_rows[1].find_all(['th', 'td'])
    info_headers_raw = [
        cell.get_text(strip=True) for cell in sub_header_cells 
        if 'display: none;' not in cell.get('style', '') and cell.get_text(strip=True) not in ['D', 'Score', 'Rk']
    ]
    final_columns = []
    final_columns.extend(info_headers_raw)
    for event in event_names:
        clean_event = event.replace(' ', '_').replace('-', '_')
        final_columns.extend([f"Result_{clean_event}_D", f"Result_{clean_event}_Score", f"Result_{clean_event}_Rnk"])
    data_rows = soup.select('tbody > tr')
    all_row_data = []
    for row in data_rows:
        cells = row.find_all('td')
        row_data = [cell.get_text(strip=True) for cell in cells if 'display: none;' not in cell.get('style', '')]
        all_row_data.append(row_data)
    if not all_row_data: return pd.DataFrame()
    df = pd.DataFrame(all_row_data)
    if len(final_columns) == df.shape[1]:
        df.columns = final_columns
        return df
    return df

def debug_jaydon():
    meet_id = "gcg_ec24"
    base_url = f"https://live.kscore.ca/results/{meet_id}"
    print(f"Investigating Jaydon scores at {base_url}")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        driver.get(base_url)
        time.sleep(5)
        
        # Get all sessions
        sess_select = driver.find_element(By.ID, "sel-sess")
        sessions = [option.text for option in sess_select.find_elements(By.TAG_NAME, "option")]
        
        for sess_text in sessions:
            if not sess_text: continue
            print(f"--- Checking Session: {sess_text} ---")
            sess_select = driver.find_element(By.ID, "sel-sess")
            for op in sess_select.find_elements(By.TAG_NAME, "option"):
                if op.text == sess_text:
                    op.click()
                    break
            time.sleep(3)
            
            # Get all categories for this session
            cat_select = driver.find_element(By.ID, "sel-cat")
            cats = [option.text for option in cat_select.find_elements(By.TAG_NAME, "option")]
            
            for cat_text in cats:
                if not cat_text or cat_text == "Select Category": continue
                print(f"  -> Category: {cat_text}")
                cat_select = driver.find_element(By.ID, "sel-cat")
                for cop in cat_select.find_elements(By.TAG_NAME, "option"):
                    if cop.text == cat_text:
                        cop.click()
                        break
                time.sleep(3)
                
                try:
                    table_el = driver.find_element(By.CSS_SELECTOR, "table.a-results")
                    html_content = table_el.get_attribute('outerHTML')
                    df = standardize_kscore_columns(f"<html><body>{html_content}</body></html>")
                    
                    if df.empty: continue
                    
                    name_col = next((c for c in df.columns if c.lower() in ['name', 'athlete']), None)
                    if name_col:
                        match = df[df[name_col].str.contains('Silva', case=False, na=False)]
                        if not match.empty:
                            print(f"\n      FOUND JAYDON in {sess_text} / {cat_text}!")
                            print(match.iloc[0].to_string())
                except Exception as e:
                    print(f"      No table or error in category: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    debug_jaydon()
