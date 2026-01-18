
import os
import time
import pandas as pd
import re
import csv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import sys

sys.path.insert(0, '.')
from etl_functions import is_tt_meet

# --- CONFIGURATION ---
BASE_URL = "https://www.meetscoresonline.com/Results.All"
OUTPUT_FILE = "discovered_meet_ids_mso.csv"

def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def parse_meets_from_page(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    meet_divs = soup.find_all("div", class_="meet-container")
    
    extracted_data = []
    for div in meet_divs:
        meet_id = div.get("data-meetid")
        state = div.get("data-state")
        filter_text = div.get("data-filter-by")
        
        name_tag = div.find("h3")
        meet_name = name_tag.get_text(strip=True) if name_tag else "Unknown Meet"
        
        date_div = div.find("div", class_="meet-dates")
        date_text = date_div.get_text(strip=True) if date_div else ""

        if meet_id:
            if is_tt_meet(meet_name) or is_tt_meet(filter_text):
                continue
            extracted_data.append({
                "MeetID": meet_id,
                "MeetName": meet_name,
                "Date": date_text,
                "State": state,
                "FilterText": filter_text
            })
    return extracted_data

def main():
    driver = setup_driver()
    all_meets = []
    seen_ids = set()

    try:
        print(f"📡 Navigating to {BASE_URL}...")
        driver.get(BASE_URL)
        time.sleep(3)  # Wait for initial load

        # 1. Find all season links in the toolbar
        # Buttons are in btn-group containers as per user's snippet
        season_elements = driver.find_elements(By.CSS_SELECTOR, "div.btn-group a.btn")
        season_links = []
        for el in season_elements:
            href = el.get_attribute("href")
            text = el.text.strip()
            if href and "Results.All" in href:
                season_links.append((text, href))
        
        print(f"🗓 Found {len(season_links)} seasons to scan.")

        # 2. Iterate through each season
        # We'll go backwards from newest if needed, or just all.
        for season_text, url in season_links:
            print(f"🔍 Scanning Season: {season_text} ({url})...")
            driver.get(url)
            time.sleep(2)
            
            # Scroll down to ensure all lazy-loaded content (if any) is present
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            meets = parse_meets_from_page(driver.page_source)
            new_count = 0
            for m in meets:
                if m["MeetID"] not in seen_ids:
                    all_meets.append(m)
                    seen_ids.add(m["MeetID"])
                    new_count += 1
            print(f"  -> Added {new_count} unique meets from this page.")

        # 3. Save to CSV
        if all_meets:
            df = pd.DataFrame(all_meets)
            # Ensure columns are in the right order
            df = df[["MeetID", "MeetName", "Date", "State", "FilterText"]]
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"✅ Successfully saved {len(all_meets)} meets to {OUTPUT_FILE}")
            
            # Delete the HTML file if it exists as requested
            html_file = "meets_meetscoreonline.html"
            if os.path.exists(html_file):
                os.remove(html_file)
                print(f"🗑 Deleted temporary file: {html_file}")
        else:
            print("❌ No meets were found.")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
