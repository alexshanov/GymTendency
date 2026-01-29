
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def fetch_livemeet_html():
    url = "https://sportzsoftlivemeet.com/find-meet/"
    output_file = "meets_livemeet.html"
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    print(f"--- Setting up Driver ---")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        print(f"--- Navigating to {url} ---")
        driver.get(url)
        time.sleep(5) # Allow initial load
        
        # Scroll to bottom a few times to trigger lazy loading
        print("--- Scrolling to trigger lazy load ---")
        last_height = driver.execute_script("return document.body.scrollHeight")
        for i in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            
        print("--- Capturing Page Source ---")
        html_content = driver.page_source
        
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)
            
        print(f"--- HTML saved to {output_file} ({len(html_content)} bytes) ---")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    fetch_livemeet_html()
