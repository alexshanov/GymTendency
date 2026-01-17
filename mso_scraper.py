
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
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
DEBUG_LIMIT = 1  # 35631 35486 33704  # Process only 1 meet for testing
OUTPUT_FOLDER = "CSVs_mso_final"
INPUT_MANIFEST = "discovered_meet_ids_mso.csv"

# Ensure output directory exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def setup_driver():
    options = Options()
    # options.add_argument("--headless")  # Comment out for visual debugging if needed
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text)).strip()

def extract_score(cell):
    """
    Extracts score from MSO format:
    <td ...>
       <span class="small place">2</span>
       <sup>500</sup>
       <span class="score">13</span>
    </td>
    Returns: (score_final, score_d, rank)
    """
    if not cell: return None, None, None
    
    # Rank
    rank_span = cell.find("span", class_="place")
    rank = clean_text(rank_span.get_text()) if rank_span else ""
    
    # Score Integer
    score_span = cell.find("span", class_="score")
    score_int = clean_text(score_span.get_text()) if score_span else ""
    
    # Score Decimal
    sup = cell.find("sup")
    score_dec = clean_text(sup.get_text()) if sup else ""
    
    # Construct final score - Logic:
    # If both int and dec exist: 13.500
    # If only int exists: 13 (could be just int part or full score if no decimal logic used)
    # If no standard classes found, try getting direct text (some meets might just have text)
    
    if score_int and score_dec:
        final_score = f"{score_int}.{score_dec}"
    elif score_int:
        final_score = score_int
    elif not score_int and not score_dec:
        # Fallback: Just get text if structure is different
        # Remove rank text to avoid pollution if it's in the same cell but not in span
        # But usually rank is in span class='place'
        text = cell.get_text(separator=" ", strip=True)
        # Remove rank if we found it
        if rank:
            text = text.replace(rank, "").strip()
        final_score = text
    else:
        final_score = ""
        
    return final_score, "", rank

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
            pass # No popup or couldn't close
            
        # 2. Select "All" / "Combined" Sessions if available
        # MSO navigation is complex, often loaded via AJAX. 
        # We will attempt to find a "View All" or simply scrape what is visible 
        # if the default view is comprehensive (which it often is for MSO result pages).
        # A more robust approach for MSO is to verify if we are seeing a partial list.
        # For now, we scrape the main table loaded.
        
        # 3. Extract Table
        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.find("table", class_="table")
        
        if not table:
            print("  -> No results table found.")
            return False

        rows = table.find_all("tr")
        print(f"  -> Found {len(rows)} rows (including header)")
        
        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        
        # Identify standard columns vs events
        # Typical header: Gimnast, Team, Sess, Lvl, Div, FLR, PH, ...
        
        data_records = []
        
        for tr in rows[1:]:
            cells = tr.find_all("td")
            if not cells: continue
            
            # Helper to safely get index
            def get_val(idx):
                return clean_text(cells[idx].get_text()) if idx < len(cells) else ""
            
            # Map standard columns based on header index or position
            # MSO is usually consistent
            # 0: Gymnast, 1: Team, 2: Sess, 3: Lvl, 4: Div
            
            name = get_val(0)
            club = get_val(1)
            sess = get_val(2)
            level = get_val(3)
            division = get_val(4)
            
            # Columns 5+ are events
            # Need to map header name to score extraction
            for i, header_col in enumerate(headers):
                if i < 5: continue # Skip info columns
                
                event_name = header_col
                final_score, score_d, rank = extract_score(cells[i])
                
                if final_score:
                    data_records.append({
                        "Include": "ok",
                        "Name": name,
                        "Club": club,
                        "Level": level,
                        "Age": "", # Not explicit
                        "Prov": "", # Not explicit
                        "Age_Group": division, # Best proxy
                        "Meet": meet_name,
                        "Group": f"{sess} - {division}",
                        "Apparatus": event_name,
                        "Score": final_score,
                        "D_Score": score_d,
                        "Rank": rank
                    })

        if not data_records:
            print("  -> No data records extracted.")
            return False
            
        df = pd.DataFrame(data_records)
        
        # --- STANDARDIZATION ---
        # 1. Standard Service Columns
        required_cols = ['Name', 'Club', 'Level', 'Age', 'Prov', 'Age_Group', 'Meet', 'Group']
        for col in required_cols:
            if col not in df.columns:
                df[col] = "" # Fill missing
                
        # 2. Apparatus Mapping
        event_map = {
            "FLR": "Floor", "FX": "Floor", "FLOOR": "Floor",
            "PH": "PommelHorse", "POMMEL HORSE": "PommelHorse",
            "SR": "Rings", 
            "VT": "Vault", "VAULT": "Vault",
            "PB": "ParallelBars", "PARALLEL BARS": "ParallelBars",
            "HB": "HighBar", "HIGH BAR": "HighBar",
            "AA": "AllAround", "ALL AROUND": "AllAround",
            "UB": "UnevenBars", "BARS": "UnevenBars", "UNEVEN BARS": "UnevenBars",
            "BB": "Beam", "Beam": "Beam", "Balance Beam": "Beam", "BEAM": "Beam"
        }
        df['Apparatus'] = df['Apparatus'].map(lambda x: event_map.get(x, x))
        
        # 3. Pivot to standard format (One row per athlete, Result_Columns)
        # However, our other scrapers produce ONE ROW PER ATHLETE with multiple Result_XX columns.
        # This records format is Long. We need wide.
        
        # Let's pivot
        df_wide = df.pivot_table(
            index=['Name', 'Club', 'Level', 'Age', 'Prov', 'Age_Group', 'Meet', 'Group'],
            columns='Apparatus',
            values=['Score', 'D_Score', 'Rank'],
            aggfunc='first'
        ).reset_index()
        
        # Flatten MultiIndex columns
        new_cols = []
        for col in df_wide.columns:
            if col[0] in ['Score', 'D_Score', 'Rank']:
                # e.g. Result_Floor_Score, Result_Floor_Rank
                metric = col[0]
                apparatus = col[1]
                if metric == 'Score':
                    new_cols.append(f"Result_{apparatus}_Score")
                elif metric == 'D_Score':
                    new_cols.append(f"Result_{apparatus}_D_Score")
                elif metric == 'Rank':
                    new_cols.append(f"Result_{apparatus}_Rank")
            else:
                new_cols.append(col[0]) # Keep index cols as is
        
        df_wide.columns = new_cols
        
        # Save
        filename = f"{meet_id}_mso.csv"
        filepath = os.path.join(OUTPUT_FOLDER, filename)
        df_wide.to_csv(filepath, index=False)
        print(f"  -> Saved {len(df_wide)} athletes to {filepath}")
        return True

    except Exception as e:
        print(f"  -> Error: {e}")
        return False

def main():
    if not os.path.exists(INPUT_MANIFEST):
        print(f"Manifest {INPUT_MANIFEST} not found.")
        return

    manifest = pd.read_csv(INPUT_MANIFEST)
    
    # Filter for 2024 testing if needed, or just take first manageable one
    # User requested: "not take 2026 meets that are still in the future, find a 2024"
    # But our manifest has 2026 dates (which are actually completed meets per metadata).
    # We will prioritize a known "Meet Complete" one.
    
    driver = setup_driver()
    
    count = 0
    for _, row in manifest.iterrows():
        if DEBUG_LIMIT > 0 and count >= DEBUG_LIMIT:
            break
            
        meet_id = str(row['MeetID'])
        meet_name = row['MeetName']
        
        # Skip if future? (Already filtered by scraper feasibility usually, but let's be safe)
        if process_meet(driver, meet_id, meet_name):
            count += 1
            
    driver.quit()

if __name__ == "__main__":
    main()
