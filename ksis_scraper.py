import os
import time
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import io
import traceback

# --- CONFIGURATION ---
KSIS_MEETS_CSV = "discovered_meet_ids_ksis.csv"
OUTPUT_DIR_KSIS = "CSVs_ksis_messy"
FINAL_DIR_KSIS = "CSVs_ksis_final"
BASE_URL = "https://rgform.eu/"

DEBUG_LIMIT = 0

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text)).strip()

def scrape_ksis_meet(meet_id, meet_name, output_dir):
    """
    Scrapes a single KSIS meet by:
    1. Fetching the main page to find session IDs (id_sut).
    2. Iterating through each session and fetching the AJAX result content.
    3. Parsing the table and saving to CSV.
    """
    print(f"--- Processing KSIS meet: {meet_name} ({meet_id}) ---")
    
    main_url = f"{BASE_URL}resultx.php?id_prop={meet_id}"
    
    try:
        # 1. Fetch Main Page
        response = requests.get(main_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 2. Extract Meet Dates/Year
        # Usually in <div class="col-sm-12"><h3>Meet Name</h3><h4>Dates</h4></div>
        meet_year = ""
        jumbo = soup.find('div', class_='jumbotron')
        if jumbo:
            h4 = jumbo.find('h4')
            if h4:
                date_str = clean_text(h4.text)
                print(f"  -> Found dates: {date_str}")
                # Parse year from "12.12.2025 - 14.12.2025"
                year_match = re.search(r'(20\d{2})', date_str)
                if year_match:
                    meet_year = year_match.group(1)
                    print(f"  -> Detected year: {meet_year}")
        
        # 3. Extract Sessions from Dropdown
        # Selector: select#id_sut > option
        session_select = soup.find('select', id='id_sut')
        if not session_select:
            print("  -> Warning: Could not find session dropdown (#id_sut).")
            return False

        sessions = []
        for option in session_select.find_all('option'):
            val = option.get('value')
            if val and val != '-1':
                sessions.append({
                    'id': val,
                    'name': clean_text(option.text),
                    'nac': option.get('data-nac', '')
                })
        
        print(f"  -> Found {len(sessions)} sessions/categories.")
        
        if not sessions:
            return False

        files_saved = 0
        os.makedirs(output_dir, exist_ok=True)

        for session in sessions:
            s_id = session['id']
            s_name = session['name']
            
            print(f"    -> Scraping session: {s_name} (ID: {s_id})")
            
            # 3. Construct AJAX URL
            # The page loads a filter menu first, which then calls:
            # load_result_total_ksismg_art.php?lang=en&id_prop=...&id_sut=...&rn=&mn=&state=&age_group=&award=&nacinie=
            ajax_url = f"{BASE_URL}load_result_total_ksismg_art.php"
            params = {
                'lang': 'en',
                'id_prop': meet_id,
                'id_sut': s_id,
                'rn': '',
                'mn': '',
                'state': '',
                'age_group': '',
                'award': '',
                'nacinie': ''
            }
            
            try:
                # Add delay to be polite
                time.sleep(1)
                
                res_resp = requests.get(ajax_url, params=params, timeout=30)
                res_resp.raise_for_status()
                
                # 4. Parse Table Manually to handle rowspan=2 and floating TDs
                res_soup = BeautifulSoup(res_resp.text, 'lxml')
                table = res_soup.find('table')
                
                if not table:
                    print(f"      -> No table found for session {s_id}")
                    continue
                
                tbody = table.find('tbody')
                if not tbody:
                    print(f"      -> No tbody found for session {s_id}")
                    continue

                # Identify apparatuses from thead
                thead = table.find('thead')
                apparatuses = []
                if thead:
                    header_imgs = thead.find_all('img')
                    for img in header_imgs:
                        src = img.get('src', '')
                        name = re.sub(r'^\./|\.png$', '', src)
                        apparatuses.append(name)
                
                if not apparatuses:
                    apparatuses = ["App1", "App2", "App3", "App4", "App5", "App6"]

                data = []
                for r1 in tbody.find_all('tr', recursive=False):
                    tds1 = r1.find_all('td', recursive=False)
                    if len(tds1) < 4:
                        continue
                    
                    # Search for component TDs in following siblings
                    tds2 = []
                    for sibling in r1.next_siblings:
                        if sibling.name == 'tr':
                             # If we find an empty TR (often at the end of an athlete group in KSIS)
                             # or next athlete's TR, we stop.
                             break
                        if sibling.name == 'td':
                             tds2.append(sibling)
                    
                    row_data = {}
                    try:
                        row_data['Place'] = clean_text(tds1[0].text)
                        row_data['Country'] = clean_text(tds1[1].text)
                        
                        # Name/Club is complex: 3 - <a...>Mihailuk Maksim</a><br>Gymnastics Mississauga
                        name_cell = tds1[2]
                        full_text = clean_text(name_cell.text)
                        
                        # Extract Rank from "3 - ..."
                        rank_match = re.match(r'^(\d+)\s*-\s*', full_text)
                        row_data['Athlete_Rank'] = rank_match.group(1) if rank_match else ""
                        
                        # Extract Name from <a> link or similar
                        anchor = name_cell.find('a')
                        row_data['Name'] = clean_text(anchor.text) if anchor else full_text
                        
                        # Extract Club from text after <br> or similar
                        # In the cell: "3 - Mihailuk Maksim\nGymnastics Mississauga"
                        parts = name_cell.get_text(separator="\n").split("\n")
                        row_data['Club'] = clean_text(parts[-1]) if len(parts) > 1 else ""
                        
                        row_data['Year'] = clean_text(tds1[3].text)
                        row_data['AA_Score'] = clean_text(tds1[-1].text)
                        
                        r2_idx = 0
                        for i, app in enumerate(apparatuses):
                            # Summary in r1
                            r1_td_idx = 4 + i
                            if r1_td_idx >= len(tds1) - 1: break
                            
                            summary = clean_text(tds1[r1_td_idx].text)
                            
                            # Components in tds2
                            d_val = clean_text(tds2[r2_idx].text) if r2_idx < len(tds2) else ""
                            e_val = clean_text(tds2[r2_idx+1].text) if r2_idx+1 < len(tds2) else ""
                            b_val = clean_text(tds2[r2_idx+2].text) if r2_idx+2 < len(tds2) else ""
                            nd_val = clean_text(tds2[r2_idx+3].text) if r2_idx+3 < len(tds2) else ""
                            
                            row_data[f"{app}_Total"] = summary
                            row_data[f"{app}_D"] = d_val
                            row_data[f"{app}_E"] = e_val
                            row_data[f"{app}_Bonus"] = b_val
                            row_data[f"{app}_ND"] = nd_val
                            
                            r2_idx += 4
                            
                        data.append(row_data)
                    except Exception as e:
                        # traceback.print_exc()
                        pass

                if not data:
                    print(f"      -> No data extracted for session {s_id}")
                    continue

                df = pd.DataFrame(data)
                
                # 5. Add Metadata & Save
                df['MeetID'] = meet_id
                df['MeetName'] = meet_name
                df['MeetYear'] = meet_year
                df['Session'] = s_name
                df['SessionID'] = s_id
                
                # Sanitize filename
                safe_sname = re.sub(r'[^a-zA-Z0-9_\-]', '_', s_name)
                filename = f"{meet_id}_ksis_{s_id}_{safe_sname}.csv"
                filepath = os.path.join(output_dir, filename)
                
                df.to_csv(filepath, index=False)
                files_saved += 1
                print(f"      -> Saved {filename} ({len(df)} athletes)")
                
            except Exception as e:
                print(f"      -> Error fetching session {s_id}: {e}")
                traceback.print_exc()
                continue
                
        return files_saved > 0

    except Exception as e:
        print(f"  -> Critical error scraping meet {meet_id}: {e}")
        traceback.print_exc()
        return False

def main():
    # Placeholder for local testing
    if not os.path.exists(KSIS_MEETS_CSV):
        print(f"Manifest {KSIS_MEETS_CSV} not found. Creating example...")
        with open(KSIS_MEETS_CSV, 'w') as f:
            f.write("MeetID,MeetName\n9143,1st Ontario Cup MAG\n")
            
    try:
        meets_df = pd.read_csv("debug_manifest_8343.csv")
    except:
        return

    for _, row in meets_df.iterrows():
        mid = str(row['MeetID'])
        mname = row['MeetName']
        scrape_ksis_meet(mid, mname, OUTPUT_DIR_KSIS)

if __name__ == "__main__":
    main()
