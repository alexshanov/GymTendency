from bs4 import BeautifulSoup
import pandas as pd
import re
import os

html_file = "user_provided_ksis.html"
# The HTML was pasted in the prompt, I'll assume I have to read it from a file I'll create or just use the string.
# Since it's huge, I'll save it to a file first for processing.

def parse_ksis_table(html_content):
    soup = BeautifulSoup(html_content, 'lxml')
    table = soup.find('table', class_='table-condensed')
    if not table:
        print("No results table found.")
        return []
    
    meets = []
    rows = table.find_all('tr')[1:] # Skip header
    for row in rows:
        tds = row.find_all('td')
        if len(tds) < 4: continue
        
        event_td = tds[3]
        is_live = "LIVE" in event_td.text.upper()
        if is_live:
            print(f"Skipping LIVE event: {event_td.text.strip()}")
            continue
            
        link = event_td.find('a')
        if not link: continue
        
        href = link.get('href', '')
        # https://ksis.eu/resultx.php?id_prop=9143
        match = re.search(r'id_prop=(\d+)', href)
        if not match: continue
        
        meet_id = match.group(1)
        meet_name = link.text.strip()
        
        # Date is in first TD
        date_str = tds[0].text.strip()
        year_match = re.search(r'(20\d{2})', date_str)
        year = year_match.group(1) if year_match else ""
        
        meets.append({
            'MeetID': meet_id,
            'MeetName': meet_name,
            'Year': year,
            'Date': date_str,
            'Location': tds[4].text.strip() if len(tds) > 4 else ""
        })
    
    return meets

# Manifest file
manifest_file = "/home/alex-shanov/OneDrive/AnalyticsProjects/GymTendency/discovered_meet_ids_ksis.csv"

# Load existing
if os.path.exists(manifest_file):
    try:
        existing_df = pd.read_csv(manifest_file)
        existing_ids = set(existing_df['MeetID'].astype(str))
    except:
        existing_ids = set()
else:
    existing_ids = set()

# Process MAG HTML
if os.path.exists("user_provided_ksis.html"):
    with open("user_provided_ksis.html", "r") as f:
        html_content = f.read()
    print("Parsing MAG file...")
    mag_meets = parse_ksis_table(html_content)
else:
    mag_meets = []

# Process WAG HTML
if os.path.exists("user_provided_ksis_wag.html"):
    with open("user_provided_ksis_wag.html", "r") as f:
        html_content = f.read()
    print("Parsing WAG file...")
    wag_meets = parse_ksis_table(html_content)
else:
    wag_meets = []

new_meets = mag_meets + wag_meets

added_count = 0
rows_to_append = []

for m in new_meets:
    if m['MeetID'] not in existing_ids:
        rows_to_append.append(m)
        existing_ids.add(m['MeetID'])
        added_count += 1
    else:
        print(f"Meet {m['MeetID']} already exists, skipping.")

if rows_to_append:
    df_new = pd.DataFrame(rows_to_append)
    if os.path.exists(manifest_file):
        df_new.to_csv(manifest_file, mode='a', header=False, index=False)
    else:
        df_new.to_csv(manifest_file, index=False)
    print(f"Added {added_count} new meets to {manifest_file}")
else:
    print("No new meets found in the provided HTML.")

# Render for the user
print("\n--- Discovered Meet IDs ---")
for m in new_meets:
    print(f"ID: {m['MeetID']} | Year: {m['Year']} | Name: {m['MeetName']}")
