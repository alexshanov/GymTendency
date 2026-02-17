import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import sys
from datetime import datetime

# URL for "Events" on KSIS (rgform.eu)
EVENTS_URL = "https://rgform.eu/menu.php?akcia=KS"
BASE_URL = "https://rgform.eu/"

def scrape_ksis_events():
    print(f"--- Fetching KSIS Events from {EVENTS_URL} ---")
    try:
        response = requests.get(EVENTS_URL, timeout=30)
        response.raise_for_status()
        html_content = response.text
    except Exception as e:
        print(f"Error fetching KSIS events: {e}")
        return None

    soup = BeautifulSoup(html_content, 'html.parser')
    
    # The events seem to be in a table or list. 
    # Based on typical PHP sites, let's look for tables with class 'table' or similar.
    # Or links to 'event.php?id_prop=...'
    
    events = []
    
    # Find all links that look like event links
    # href="event.php?id_prop=9143"
    links = soup.find_all('a', href=re.compile(r'event\.php\?id_prop=\d+'))
    
    print(f"Found {len(links)} potential event links.")
    
    seen_ids = set()
    
    for link in links:
        href = link.get('href')
        match = re.search(r'id_prop=(\d+)', href)
        if not match:
            continue
            
        meet_id = match.group(1)
        if meet_id in seen_ids:
            continue
            
        meet_name_raw = link.get_text(strip=True)
        if not meet_name_raw:
            continue
            
        # Sometimes the link is inside a TD, and other details are in sibling TDs
        # Let's try to find the row (TR) this link belongs to, to get date/loc
        row = link.find_parent('tr')
        
        date_str = "N/A"
        location = "N/A"
        year = "N/A"
        
        if row:
            cols = row.find_all('td')
            # Table structure assumption needed. 
            # Usually: Date | Event Name | Location | ...
            # Let's simple-print the row content to debug if needed, but for now apply heuristics.
            
            # Heuristic: search for date-like string in columns
            for col in cols:
                txt = col.get_text(strip=True)
                # Look for Year
                if re.search(r'20\d{2}', txt):
                     # Likely the date column
                     date_str = txt
                     break
        
        # Extract year from date_str
        ym = re.search(r'(20\d{2})', date_str)
        if ym:
            year = ym.group(1)
        
        # Append
        events.append({
            "Source": "cases", # User calls it cases (KSIS)
            "MeetID": meet_id,
            "MeetName": meet_name_raw,
            "Dates": date_str,
            "Location": location,
            "Year": year
        })
        seen_ids.add(meet_id)
        
    return events

if __name__ == "__main__":
    discovered = scrape_ksis_events()
    
    if discovered:
        df = pd.DataFrame(discovered)
        # Filter for relevant meets if possible? 
        # For now, keep all, the orchestrator/scraper can decide to skip if wrong type.
        # But user specifically mentioned "cases" involves KSIS.
        
        output_file = "discovered_meet_ids_ksis.csv"
        df.to_csv(output_file, index=False)
        print(f"\nSaved {len(df)} meets to {output_file}")
        print(df.head())
    else:
        print("No meets found.")
