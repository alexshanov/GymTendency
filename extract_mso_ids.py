import bs4
import csv
import re
import os

HTML_FILE = "meets_meetscoreonline.html"
OUTPUT_FILE = "discovered_meet_ids_mso.csv"

def extract_ids():
    if not os.path.exists(HTML_FILE):
        print(f"Error: {HTML_FILE} not found.")
        return

    with open(HTML_FILE, "r", encoding="utf-8") as f:
        soup = bs4.BeautifulSoup(f, "html.parser")

    # Find all meet containers
    # <div class="meet-container clear status-3" data-meetid="35530" ...>
    meet_divs = soup.find_all("div", class_="meet-container")

    extracted_data = []
    
    print(f"Found {len(meet_divs)} meet containers.")

    for div in meet_divs:
        meet_id = div.get("data-meetid")
        state = div.get("data-state")
        filter_text = div.get("data-filter-by")
        
        # Extract name from the <h3><a> tag inside
        # <h3><a href="/R35530">2025 BTH AAU</a></h3>
        name_tag = div.find("h3")
        meet_name = name_tag.get_text(strip=True) if name_tag else "Unknown Meet"
        
        # Find date
        # <div class="meet-dates float-end">Jan 11, 2026</div>
        date_div = div.find("div", class_="meet-dates")
        date_text = date_div.get_text(strip=True) if date_div else ""

        if meet_id:
            extracted_data.append([meet_id, meet_name, date_text, state, filter_text])

    # Write to CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["MeetID", "MeetName", "Date", "State", "FilterText"])
        writer.writerows(extracted_data)

    print(f"Successfully extracted {len(extracted_data)} meets to {OUTPUT_FILE}.")

if __name__ == "__main__":
    extract_ids()
