import kscore_scraper
import sys

# Test with a known failing meet
# Indiscovered_meet_ids_kscore.csv, rows are: MeetID, MeetName
# meet_id = 'kscore_yukon_champs25' # Found in grep
# BUT, is that the ID used in the URL?
# Let's inspect discovered_meet_ids_kscore.csv head first to be sure of ID column.

def main():
    from webdriver_manager.chrome import ChromeDriverManager
    driver_path = ChromeDriverManager().install()
    print(f"DEBUG: Using driver path: {driver_path}")
    
    meet_id = "kscore_clubaviva_avivacup21" 
    meet_name = "Aviva Cup 2021"
    output_dir = "CSVs_kscore_final"
    
    print(f"DEBUG: Starting scrape for {meet_id}...")
    success, count = kscore_scraper.scrape_kscore_meet(meet_id, meet_name, output_dir, driver_path=driver_path)
    print(f"DEBUG: Finished. Success={success}, Count={count}")

if __name__ == "__main__":
    main()
