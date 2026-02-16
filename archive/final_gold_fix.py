import sqlite3
import json
import os
import logging

DB_FILE = "gym_data.db"
AL_FILE = "level_aliases.json"

def run_fix():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    if not os.path.exists(AL_FILE):
        logging.error(f"Alias file {AL_FILE} not found")
        return

    with open(AL_FILE, 'r') as f:
        aliases = json.load(f)

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    logging.info("Standardizing levels in Results table...")
    for alias, canonical in aliases.items():
        cursor.execute("UPDATE Results SET level = ? WHERE level = ?", (canonical, alias))
    
    # Extra manual ones from user reports
    extras = {
        'SrNG': 'Senior Next Gen',
        'M1314': 'Aspire (13-14)',
        'Senior Next Gen': 'Senior Next Gen',
        'Next Gen 19-20': 'Senior Next Gen',
        'Next Gen 18-20': 'Senior Next Gen',
        'Sr NG': 'Senior Next Gen',
        'SNG': 'Senior Next Gen',
        'NGSR': 'Senior Next Gen',
        'Senior': 'Senior',
        'Elite': 'Elite',
        'Provincial 2A': 'P2A',
        'Provincial 2B': 'P2B',
        'Provincial 2C': 'P2C',
        'Provincial 2D': 'P2D',
        'Provincial 2E': 'P2E',
    }
    for alias, canonical in extras.items():
        cursor.execute("UPDATE Results SET level = ? WHERE level = ?", (canonical, alias))

    conn.commit()
    logging.info("Level standardization complete.")

    from load_orchestrator import refresh_gold_tables
    logging.info("Starting Gold table refresh with new deduplication logic...")
    refresh_gold_tables(conn)
    
    conn.close()
    logging.info("All tasks complete.")

if __name__ == "__main__":
    run_fix()
