# Data Refresh Workflow

> **Purpose**: Step-by-step process for adding new competition data to the pipeline.

---

## Quick Reference

```bash
# Full refresh (all sources)
./venv/bin/python extract_kscore_ids.py
./venv/bin/python extract_livemeet_ids.py
./venv/bin/python extract_mso_ids.py

./venv/bin/python kscore_scraper.py
./venv/bin/python livemeet_scraper.py
./venv/bin/python mso_scraper.py

./venv/bin/python create_db.py  # Only if schema changed!
./venv/bin/python kscore_load_data.py
./venv/bin/python livemeet_load_data.py
./venv/bin/python mso_load_data.py
```

---

## Detailed Steps

### 1. Update Meet Manifests
Download fresh HTML from each source and extract IDs:

```bash
# Save HTML pages manually from:
# - https://live.kscore.ca/results
# - https://www.sportzsoft.com/meet/meetWeb.dll/CompetitionList
# - https://www.meetscoresonline.com/Meets

# Then extract IDs
./venv/bin/python extract_kscore_ids.py
./venv/bin/python extract_livemeet_ids.py
./venv/bin/python extract_mso_ids.py
```

**Output**: Updated `discovered_meet_ids_*.csv` files

### 2. Run Scrapers
For incremental updates, set `DEBUG_LIMIT` in each scraper or filter manifest:

```bash
# Full scrape (can take hours)
./venv/bin/python kscore_scraper.py
./venv/bin/python livemeet_scraper.py
./venv/bin/python mso_scraper.py
```

**Output**: CSV files in `CSVs_*_final/` directories

### 3. Verify CSV Quality
Spot-check a few files:

```bash
# Check column structure
head -n 1 CSVs_mso_final/*.csv | head -n 5

# Check for empty files
find CSVs_*_final/ -empty -type f
```

### 4. Load to Database
**⚠️ IMPORTANT**: Loaders are additive. They won't duplicate existing meets.

```bash
# If schema changed, recreate DB first:
# ./venv/bin/python create_db.py

./venv/bin/python kscore_load_data.py
./venv/bin/python livemeet_load_data.py
./venv/bin/python mso_load_data.py
```

### 5. Verify Database
```bash
# Check record counts by source
sqlite3 gym_data.db "SELECT source, COUNT(*) FROM Meets GROUP BY source;"
sqlite3 gym_data.db "SELECT m.source, COUNT(r.result_id) FROM Results r JOIN Meets m ON r.meet_db_id = m.meet_db_id GROUP BY m.source;"

# Check latest meets by date
sqlite3 gym_data.db "SELECT source, name, comp_year FROM Meets ORDER BY comp_year DESC LIMIT 10;"
```

### 6. Update Gold Tables
```bash
./venv/bin/python create_gold_tables.py
```

---

## Incremental vs Full Refresh

| Scenario | Action |
|----------|--------|
| New season started | Full refresh recommended |
| Weekly update | Incremental: filter manifest to new meets only |
| Schema changed | Run `create_db.py` first (wipes data!) |
| Single meet reprocess | Delete specific CSVs, re-scrape, then load |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Scraper hangs | Check `DEBUG_LIMIT`, verify site is accessible |
| "Database not found" | Run `create_db.py` first |
| Duplicate records | Loaders skip existing (source, source_meet_id) |
| Empty CSVs | Check scraper logs for errors |
