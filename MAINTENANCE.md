# Maintenance Guide: Rescraping and Resetting Data

This guide explains how to properly reset the pipeline for both full and partial rescrapes.

## 1. Full Rescrape (The "Nuclear" Option)
Use this if you want to wipe the slate clean and re-download/re-ingest everything from scratch.

### Steps:
1.  **Reset Orchestrator Status**: Delete the status manifest to make the orchestrator "forget" everything it has scraped.
    ```bash
    ./reset_status.py
    ```
2.  **Clear Raw CSVs**: Empty the finalized CSV folders to prevent the loader from mixing old and new data.
    ```bash
    rm CSVs_Livemeet_final/*.csv
    rm CSVs_kscore_final/*.csv
    rm CSVs_mso_final/*.csv
    rm CSVs_ksis_final/*.csv
    ```
3.  **Clear Database (Optional but Recommended)**: Delete the database file.
    ```bash
    rm gym_data.db
    ```
    > [!IMPORTANT]
    > Only delete `gym_data.db` if you are okay with losing manually mapped athlete/club links that aren't yet in the `.json` alias files.
4.  **Run Orchestrator**:
    ```bash
    python3 orchestrator.py
    ```

---

## 2. Recent Rescrape (Timed Resets)
Use this if you only want to pick up updates for recent meets (e.g., meets from the last month that might have had score corrections).

### The "30 Days" Command:
To reset and re-scrape all meets from the **last 30 days**:
```bash
python3 reset_recent_meets.py --days 30
python3 orchestrator.py --days 30
```

> **Note**: The `--days` flag on `orchestrator.py` restricts the task list to only meets within that time window, preventing the scraper from processing historical backlog.

### Other Options:
- **Reset current year**: `python3 reset_recent_meets.py --years 1`
- **Default (Since Jan 1, 2026)**: `python3 reset_recent_meets.py`

### What this does:
1.  `reset_recent_meets.py` removes the `DONE` status for meets in the time window.
2.  `orchestrator.py --days 30` loads only meets from the last 30 days, then scrapes any without `DONE` status.

---

## 3. Handling Gaps
If you notice specific meets are missing apparatus scores (e.g., MAG missing Horizontal Bar), use the gap resetter:
```bash
python3 reset_gapped_meets.py
```
This will automatically identify meets with partial data and reset them in the database and manifest.
