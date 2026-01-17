# Gymnastics Data Pipeline - Metadata Capture Specification

> **Purpose**: Reference document for source compliance and data capture standards.  
> **Last Updated**: 2026-01-17

---

## Standard Output Columns

All scrapers produce CSVs with these **service columns**:
```
Name, Club, Level, Age, Prov, Age_Group, Meet, Group
```

Followed by **result columns** in FIG Olympic order as triplets:

**WAG**: `Vault → Uneven_Bars → Beam → Floor → AllAround`  
**MAG**: `Floor → Pommel_Horse → Rings → Vault → Parallel_Bars → High_Bar → AllAround`

Each apparatus outputs: `Result_{Apparatus}_D`, `Result_{Apparatus}_Score`, `Result_{Apparatus}_Rnk`

---

## Per-Source Metadata Capture

### K-Score (`kscore_scraper.py`)

| Field | Source | Notes |
|-------|--------|-------|
| **Meet Name** | Manifest | `discovered_meet_ids_kscore.csv` |
| **Date** | Manifest | Parsed to `comp_year` |
| **Location** | Manifest | State/Province |
| **Session** | Page | From `#sel-sess` dropdown text |
| **Level** | Page | Extracted from category name if contains "Level" or "CCP" |
| **Age_Group** | Page | From category info object |

---

### LiveMeet (`livemeet_scraper.py`)

| Field | Source | Notes |
|-------|--------|-------|
| **Meet Name** | Page | `.TournamentHeading` element |
| **Date** | Manifest | `discovered_meet_ids_livemeet.csv` |
| **Location** | Manifest | From manifest |
| **Group** | Page | Event group name (`.liCategory` text) |
| **Level** | Page | Extracted from group name via regex |
| **Age_Group** | Page | From `.rpSubTitle` element |

---

### MSO (`mso_scraper.py`)

| Field | Source | Notes |
|-------|--------|-------|
| **Meet Name** | Manifest | `discovered_meet_ids_mso.csv` |
| **Date** | Manifest | Raw string (e.g., "Jan 11, 2026") |
| **Location** | ❌ Not captured | Could be added from page header |
| **Session** | Table | Column 2 (Sess) |
| **Level** | Table | Column 3 (Lvl) |
| **Age_Group/Division** | Table | Column 4 (Div) |

---

## Database Schema Reference

### Meets Table
```sql
CREATE TABLE Meets (
    meet_db_id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,        -- 'kscore', 'livemeet', 'mso'
    source_meet_id TEXT NOT NULL,
    name TEXT,
    start_date_iso TEXT,         -- Full date when available
    comp_year INTEGER,           -- Always populated (parsed from date)
    location TEXT,
    UNIQUE(source, source_meet_id)
);
```

### Results Table
```sql
CREATE TABLE Results (
    result_id INTEGER PRIMARY KEY,
    meet_db_id INTEGER NOT NULL,
    athlete_id INTEGER NOT NULL,
    apparatus_id INTEGER NOT NULL,
    level TEXT,                  -- From scraper
    age REAL,                    -- From scraper (if available)
    province TEXT,               -- From scraper (if available)
    score_d REAL,                -- D-Score (NULL if not available)
    score_final REAL,            -- Final score
    score_text TEXT,             -- For non-numeric scores
    rank_numeric INTEGER,
    rank_text TEXT,
    details_json TEXT            -- Extra metadata bag
);
```

---

## Compliance Checklist

When adding a new data source, ensure:
- [ ] ID extraction script creates manifest with `MeetID`, `MeetName`, `Date`, `Location`
- [ ] Scraper outputs standard service columns
- [ ] Result columns are triplets in Olympic apparatus order
- [ ] Apparatus names use underscores (e.g., `Uneven_Bars`, `Pommel_Horse`)
- [ ] Loader maps `source` field correctly
- [ ] `comp_year` is populated (directly or parsed from date)
