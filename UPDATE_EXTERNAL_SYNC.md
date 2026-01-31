# How to Update Your External Sync Script

Since the `sync_to_supabase_robust.py` script now generates SQL files instead of syncing directly, you should update your external sync process to:

1.  **Locate the SQL Files:** This script will output `Gold_Results_MAG.sql`, `Gold_Results_MAG_Filtered_L1.sql`, and `Gold_Results_MAG_Filtered_L2.sql` in the same directory.
2.  **Execute via psql:** Instead of Python logic, your external script can simply run these SQL files against your Supabase database using `psql`.

**Example Command:**
```bash
# Set your database URL in the environment or replace directly
export DATABASE_URL="postgres://user:password@hostname:5432/postgres"

# Execute the files
psql $DATABASE_URL -f Gold_Results_MAG.sql
psql $DATABASE_URL -f Gold_Results_MAG_Filtered_L1.sql
psql $DATABASE_URL -f Gold_Results_MAG_Filtered_L2.sql
```

**Why this is better:**
- **Performance:** `psql` executing a file is much faster than row-by-row Python inserts.
- **Reliability:** The files use `ON CONFLICT DO NOTHING`, making them safe to re-run anytime.
