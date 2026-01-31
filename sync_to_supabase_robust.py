import sqlite3
import os
import requests
import json
import argparse

# Manual .env loading
def load_env_manual():
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    parts = line.strip().split("=", 1)
                    if len(parts) == 2:
                        os.environ[parts[0]] = parts[1]

load_env_manual()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
LOCAL_DB_PATH = "gym_data.db"

TABLE_MAP = {
    "L0": "Gold_Results_MAG",
    "L1": "Gold_Results_MAG_Filtered_L1",
    "L2": "Gold_Results_MAG_Filtered_L2"
}

def clean_record(record):
    """Clean record for Supabase ingestion."""
    # Remove local-only columns
    if 'source' in record:
        del record['source']
    
    # Numerics cleanup
    numeric_keys = [
        'fx_score', 'fx_d', 'ph_score', 'ph_d', 'sr_score', 'sr_d', 
        'vt_score', 'vt_d', 'pb_score', 'pb_d', 'hb_score', 'hb_d', 
        'aa_score', 'aa_d'
    ]
    for key in numeric_keys:
        if key in record:
            val = record[key]
            if val == '' or val is None:
                record[key] = None
            else:
                try:
                    record[key] = float(val)
                except (ValueError, TypeError):
                    record[key] = None
    return record

def export_sql(data, table_name, output_file):
    """Export data as SQL INSERT statements."""
    if not data:
        return
    
    keys = data[0].keys()
    columns = ", ".join(keys)
    
    with open(output_file, 'w') as f:
        print(f"Generating SQL export to {output_file}...")
        for record in data:
            values = []
            for k in keys:
                v = record[k]
                if v is None:
                    values.append("NULL")
                elif isinstance(v, (int, float)):
                    values.append(str(v))
                else:
                    # Escape single quotes
                    escaped_v = str(v).replace("'", "''")
                    values.append(f"'{escaped_v}'")
            
            val_str = ", ".join(values)
            f.write(f"INSERT INTO \"{table_name}\" ({columns}) VALUES ({val_str});\n")

def sync_results(level, target_table, format='api'):
    if not os.path.exists(LOCAL_DB_PATH):
        print(f"Error: Local database not found at {LOCAL_DB_PATH}")
        return

    source_table = TABLE_MAP.get(level)
    if not source_table:
        print(f"Error: Invalid level {level}")
        return

    conn = sqlite3.connect(LOCAL_DB_PATH)
    cursor = conn.cursor()

    try:
        print(f"Reading from {source_table}...")
        cursor.execute(f"SELECT * FROM {source_table}")
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        if not rows:
            print(f"No results found in {source_table}.")
            return

        print(f"Found {len(rows)} results. Cleaning and de-duplicating...")

        unique_results = {}
        for row in rows:
            record = dict(zip(columns, row))
            record = clean_record(record)

            # Unique key: (athlete_name, meet_name, year, level, age)
            unique_key = (
                str(record.get('athlete_name', '')).strip(),
                str(record.get('meet_name', '')).strip(),
                record.get('year'),
                str(record.get('level', '')).strip(),
                str(record.get('age', '')).strip()
            )

            if unique_key not in unique_results:
                unique_results[unique_key] = record

        results_data = list(unique_results.values())
        print(f"Prepared {len(results_data)} unique records.")

        if format == 'sql':
            output_file = f"{target_table}_{level}.sql"
            export_sql(results_data, target_table, output_file)
            print(f"SQL export complete: {output_file}")
            return

        # Supabase API Sync
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: SUPABASE_URL or SUPABASE_SERVICE_KEY not found. Skipping API sync.")
            return

        print(f"Clearing existing data from Supabase table {target_table}...")
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        delete_url = f"{SUPABASE_URL}/rest/v1/{target_table}?id=not.is.null"
        del_res = requests.delete(delete_url, headers=headers)
        if del_res.status_code not in [200, 204]:
            print(f"Error clearing data: {del_res.status_code} - {del_res.text}")
            return
            
        print("Existing data cleared.")

        url = f"{SUPABASE_URL}/rest/v1/{target_table}"
        headers["Prefer"] = "resolution=merge-duplicates"
        
        chunk_size = 100
        for i in range(0, len(results_data), chunk_size):
            chunk = results_data[i:i + chunk_size]
            print(f"Upserting chunk {i//chunk_size + 1} ({len(chunk)} rows)...")
            response = requests.post(url, headers=headers, data=json.dumps(chunk))
            if response.status_code not in [200, 201]:
                print(f"Error upserting chunk: {response.status_code} - {response.text}")
                break

        print("API Sync complete!")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Gold Results to Supabase or Export as SQL.")
    parser.add_argument("--level", choices=["L0", "L1", "L2"], default="L1", help="Level of filtering to sync (L0=Main, L1=Roster, L2=Peers)")
    parser.add_argument("--table", default="Gold_Results", help="Target table in Supabase")
    parser.add_argument("--format", choices=["api", "sql"], default="api", help="Export format: api (Supabase sync) or sql (SQL file)")
    
    args = parser.parse_args()
    sync_results(args.level, args.table, args.format)
