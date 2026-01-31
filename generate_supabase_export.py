import sqlite3
import os
import argparse

LOCAL_DB_PATH = "gym_data.db"

TABLE_MAP = {
    "L0": "Gold_Results_MAG",
    "L1": "Gold_Results_MAG_Filtered_L1",
    "L2": "Gold_Results_MAG_Filtered_L2"
}

def clean_record(record):
    """Clean record for export."""
    # Remove local-only columns
    if 'source' in record:
        del record['source']
    
    # Numerics cleanup
    numeric_keys = [
        'fx_score', 'fx_d', 'ph_score', 'ph_d', 'sr_score', 'sr_d', 
        'vt_score', 'vt_d', 'pb_score', 'pb_d', 'hb_score', 'hb_d', 
        'ub_score', 'ub_d', 'bb_score', 'bb_d',
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
        for i, record in enumerate(data):
            if i % 1000 == 0:
                print(f"Processing row {i}...")
            values = []
            for k in keys:
                v = record[k]
                if v is None:
                    values.append("NULL")
                elif isinstance(v, (int, float)):
                    values.append(str(v))
                else:
                    # Escape single quotes and handle potential newlines
                    escaped_v = str(v).replace("'", "''").replace("\n", " ").strip()
                    values.append(f"'{escaped_v}'")
            
            val_str = ", ".join(values)
            # Use PostgreSQL-compatible ON CONFLICT clause
            # Assumes a unique index exists on (athlete_name, meet_name, year, level, age)
            f.write(f"INSERT INTO \"{table_name}\" ({columns}) VALUES ({val_str}) ON CONFLICT DO NOTHING;\n")

def generate_export(level, target_table):
    if not os.path.exists(LOCAL_DB_PATH):
        print(f"Error: Local database not found at {LOCAL_DB_PATH}")
        return

    source_table = TABLE_MAP.get(level)
    if not source_table:
        print(f"Error: Invalid level {level}")
        return

    conn = sqlite3.connect(LOCAL_DB_PATH)
    # Set busy timeout to handle concurrent loader writes
    conn.execute("PRAGMA busy_timeout = 60000")
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

        output_file = f"{target_table}_{level}.sql"
        export_sql(results_data, target_table, output_file)
        print(f"SQL export complete: {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Gold Results as SQL for Supabase.")
    parser.add_argument("--level", choices=["L0", "L1", "L2"], default="L1", help="Level of filtering to export (L0=Main, L1=Roster, L2=Peers)")
    parser.add_argument("--table", default="Gold_Results", help="Target table name in SQL (Schema: public)")
    
    args = parser.parse_args()
    generate_export(args.level, args.table)
