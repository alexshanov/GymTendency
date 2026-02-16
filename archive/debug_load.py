import pandas as pd
import re
import os
import sys

# Flush output immediately
def print_flush(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()

filepath = "CSVs_Livemeet_final/D6A65C67D6FEE84AAF3BB44FDE4ECC93_FINAL_AA_MAG_Novice_Combined_MAG.csv"
print_flush(f"Reading file: {filepath}")
df = pd.read_csv(filepath, keep_default_na=False, dtype=str, nrows=10) # Limit rows

print_flush("Original Columns:")
print_flush(df.columns.tolist())

# --- Loader Header Normalization Logic ---
raw_apps = ['Vault', 'Uneven_Bars', 'Beam', 'Floor', 'Pommel_Horse', 'Rings', 'Parallel_Bars', 'High_Bar', 'AllAround',
            'Uneven Bars', 'Pommel Horse', 'Parallel Bars', 'High Bar', 'All Around']
new_headers = []
seen_counts = {}
for col in df.columns:
    base = col.split('.')[0]
    if base in raw_apps:
        count = seen_counts.get(base, 0)
        seen_counts[base] = count + 1
        triplet_pos = count % 3
        triplet_num = count // 3
        suffix = ['D', 'Score', 'Rnk'][triplet_pos]
        if triplet_num == 0:
            proposed_name = f"Result_{base}_{suffix}"
        else:
            proposed_name = f"EXTRA_{base}_{triplet_num}_{suffix}"
        
        if proposed_name in df.columns:
            new_headers.append(col)
        else:
            new_headers.append(proposed_name)
    else:
        new_headers.append(col)

df.columns = new_headers
print("\nNormalized Columns:")
print(df.columns.tolist())

# --- Event identification ---
result_columns = [col for col in df.columns if col.startswith('Result_')]
print(f"\nResult Columns Found: {result_columns}")

event_bases = {}
for col in result_columns:
    match = re.search(r'Result_(.*)_(Score|D|E|Rnk|Total)$', col)
    if match:
        raw_event_name = match.group(1)
        event_bases[raw_event_name] = raw_event_name
        print(f"  Mapped column '{col}' to event '{raw_event_name}'")

print("\nEvent Bases Unique List:")
print(list(event_bases.keys()))

# --- Check for duplicate column names in DF ---
if df.columns.duplicated().any():
    print("\nWARNING: Duplicate columns detected in DataFrame!")
    print(df.columns[df.columns.duplicated()].tolist())

# --- Sample Row Extraction for Anton ---
anton_rows = df[df['Name'] == 'Anton Prosolin']
if not anton_rows.empty:
    row = anton_rows.iloc[0]
    print("\nAnton Prosolin Full Row Series:")
    print(row)
    print("\nExtraction Test:")
    for key in event_bases:
        col_name = f'Result_{key}_Score'
        score_val = row.get(col_name)
        d_name = f'Result_{key}_D'
        d_val = row.get(d_name)
        print(f"  {key} -> Column '{col_name}': Value={score_val!r}, Type={type(score_val)}")
        print(f"  {key} -> Column '{d_name}': Value={d_val!r}, Type={type(d_val)}")
else:
    print("\nAnton Prosolin not found in DF")
