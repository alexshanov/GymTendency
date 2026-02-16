
import pandas as pd
import re
import io

csv_data = """Name,Club,Level,Age,Prov,Age_Group,Reporting_Category,Meet,Group,MAG_Novice,Artistic_Elite_Canada_2026,MAG_Novice_-_Combined,#,Result_Floor_D,Result_Floor_Score,Result_Floor_Rnk,Result_Pommel_Horse_D,Result_Pommel_Horse_Score,Result_Pommel_Horse_Rnk,Result_Rings_D,Result_Rings_Score,Result_Rings_Rnk,Result_Vault_D,Result_Vault_Score,Result_Vault_Rnk,Result_Parallel_Bars_D,Result_Parallel_Bars_Score,Result_Parallel_Bars_Rnk,Result_High_Bar_D,Result_High_Bar_Score,Result_High_Bar_Rnk,Result_AllAround_D,Result_AllAround_Score,Result_AllAround_Rnk
Anton Prosolin,CGC,MNov,14,AB,,,,,MAG Novice,Artistic Elite Canada 2026,MAG Novice - Combined,,6.60,24.066,1,4.80,19.633,4,4.20,20.733,6,5.60,22.916,2,5.40,22.900,3,4.60,20.632,5,31.2,130.880,5"""

df = pd.read_csv(io.StringIO(csv_data), keep_default_na=False, dtype=str)

# --- Event identification ---
result_columns = [col for col in df.columns if col.startswith('Result_')]
event_bases = {}
for col in result_columns:
    match = re.search(r'Result_(.*)_(Score|D|E|Rnk|Total)$', col)
    if match:
        raw_event_name = match.group(1)
        event_bases[raw_event_name] = raw_event_name

# --- Extraction Test ---
row = df.iloc[0]
for key in event_bases:
    col_name = f'Result_{key}_Score'
    score_val = row.get(col_name)
    print(f"  {key} -> Column '{col_name}': Value={score_val!r}")
