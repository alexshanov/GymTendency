
import csv
import io
import re

csv_desc = "Name,Club,Level,Age,Prov,Age_Group,Reporting_Category,Meet,Group,MAG_Novice,Artistic_Elite_Canada_2026,MAG_Novice_-_Combined,#,Result_Floor_D,Result_Floor_Score,Result_Floor_Rnk,Result_Pommel_Horse_D,Result_Pommel_Horse_Score,Result_Pommel_Horse_Rnk,Result_Rings_D,Result_Rings_Score,Result_Rings_Rnk,Result_Vault_D,Result_Vault_Score,Result_Vault_Rnk,Result_Parallel_Bars_D,Result_Parallel_Bars_Score,Result_Parallel_Bars_Rnk,Result_High_Bar_D,Result_High_Bar_Score,Result_High_Bar_Rnk,Result_AllAround_D,Result_AllAround_Score,Result_AllAround_Rnk\n"
csv_data = "Anton Prosolin,CGC,MNov,14,AB,,,,,MAG Novice,Artistic Elite Canada 2026,MAG Novice - Combined,,6.60,24.066,1,4.80,19.633,4,4.20,20.733,6,5.60,22.916,2,5.40,22.900,3,4.60,20.632,5,31.2,130.880,5\n"

content = csv_desc + csv_data
reader = csv.DictReader(io.StringIO(content))

headers = reader.fieldnames
result_columns = [h for h in headers if h.startswith('Result_')]
event_bases = {}
for col in result_columns:
    match = re.search(r'Result_(.*)_(Score|D|E|Rnk|Total)$', col)
    if match: event_bases[match.group(1)] = match.group(1)

print(f"Bases: {list(event_bases.keys())}")

for row in reader:
    print(f"Row for {row.get('Name')}")
    for key in event_bases:
        score = row.get(f'Result_{key}_Score')
        print(f"  {key}: {score!r}")
