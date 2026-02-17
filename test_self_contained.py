
import csv
import io
import re
import os

def extract_livemeet_data_minimal(filepath, meet_details):
    import csv
    import io
    import re
    import os
    
    try:
        with open(filepath, 'r', encoding='utf-8-sig', errors='replace') as f:
            lines = f.readlines()
            
        if not lines: return None
            
        header_idx = -1
        for i, line in enumerate(lines[:10]):
            parts = [p.strip() for p in line.split(',')]
            if 'Name' in parts and 'Club' in parts:
                header_idx = i
                break
        
        if header_idx == -1: header_idx = 0
            
        headers = [p.strip() for p in lines[header_idx].split(',')]
        is_normalized = any(h.startswith('Result_') for h in headers)
        deduped_headers = headers

        content = "".join(lines[header_idx:])
        
        result_columns = [h for h in deduped_headers if h.startswith('Result_')]
        event_bases = {}
        for col in result_columns:
            match = re.search(r'Result_(.*)_(Score|D|E|Rnk|Total)$', col)
            if match: 
                event_bases[match.group(1)] = match.group(1)

        MAG_INDICATORS = {'Pommel_Horse', 'PommelHorse', 'Rings', 'Parallel_Bars', 'ParallelBars', 'High_Bar', 'HighBar'}
        mag_score = 0
        for col in deduped_headers:
            if any(ind in col for ind in MAG_INDICATORS): mag_score += 1
        
        discipline_id = 2 if mag_score > 0 else 1
        gender_heuristic = 'M' if discipline_id == 2 else 'F'

        extracted_results = []
        reader = csv.DictReader(io.StringIO(content))
        
        for row in reader:
            raw_name = row.get('Name')
            if not raw_name: continue
            
            apparatus_results = []
            for raw_event in event_bases:
                score_val = row.get(f'Result_{raw_event}_Score')
                d_val = row.get(f'Result_{raw_event}_D')
                
                if (not score_val or str(score_val).strip() == '') and \
                   (not d_val or str(d_val).strip() == ''):
                    continue
                
                apparatus_results.append({
                    'raw_event': raw_event,
                    'score_final': score_val,
                    'score_d': d_val
                })

            extracted_results.append({
                'raw_name': raw_name,
                'raw_club': row.get('Club', ''),
                'apparatus_results': apparatus_results
            })

        return extracted_results
    except Exception as e:
        return str(e)

filepath = "CSVs_Livemeet_final/D6A65C67D6FEE84AAF3BB44FDE4ECC93_FINAL_AA_MAG_Novice_Combined_MAG.csv"
res = extract_livemeet_data_minimal(filepath, {})
if isinstance(res, list):
    print(f"Success! Athletes: {len(res)}")
    anton = next((a for a in res if "Anton Prosolin" in a['raw_name']), None)
    if anton:
        print(f"Anton Results: {anton['apparatus_results']}")
    else:
        print("Anton not found")
else:
    print(f"Error: {res}")
