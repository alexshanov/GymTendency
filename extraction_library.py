# extraction_library.py

import os
import pandas as pd
import re
import json

# ==============================================================================
#  KSCORE EXTRACTION
# ==============================================================================

def extract_kscore_data(filepath, meet_manifest, level_alias_map):
    """
    Extracts data from a single Kscore CSV without DB interaction.
    """
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty:
            return None
    except Exception as e:
        print(f"Warning: Could not read CSV file '{filepath}'. Error: {e}")
        return None

    filename = os.path.basename(filepath)
    full_source_id = filename.split('_FINAL_')[0]
    source_meet_id = full_source_id.replace('kscore_', '', 1)
    
    meet_details = meet_manifest.get(full_source_id, {})
    if not meet_details.get('name'):
        meet_details['name'] = df['Raw_Meet_Name'].iloc[0] if 'Raw_Meet_Name' in df.columns else (df['Meet'].iloc[0] if 'Meet' in df.columns and not df.empty else f"Kscore {source_meet_id}")
    
    if not meet_details.get('year'):
        if 'Year' in df.columns and not df.empty:
            meet_details['year'] = df['Year'].iloc[0]

    # Detect Discipline
    MAG_INDICATORS = {'Pommel_Horse', 'PommelHorse', 'Rings', 'Parallel_Bars', 'ParallelBars', 'High_Bar', 'HighBar', 'Horizontal_Bar'}
    WAG_INDICATORS = {'Uneven_Bars', 'UnevenBars', 'Beam'}
    discipline_id = 99
    discipline_name = 'Other'
    gender_heuristic = 'Unknown'
    
    for col in df.columns:
        if any(indicator in col for indicator in MAG_INDICATORS):
            discipline_id = 2
            discipline_name = 'MAG'
            gender_heuristic = 'M'
            break
        if any(indicator in col for indicator in WAG_INDICATORS):
            discipline_id = 1
            discipline_name = 'WAG'
            gender_heuristic = 'F'
            break

    # Key Mapping
    KEY_MAP = {'Gymnast': 'Name', 'Athlete': 'Name', 'Name': 'Name', 'Club': 'Club', 'Team': 'Club', 'Level': 'Level', 'Age': 'Age', 'Prov': 'Prov'}
    col_map = {col: KEY_MAP.get(col, col) for col in df.columns}
    name_col = next((c for c, v in col_map.items() if v == 'Name'), None)
    
    if not name_col:
        return None

    result_columns = [col for col in df.columns if col.startswith('Result_')]
    event_bases = {}
    for col in result_columns:
        match = re.search(r'Result_(.*)_(Score|D|Rnk)$', col)
        if match:
            event_bases[match.group(1)] = match.group(1)

    ignore_cols = list(event_bases.keys()) + result_columns + [name_col]
    if 'Club' in df.columns: ignore_cols.append('Club')
    
    dynamic_cols = []
    for col in df.columns:
        if col not in ignore_cols and not col.startswith('Result_'):
            dynamic_cols.append(col)

    extracted_results = []
    for _, row in df.iterrows():
        raw_name = row.get(name_col)
        if not raw_name: continue
        
        raw_level = row.get('Level', '')
        mapped_level = level_alias_map.get(raw_level, raw_level)
        
        dynamic_values = {}
        for col in dynamic_cols:
            val = row.get(col)
            if col == 'Level': val = mapped_level
            if val: dynamic_values[col] = str(val)

        apparatus_results = []
        for raw_event in event_bases:
            d_val = row.get(f'Result_{raw_event}_D')
            score_val = row.get(f'Result_{raw_event}_Score')
            rank_val = row.get(f'Result_{raw_event}_Rnk')
            bonus_val = row.get(f'Result_{raw_event}_Bonus')
            exec_bonus_val = row.get(f'Result_{raw_event}_Exec_Bonus') or row.get(f'Result_{raw_event}_Execution_Bonus')
            
            if not score_val and not d_val: continue

            # --- Score Swapping / Preference Logic ---
            # For Interclub/Level 1, Kscore often puts numeric score in D and award text in Score.
            def is_numeric(s):
                try:
                    float(str(s).strip().replace(',', ''))
                    return True
                except:
                    return False
            
            actual_score = score_val
            actual_d = d_val
            actual_rank = rank_val
            
            if is_numeric(d_val) and not is_numeric(score_val):
                # Swap: use numeric D as the actual score
                actual_score = d_val
                # If rank is empty, move the non-numeric "Score" (e.g. "Gold") to rank_text
                if not rank_val or str(rank_val).strip() == '':
                    actual_rank = score_val
            elif not is_numeric(score_val) and (score_val and str(score_val).strip() != ''):
                # If Score is non-numeric (e.g. "Silver") and D is not a trigger,
                # move the Score to rank_text if rank is empty.
                if not rank_val or str(rank_val).strip() == '':
                    actual_rank = score_val
            
            apparatus_results.append({
                'raw_event': raw_event,
                'score_final': actual_score,
                'score_d': actual_d,
                'rank_text': actual_rank,
                'bonus': bonus_val,
                'execution_bonus': exec_bonus_val
            })

        extracted_results.append({
            'raw_name': raw_name,
            'raw_club': row.get('Club', ''),
            'discipline_id': discipline_id,
            'gender_heuristic': gender_heuristic,
            'apparatus_results': apparatus_results,
            'dynamic_metadata': dynamic_values
        })

    return {
        'source': 'kscore',
        'source_meet_id': source_meet_id,
        'meet_details': meet_details,
        'results': extracted_results
    }

# ==============================================================================
#  LIVEMEET EXTRACTION
# ==============================================================================

def extract_livemeet_data(filepath, meet_manifest):
    """
    Extracts data from a single Livemeet CSV without DB interaction.
    """
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty:
            return None
            
        # --- Handle Multi-row / Messy Headers ---
        # If 'Name' is not in columns, it might be a Sportzsoft CSV with junk rows at the top
        if 'Name' not in df.columns:
            found_header = False
            # Check the first 10 rows for a valid header row
            for i in range(min(10, len(df))):
                row_vals = [str(x).strip() for x in df.iloc[i].values]
                if 'Name' in row_vals and 'Club' in row_vals:
                    # Found it! Set columns and drop the junk rows above
                    df.columns = row_vals
                    df = df.iloc[i+1:].reset_index(drop=True)
                    found_header = True
                    break
            if not found_header:
                # Still didn't find it? Check if we have 'Unnamed: 4' as Name (common offset)
                # But safer to skip if we can't find 'Name' explicitly
                return None
    except Exception as e:
        print(f"Warning: Could not read CSV file '{filepath}'. Error: {e}")
        return None

    filename = os.path.basename(filepath)
    source_meet_id = filename.split('_FINAL_')[0]
    
    meet_details = meet_manifest.get(source_meet_id, {})
    if not meet_details.get('name'):
        meet_details['name'] = df['Meet'].iloc[0] if 'Meet' in df.columns and not df.empty else f"Livemeet {source_meet_id}"
    
    # FETCH YEAR FROM MANIFEST IF MISSING
    if not meet_details.get('year'):
        if 'Year' in meet_details:
             meet_details['year'] = meet_details['Year']
        elif 'comp_year' in meet_details:
             meet_details['year'] = meet_details['comp_year']

    # Normalize Headers (Sportzsoft triplet logic)
    raw_apps = ['Vault', 'Uneven_Bars', 'Beam', 'Floor', 'Pommel_Horse', 'Rings', 'Parallel_Bars', 'High_Bar', 'AllAround', 'All_Around']
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
            proposed_name = f"Result_{base}_{suffix}" if triplet_num == 0 else f"EXTRA_{base}_{triplet_num}_{suffix}"
            new_headers.append(proposed_name)
        else:
            new_headers.append(col)
    
    # --- Ensure Uniqueness ---
    unique_headers = []
    counts = {}
    for h in new_headers:
        if h in counts:
            counts[h] += 1
            unique_headers.append(f"{h}_{counts[h]}")
        else:
            counts[h] = 0
            unique_headers.append(h)
            
    df.columns = unique_headers

    # Detect Discipline
    MAG_INDICATORS = {'Pommel_Horse', 'PommelHorse', 'Rings', 'Parallel_Bars', 'ParallelBars', 'High_Bar', 'HighBar'}
    WAG_INDICATORS = {'Uneven_Bars', 'UnevenBars', 'Beam'}
    discipline_id = 99
    discipline_name = 'Other'
    gender_heuristic = 'Unknown'
    for col in df.columns:
        if any(indicator in col for indicator in MAG_INDICATORS):
            discipline_id = 2; gender_heuristic = 'M'; break
        if any(indicator in col for indicator in WAG_INDICATORS):
            discipline_id = 1; gender_heuristic = 'F'; break
            
    # Add AA to event bases if not implicitly caught
    # Logic below catches Result_X_Score. We need to ensure Result_AllAround_Score is caught.
    # The fix_and_standardize_headers in scraper should have produced Result_AllAround_Score if 'AA' or 'All Around' was present.
    # If not, the AA fallback will catch it.

    result_columns = [col for col in df.columns if col.startswith('Result_')]
    event_bases = {}
    for col in result_columns:
        match = re.search(r'Result_(.*)_(Score|D|E|Rnk|Total)$', col)
        if match: event_bases[match.group(1)] = match.group(1)

    ignore_cols = list(event_bases.keys()) + result_columns + ['Name', 'Club']
    dynamic_cols = [col for col in df.columns if col not in ignore_cols and not col.startswith('Result_')]

    extracted_results = []
    for _, row in df.iterrows():
        raw_name = row.get('Name')
        if not raw_name: continue

        dynamic_values = {col: str(row.get(col)) for col in dynamic_cols if row.get(col)}

        apparatus_results = []
        for raw_event in event_bases:
            score_val = row.get(f'Result_{raw_event}_Score')
            d_val = row.get(f'Result_{raw_event}_D')
            sv_val = row.get(f'Result_{raw_event}_SV')
            e_val = row.get(f'Result_{raw_event}_E')
            bonus_val = row.get(f'Result_{raw_event}_Bonus')
            penalty_val = row.get(f'Result_{raw_event}_Penalty')
            rank_val = row.get(f'Result_{raw_event}_Rnk')
            exec_bonus_val = row.get(f'Result_{raw_event}_Exec_Bonus') or row.get(f'Result_{raw_event}_Execution_Bonus')
            
            # Fallback: SV is often used for Difficulty in lower levels
            if (not d_val or str(d_val).strip() == '') and (sv_val and str(sv_val).strip() != ''):
                d_val = sv_val

            apparatus_results.append({
                'raw_event': raw_event,
                'score_final': score_val,
                'score_d': d_val,
                'score_sv': sv_val,
                'score_e': e_val,
                'bonus': bonus_val,
                'penalty': penalty_val,
                'rank_text': rank_val,
                'execution_bonus': exec_bonus_val
            })
            
            # --- Score Swapping / Preference Logic ---
            res = apparatus_results[-1]
            def is_numeric(s):
                try:
                    float(str(s).strip().replace(',', ''))
                    return True
                except:
                    return False
            
            if is_numeric(res['score_d']) and not is_numeric(res['score_final']):
                res['score_final'] = res['score_d']
                # Optionally keep the award text elsewhere, but the priority is the numeric score.

        extracted_results.append({
            'raw_name': raw_name,
            'raw_club': row.get('Club', ''),
            'discipline_id': discipline_id,
            'gender_heuristic': gender_heuristic,
            'apparatus_results': apparatus_results,
            'dynamic_metadata': dynamic_values
        })

        # --- AA Fallback Calculation ---
        # If no AA result was found (which often happens with _BYEVENT_ files having partial headers),
        # calculate it from the sum of valid apparatus scores.
        has_aa = any(r['raw_event'] in ['AllAround', 'All Around', 'AA'] for r in apparatus_results)
        
        if not has_aa:
            valid_sum = 0.0
            valid_count = 0
            # Sum all numeric scores for valid apparatuses
            for res in apparatus_results:
                evt = res['raw_event']
                if evt in ['AllAround', 'All Around', 'AA', 'Team']: continue
                
                try:
                    s = float(res['score_final'])
                    valid_sum += s
                    valid_count += 1
                except (ValueError, TypeError):
                    continue
            
            if valid_count > 0:
                extracted_results[-1]['apparatus_results'].append({
                    'raw_event': 'All Around',
                    'score_final': f"{valid_sum:.3f}", 
                    'score_d': '',
                    'rank_text': '',
                    'calculated': True
                })

    return {
        'source': 'livemeet',
        'source_meet_id': source_meet_id,
        'meet_details': meet_details,
        'results': extracted_results
    }

# ==============================================================================
#  MSO EXTRACTION
# ==============================================================================

COLUMN_MAP_MSO = {
    'Gymnast': 'Name', 'Team': 'Club', 'Sess': 'Session', 'Lvl': 'Level', 'Div': 'Age_Group',
    'VT': 'Vault', 'VAULT': 'Vault', 'UB': 'Uneven Bars', 'BARS': 'Uneven Bars', 'UNEVEN BARS': 'Uneven Bars',
    'BB': 'Beam', 'BEAM': 'Beam', 'Balance Beam': 'Beam', 'FX': 'Floor', 'FLR': 'Floor', 'FLOOR': 'Floor',
    'AA': 'All Around', 'ALL AROUND': 'All Around', 'PH': 'Pommel Horse', 'POMMEL HORSE': 'Pommel Horse', 'POMML': 'Pommel Horse',
    'SR': 'Rings', 'RINGS': 'Rings', 'PB': 'Parallel Bars', 'PBARS': 'Parallel Bars', 'PARALLEL BARS': 'Parallel Bars',
    'HB': 'High Bar', 'HIBAR': 'High Bar', 'HIGH BAR': 'High Bar'
}

def parse_mso_cell_value(cell_str):
    if not isinstance(cell_str, str) or not cell_str.strip(): return None, None, None, None, None
    parts = cell_str.split()
    if not parts: return None, None, None, None, None
    
    score_final = None; d_score = None; rank_numeric = None; rank_text = None; bonus = None
    
    def is_float(s):
        try: float(s); return True
        except: return False
        
    if len(parts) == 1:
        if is_float(parts[0]): score_final = float(parts[0])
    elif len(parts) == 2:
        if is_float(parts[0]) and is_float(parts[1]): score_final = float(parts[1]) + (float(parts[0]) / 1000.0)
    elif len(parts) == 3:
        if is_float(parts[1]) and is_float(parts[2]):
            score_final = float(parts[2]) + (float(parts[1]) / 1000.0)
            rank_text = parts[0]
    elif len(parts) == 4:
        if is_float(parts[1]) and is_float(parts[2]):
            score_final = float(parts[2]) + (float(parts[1]) / 1000.0)
            rank_text = parts[0]
            if is_float(parts[3]): bonus = float(parts[3])
    elif is_float(parts[-1]):
        score_final = float(parts[-1])
        rank_text = parts[0]
        
    return score_final, d_score, None, rank_text, bonus

def extract_mso_data(filepath, meet_manifest):
    """
    Extracts data from a single MSO CSV without DB interaction.
    """
    filename = os.path.basename(filepath)
    source_meet_id = filename.split('_mso.csv')[0]
    
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty: return None
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

    meet_details = meet_manifest.get(source_meet_id, {})
    if not meet_details.get('name') and 'Meet' in df.columns:
        meet_details['name'] = df['Meet'].iloc[0]

    headers = df.columns
    def find_col_by_fuzzy(candidates):
        for c in headers:
            if c.upper().strip() in [cand.upper() for cand in candidates]: return c
        return None

    name_col = find_col_by_fuzzy(['Gymnast', 'Name'])
    club_col = find_col_by_fuzzy(['Team', 'Club'])
    if not name_col: return None

    apparatus_cols = []
    dynamic_metadata_cols = []
    known_apps = ['Vault', 'Uneven Bars', 'Beam', 'Floor', 'All Around', 'Pommel Horse', 'Rings', 'Parallel Bars', 'High Bar']
    
    for col in headers:
        norm_key = COLUMN_MAP_MSO.get(col.strip(), col)
        if norm_key in known_apps: apparatus_cols.append(col)
        elif col not in [name_col, club_col]: dynamic_metadata_cols.append(col)

    detected_names = [COLUMN_MAP_MSO.get(c.strip(), c) for c in apparatus_cols]
    if any(x in ['Pommel Horse', 'Rings', 'Parallel Bars', 'High Bar'] for x in detected_names):
        discipline_id = 2; gender_heuristic = 'M'
    else:
        discipline_id = 1; gender_heuristic = 'F'

    extracted_results = []
    for _, row in df.iterrows():
        raw_name = row.get(name_col)
        if not raw_name: continue
        
        dynamic_vals = {col: str(row.get(col)) for col in dynamic_metadata_cols if row.get(col)}
        
        apparatus_results = []
        for raw_app_col in apparatus_cols:
            cell_value = row.get(raw_app_col)
            if not cell_value: continue
            
            clean_app_name = COLUMN_MAP_MSO.get(raw_app_col.strip(), raw_app_col)
            score_final, score_d, _, rank_text, bonus = parse_mso_cell_value(cell_value)
            
            if score_final is None and score_d is None: continue
            
            apparatus_results.append({
                'raw_event': clean_app_name,
                'score_final': score_final,
                'score_d': score_d,
                'rank_text': rank_text,
                'bonus': bonus,
                'score_text': str(cell_value)
            })

        extracted_results.append({
            'raw_name': raw_name,
            'raw_club': row.get(club_col, ''),
            'discipline_id': discipline_id,
            'gender_heuristic': gender_heuristic,
            'apparatus_results': apparatus_results,
            'dynamic_metadata': dynamic_vals
        })

    return {
        'source': 'mso',
        'source_meet_id': source_meet_id,
        'meet_details': meet_details,
        'results': extracted_results
    }
