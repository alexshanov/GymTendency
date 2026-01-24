# extraction_library.py

import os
import pandas as pd
import re
import json

# ==============================================================================
#  KSCORE EXTRACTION
# ==============================================================================

def extract_kscore_data(filepath, meet_details, level_alias_map):
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

def extract_livemeet_data(filepath, meet_details):
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
                    # Found it! Deduplicate columns and set
                    seen_cols = {}
                    deduped_cols = []
                    for c in row_vals:
                        if c not in seen_cols:
                            seen_cols[c] = 0
                            deduped_cols.append(c)
                        else:
                            seen_cols[c] += 1
                            deduped_cols.append(f"{c}.{seen_cols[c]}")
                    
                    df.columns = deduped_cols
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
    # Handle PER/BYEVENT variants
    if '_PEREVENT_' in source_meet_id: source_meet_id = source_meet_id.split('_PEREVENT_')[0]
    if '_BYEVENT_' in source_meet_id: source_meet_id = source_meet_id.split('_BYEVENT_')[0]

    if not meet_details.get('name'):
        meet_val = None
        if 'Meet' in df.columns and not df.empty:
            raw_meet = df['Meet']
            if isinstance(raw_meet, pd.DataFrame):
                raw_meet = raw_meet.iloc[:, 0]
            meet_val = raw_meet.iloc[0]
            
        meet_details['name'] = meet_val if meet_val else f"Livemeet {source_meet_id}"
    
    # FETCH YEAR FROM MANIFEST IF MISSING
    if not meet_details.get('year'):
        if 'Year' in meet_details:
             meet_details['year'] = meet_details['Year']
        elif 'comp_year' in meet_details:
             meet_details['year'] = meet_details['comp_year']

    # Recognize Already-Normalized Headers (DETAILED files)
    if any(col.startswith('Result_') for col in df.columns):
        # Already normalized, don't apply triplet logic
        pass
    else:
        # Normalize Headers (Sportzsoft triplet logic)
        raw_apps = ['Vault', 'Uneven_Bars', 'Beam', 'Floor', 'Pommel_Horse', 'Rings', 'Parallel_Bars', 'High_Bar', 'AllAround', 'All_Around', 'PommelHorse', 'ParallelBars', 'HighBar']
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
        df.columns = new_headers

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

def extract_mso_data(filepath, meet_details):
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
    
    # Heuristic 1: Apparatus names
    has_mag_apps = any(x in ['Pommel Horse', 'Rings', 'Parallel Bars', 'High Bar'] for x in detected_names)
    has_wag_apps = any(x in ['Uneven Bars', 'Beam'] for x in detected_names)
    
    # Heuristic 2: Level codes (WAG-specific: XS, XG, XP, XB, Xcel, CCP; MAG-specific: P1, P2... PO)
    # Check FIRST athlete's level as a sample
    sample_level = str(df['Lvl'].iloc[0]).upper() if 'Lvl' in df.columns and not df.empty else ""
    
    is_wag_level = any(x in sample_level for x in ['XS', 'XG', 'XP', 'XB', 'XCEL', 'CCP'])
    is_mag_level = any(re.match(r'^P\d', sample_level) for x in [sample_level]) or 'PO' in sample_level
    
    if is_wag_level:
        discipline_id = 1; gender_heuristic = 'F'
    elif is_mag_level:
        discipline_id = 2; gender_heuristic = 'M'
    elif has_mag_apps and not has_wag_apps:
        discipline_id = 2; gender_heuristic = 'M'
    else:
        discipline_id = 1; gender_heuristic = 'F' # Default WAG

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

# ==============================================================================
#  KSIS EXTRACTION
# ==============================================================================

def extract_ksis_data(filepath, meet_details):
    """
    Extracts data from a KSIS CSV.
    """
    try:
        df = pd.read_csv(filepath, keep_default_na=False, dtype=str)
        if df.empty or 'Name' not in df.columns:
            return None
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

    # Derive IDs
    source_meet_id = str(df['MeetID'].iloc[0])
    meet_year = df['MeetYear'].iloc[0] if 'MeetYear' in df.columns else ""
    session_name = df['Session'].iloc[0] if 'Session' in df.columns else ""
    
    # Update meet details
    if not meet_details.get('name') and 'MeetName' in df.columns:
        meet_details['name'] = df['MeetName'].iloc[0]
    
    if not meet_details.get('year') and meet_year:
        meet_details['year'] = meet_year

    # Detect Discipline
    # 1=WAG, 2=MAG
    discipline_id = 2  # Default MAG
    if "WAG" in session_name.upper() or "WOMEN" in session_name.upper() or "ARTW" in (meet_details.get('name') or "").upper(): 
        discipline_id = 1
    
    gender_heuristic = 'M' if discipline_id == 2 else 'F'

    # Identify apparatus columns
    app_bases = set()
    for col in df.columns:
        if col.endswith('_Total') and col != 'AA_Total':
            app_bases.add(col.replace('_Total', ''))

    # Apparatus mapping
    APP_MAP = {
        'mfloor': 'Floor', 'horse': 'Pommel Horse', 'rings': 'Rings', 'mvault': 'Vault', 
        'pbars': 'Parallel Bars', 'hbar': 'High Bar', 'wvault': 'Vault', 'ubars': 'Uneven Bars', 
        'beam': 'Beam', 'wfloor': 'Floor'
    }

    extracted_results = []
    
    for _, row in df.iterrows():
        raw_name = row['Name']
        if not raw_name: continue
        
        level = row.get('Session', '')
        dynamic_vals = {'level': level}
        
        apparatus_results = []
        
        # AA Special Case
        aa_score_str = row.get('AA_Score')
        if aa_score_str:
            aa_rank = row.get('Place', '')
            apparatus_results.append({
                'raw_event': 'All Around',
                'score_final': aa_score_str,
                'rank_text': aa_rank
            })

        # Individual Apps
        for app_base in app_bases:
            std_app_name = APP_MAP.get(app_base, app_base)
            
            total_str = row.get(f"{app_base}_Total")
            d_str = row.get(f"{app_base}_D")
            e_str = row.get(f"{app_base}_E")
            bonus_str = row.get(f"{app_base}_Bonus")
            nd_str = row.get(f"{app_base}_ND")
            
            # Helper to parse "12.150(5)" -> 12.150, rank 5
            score_final = total_str
            rank = ""
            if total_str:
                match = re.search(r'([\d\.]+)\((\d+)\)', total_str)
                if match:
                    score_final = match.group(1)
                    rank = match.group(2)
            
            apparatus_results.append({
                'raw_event': std_app_name,
                'score_final': score_final,
                'score_d': d_str,
                'score_e': e_str,
                'rank_text': rank,
                'bonus': bonus_val if 'bonus_val' in locals() else bonus_str, # Fix: bonus_str
                'penalty': nd_str
            })

        extracted_results.append({
            'raw_name': raw_name,
            'raw_club': row.get('Club'),
            'discipline_id': discipline_id,
            'gender_heuristic': gender_heuristic,
            'apparatus_results': apparatus_results,
            'dynamic_metadata': dynamic_vals
        })

    return {
        'source': 'ksis',
        'source_meet_id': source_meet_id,
        'meet_details': meet_details,
        'results': extracted_results
    }
