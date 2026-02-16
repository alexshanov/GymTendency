import os
import re
import json

# ==============================================================================
#  KSCORE EXTRACTION
# ==============================================================================

def extract_kscore_data(filepath, meet_details, level_alias_map):
    """
    Extracts data from a single Kscore CSV without DB interaction.
    """
    import pandas as pd
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
    Extracts data from a single Livemeet CSV using standard csv module for speed and robustness.
    """
    import csv
    
    try:
        # We read the file twice: once to find the header row, then to parse
        with open(filepath, 'r', encoding='utf-8-sig', errors='replace') as f:
            lines = f.readlines()
            
        if not lines:
            return None
            
        # Find header row index
        header_idx = -1
        for i, line in enumerate(lines[:10]):
            parts = [p.strip() for p in line.split(',')]
            if 'Name' in parts and 'Club' in parts:
                header_idx = i
                break
        
        if header_idx == -1:
            # Fallback: assume first line is header if it has 'Name' or '@' (some Sportzsoft)
            header_idx = 0
            
        headers = [p.strip() for p in lines[header_idx].split(',')]
        
        # Deduplicate headers if Sportzsoft triplet logic is not yet applied
        seen_cols = {}
        seen_counts = {}
        deduped_headers = []
        is_normalized = any(h.startswith('Result_') for h in headers)
        
        if is_normalized:
            deduped_headers = headers
        else:
            raw_apps = [
                'Vault', 'Uneven_Bars', 'Uneven Bars', 'Beam', 'Floor', 
                'Pommel_Horse', 'Pommel Horse', 'PommelHorse', 
                'Rings', 'Parallel_Bars', 'Parallel Bars', 'ParallelBars', 
                'High_Bar', 'High Bar', 'HighBar', 
                'AllAround', 'All_Around', 'All Around', 'AA'
            ]
            for col in headers:
                base = col.split('.')[0]
                if base in raw_apps:
                    count = seen_counts.get(base, 0)
                    seen_counts[base] = count + 1
                    triplet_pos = count % 3
                    triplet_num = count // 3
                    suffix = ['D', 'Score', 'Rnk'][triplet_pos]
                    proposed_name = f"Result_{base}_{suffix}" if triplet_num == 0 else f"EXTRA_{base}_{triplet_num}_{suffix}"
                    deduped_headers.append(proposed_name)
                else:
                    deduped_headers.append(col)

        # Parse data rows
        import io
        content = "".join(lines[header_idx:])
        
        filename = os.path.basename(filepath)
        source_meet_id = filename.split('_FINAL_')[0]
        if '_PEREVENT_' in source_meet_id: source_meet_id = source_meet_id.split('_PEREVENT_')[0]
        if '_BYEVENT_' in source_meet_id: source_meet_id = source_meet_id.split('_BYEVENT_')[0]

        # Fetch basic meet info
        if not meet_details.get('name'):
            # Try to get from first data row 'Meet' column
            pass 

        # Event identification
        result_columns = [h for h in deduped_headers if h.startswith('Result_')]
        event_bases = {}
        for col in result_columns:
            match = re.search(r'Result_(.*)_(Score|D|E|Rnk|Total)$', col)
            if match: 
                event_bases[match.group(1)] = match.group(1)

        # Discipline Detection
        MAG_INDICATORS = {'Pommel_Horse', 'PommelHorse', 'Rings', 'Parallel_Bars', 'ParallelBars', 'High_Bar', 'HighBar'}
        WAG_INDICATORS = {'Uneven_Bars', 'UnevenBars', 'Beam'}
        mag_score = 0; wag_score = 0
        for col in deduped_headers:
            if any(ind in col for ind in MAG_INDICATORS): mag_score += 1
            if any(ind in col for ind in WAG_INDICATORS): wag_score += 1
        
        discipline_id = 2 if mag_score >= wag_score and mag_score > 0 else 1
        gender_heuristic = 'M' if discipline_id == 2 else 'F'

        ignore_cols = list(event_bases.keys()) + result_columns + ['Name', 'Club']
        dynamic_cols = [h for h in deduped_headers if h not in ignore_cols and not h.startswith('Result_')]

        extracted_results = []
        
        f_stream = io.StringIO(content)
        reader = csv.DictReader(f_stream)
        
        for row in reader:
            raw_name = row.get('Name')
            if not raw_name: continue
            
            # Basic normalization for meet name if missing
            if not meet_details.get('name') and row.get('Meet'):
                meet_details['name'] = row.get('Meet')

            dynamic_values = {}
            session_markers = ['Day', 'Session', 'Flight', 'Combined', 'Finals', 'Apparatus Final']
            for col in dynamic_cols:
                val = row.get(col)
                if not val or str(val).strip() == '': continue
                safe_key = str(col).strip()
                if "unnamed" in safe_key.lower(): continue
                if len(safe_key) > 60: continue # Some are long but we need them
                
                # Heuristic: if column header looks like a session and we don't have one yet
                if any(m.lower() in safe_key.lower() for m in session_markers):
                    if 'session' not in dynamic_values:
                        dynamic_values['session'] = str(val).strip()
                
                if safe_key.count(' ') > 4 and safe_key not in dynamic_values.get('session', ''): continue
                dynamic_values[safe_key] = str(val).strip()

            apparatus_results = []
            for raw_event in event_bases:
                # Prioritize 'Total' column over 'Score' column if it exists. 
                # This is crucial for multi-day 'Combined' DETAILED files where 'Score' is often Day 1.
                score_val = row.get(f'Result_{raw_event}_Total') or row.get(f'Result_{raw_event}_Score')
                d_val = row.get(f'Result_{raw_event}_D')
                sv_val = row.get(f'Result_{raw_event}_SV')
                e_val = row.get(f'Result_{raw_event}_E')
                bonus_val = row.get(f'Result_{raw_event}_Bonus')
                penalty_val = row.get(f'Result_{raw_event}_Penalty')
                rank_val = row.get(f'Result_{raw_event}_Rnk')
                exec_bonus_val = row.get(f'Result_{raw_event}_Exec_Bonus') or row.get(f'Result_{raw_event}_Execution_Bonus')
                
                if (not score_val or str(score_val).strip() == '') and \
                   (not d_val or str(d_val).strip() == '') and \
                   (not sv_val or str(sv_val).strip() == ''):
                    continue
                
                # Fallback: SV is often used for Difficulty
                if (not d_val or str(d_val).strip() == '') and (sv_val and str(sv_val).strip() != ''):
                    d_val = sv_val

                app_res = {
                    'raw_event': raw_event,
                    'score_final': score_val,
                    'score_d': d_val,
                    'score_sv': sv_val,
                    'score_e': e_val,
                    'bonus': bonus_val,
                    'penalty': penalty_val,
                    'rank_text': rank_val,
                    'execution_bonus': exec_bonus_val
                }
                
                # Score Swap Logic
                def is_numeric(s):
                    try:
                        if not s: return False
                        float(str(s).strip().replace(',', ''))
                        return True
                    except: return False
                
                if is_numeric(app_res['score_d']) and not is_numeric(app_res['score_final']):
                    app_res['score_final'] = app_res['score_d']
                
                apparatus_results.append(app_res)

            extracted_results.append({
                'raw_name': raw_name,
                'raw_club': row.get('Club', ''),
                'discipline_id': discipline_id,
                'gender_heuristic': gender_heuristic,
                'apparatus_results': apparatus_results,
                'dynamic_metadata': dynamic_values
            })

            # AA Enrichement
            aa_record = next((r for r in apparatus_results if r['raw_event'] in ['AllAround', 'All Around', 'AA']), None)
            valid_sum = 0.0; valid_d_sum = 0.0; valid_app_count = 0
            
            for res in apparatus_results:
                if res['raw_event'] in ['AllAround', 'All Around', 'AA', 'Team']: continue
                try:
                    s = float(str(res['score_final']).replace(',', ''))
                    valid_sum += s; valid_app_count += 1
                except: pass
                try:
                    d = float(str(res['score_d']).replace(',', ''))
                    valid_d_sum += d
                except: pass
            
            if not aa_record:
                if valid_app_count > 0:
                    apparatus_results.append({
                        'raw_event': 'All Around',
                        'score_final': f"{valid_sum:.3f}", 
                        'score_d': f"{valid_d_sum:.3f}" if valid_d_sum > 0 else '',
                        'calculated': True
                    })
            else:
                if (not aa_record.get('score_d') or str(aa_record['score_d']).strip() == '') and valid_d_sum > 0:
                    aa_record['score_d'] = f"{valid_d_sum:.3f}"
                    aa_record['calculated_d'] = True

        return {
            'source': 'livemeet',
            'source_meet_id': source_meet_id,
            'meet_details': meet_details,
            'results': extracted_results
        }
    except Exception as e:
        print(f"Error in extract_livemeet_data: {e}")
        import traceback
        traceback.print_exc()
        return None

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
    import pandas as pd
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
            if str(c).upper().strip() in [cand.upper() for cand in candidates]: return c
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
    is_mag_level = any(re.match(r'^[PB]\d|^AP|^SR|^SNG|^NG|^J\d', sample_level) for x in [sample_level]) or 'PO' in sample_level
    
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
    import pandas as pd
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
    session_upper = str(session_name).upper()
    meet_name_upper = (meet_details.get('name') or "").upper()
    if "WAG" in session_upper or "WOMEN" in session_upper or "ARTW" in meet_name_upper: 
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
        'beam': 'Beam', 'wfloor': 'Floor',
        'vault': 'Vault', 'bars': 'Uneven Bars', 'floor': 'Floor' # Common KSIS variants
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
                'bonus': bonus_str,
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
