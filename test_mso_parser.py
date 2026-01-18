
import re

def parse_cell_value_original(cell_str):
    """
    The current (problematic) implementation from mso_load_data.py
    """
    if not isinstance(cell_str, str) or not cell_str.strip():
        return None, None, None, None
        
    parts = cell_str.split()
    if not parts:
        return None, None, None, None
    
    score_final = None
    d_score = None
    rank_numeric = None
    rank_text = None
    
    def is_float(s):
        try:
            float(s)
            return True
        except ValueError:
            return False
            
    def parse_rank(s):
        clean = re.sub(r'\D', '', s)
        return int(clean) if clean else None

    # Logic based on token count
    # Default assumption: Last token is Score
    if is_float(parts[-1]):
        score_final = float(parts[-1])
        remaining = parts[:-1]
    else:
        return None, None, None, None
        
    if not remaining:
        pass
    
    elif len(remaining) == 1:
        # Case: "1 9.500" -> Rank 1, Score 9.5
        token = remaining[0]
        if is_float(token) and '.' in token:
            d_score = float(token) # Likely D-score if it has decimal
        else:
            rank_text = token
            rank_numeric = parse_rank(token)
            
    elif len(remaining) == 2:
        rank_token = remaining[0]
        d_token = remaining[1]
        rank_text = rank_token
        rank_numeric = parse_rank(rank_token)
        if is_float(d_token):
            d_score = float(d_token)
            
    return score_final, d_score, rank_numeric, rank_text

def parse_cell_value_new(cell_str):
    """
    Proposed strict logic handling [Score] [Rank] format
    """
    if not isinstance(cell_str, str) or not cell_str.strip():
        return None, None, None, None
        
    parts = cell_str.split()
    
    score_final = None
    d_score = None
    rank_numeric = None
    rank_text = None
    
    def is_float(s):
        try:
            float(s)
            return True
        except ValueError:
            return False
            
    def parse_rank(s):
        clean = re.sub(r'\D', '', s)
        return int(clean) if clean else None
        
    def is_likely_rank(s):
        # Rank is typically integer-like, or "T"+int. 
        # Should NOT be a float like "8.500".
        if is_float(s) and '.' in s: return False # It's a score
        if parse_rank(s) is not None: return True
        return False
        
    # --- LOGIC ---
    
    # 1. Single Token
    if len(parts) == 1:
        if is_float(parts[0]):
            return float(parts[0]), None, None, None
            
    # 2. Two Tokens: "[Score] [Rank]" OR "[Rank] [Score]"
    if len(parts) == 2:
        p0, p1 = parts[0], parts[1]
        
        # Case A: "8.800 5" (Score Rank) -> Specific MSO format
        if is_float(p0) and is_likely_rank(p1):
            return float(p0), None, parse_rank(p1), p1
            
        # Case B: "1 9.500" (Rank Score) -> Traditional
        if is_likely_rank(p0) and is_float(p1):
            return float(p1), None, parse_rank(p0), p0
            
    # 3. Three Tokens: "1 4.5 9.500" (Rank D-Score Score) 
    if len(parts) == 3:
        p0, p1, p2 = parts[0], parts[1], parts[2]
        # Assume R D S
        if is_likely_rank(p0) and is_float(p1) and is_float(p2):
             return float(p2), float(p1), parse_rank(p0), p0

    # Fallback to original logic if no patterns match? 
    # Or just return whatever we found.
    
    return None, None, None, None

# --- TESTING ---
test_cases = [
    "9.500",      # Pure Score
    "8.800 5",    # Score Rank (The User Case!)
    "35.200 3",   # High Score Rank
    "1 9.200",    # Rank Score
    "T2 9.250",   # Tied Rank Score
    "1 3.5 9.5",  # Rank D Score
    "9.250 2T"    # Score Rank (Tied)
]

print(f"{'INPUT':<15} | {'ORIGINAL (S, D, R)':<25} | {'NEW (S, D, R)':<25}")
print("-" * 70)

for val in test_cases:
    orig = parse_cell_value_original(val)
    new = parse_cell_value_new(val)
    print(f"{val:<15} | {str(orig[0:3]):<25} | {str(new[0:3]):<25}")
