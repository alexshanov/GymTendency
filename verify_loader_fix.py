import pandas as pd
import os

def mock_loader_logic(columns):
    raw_apps = ['Vault', 'Uneven_Bars', 'Beam', 'Floor', 'Pommel_Horse', 'Rings', 'Parallel_Bars', 'High_Bar', 'AllAround',
                'Uneven Bars', 'Pommel Horse', 'Parallel Bars', 'High Bar', 'All Around']
    new_headers = []
    seen_counts = {}
    for col in columns:
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
            
            if proposed_name in columns:
                new_headers.append(col)
            else:
                new_headers.append(proposed_name)
        else:
            new_headers.append(col)
    return new_headers

# Test with BYEVENT columns from the problematic meet
test_cols = [
    'Level', 'Meet', 'Group', 'Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5', 
    'Pommel Horse', 'Pommel Horse.1', 'Pommel Horse.2', 'Parallel Bars', 'Parallel Bars.1', 'Parallel Bars.2'
]

print("Original columns:", test_cols)
new_cols = mock_loader_logic(test_cols)
print("Normalized columns:", new_cols)

# Check if Pommel Horse was correctly mapped
assert 'Result_Pommel Horse_D' in new_cols
assert 'Result_Pommel Horse_Score' in new_cols
assert 'Result_Pommel Horse_Rnk' in new_cols
print("\nSUCCESS: Pommel Horse with spaces is correctly identified and mapped to Result_ headers!")
