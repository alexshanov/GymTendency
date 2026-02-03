import json
import os
from etl_functions import standardize_level_name

def test_standardization():
    # Load the aliases as kscore_load_data.py would
    alias_file = "kscore_level_aliases.json"
    with open(alias_file, 'r') as f:
        level_aliases = json.load(f)
    
    test_cases = [
        ("Provincial 2A", "P2A"),
        ("Provincial 1-D", "P1D"),
        ("Provincial 1 E", "P1E"),
        ("P1A", "P1A"),
        ("Provincial 2C", "P2C"),
        ("P2", "Provincial 2"), # Should be untouched per instructions
        ("Provincial 2", "Provincial 2"),
        ("Provincial 1A", "P1A"),
    ]
    
    passed = True
    for input_level, expected in test_cases:
        actual = standardize_level_name(input_level, level_aliases)
        if actual == expected:
            print(f"PASS: '{input_level}' -> '{actual}'")
        else:
            print(f"FAIL: '{input_level}' -> '{actual}' (Expected: '{expected}')")
            passed = False
    
    if passed:
        print("\nAll level standardization tests PASSED!")
    else:
        print("\nSome tests FAILED.")
        exit(1)

if __name__ == "__main__":
    test_standardization()
