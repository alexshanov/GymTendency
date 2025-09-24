import pandas as pd
import numpy as np

def fix_csv_headers(input_filename, output_filename):
    """
    Reads a CSV with repeated double headers, merges each pair into a single,
    standardized header row, and removes the redundant second row.
    - Standardizes 'SV' and 'D' to '_D'.
    - Replaces all spaces with underscores.
    """
    print(f"--- Starting header fixing for '{input_filename}' ---")

    try:
        df = pd.read_csv(input_filename, header=None)
    except FileNotFoundError:
        print(f"Error: The input file '{input_filename}' was not found.")
        return

    rows_to_drop = []
    
    for i in range(len(df) - 1):
        current_row = df.iloc[i].astype(str).values
        next_row = df.iloc[i + 1].astype(str).values

        if 'Name' in current_row:
            main_header = pd.Series(current_row).ffill()
            sub_header = pd.Series(next_row)
            
            new_header = []
            for h1, h2 in zip(main_header, sub_header):
                h1_clean = str(h1).strip().replace(' ', '_')
                h2_clean = str(h2).strip()
                
                # Standardize 'SV' and 'D' to '_D'
                if h2_clean in ['SV', 'D']:
                    h2_clean = 'D'
                
                # Combine headers
                if h2_clean in ['D', 'Score', 'Rnk'] and 'nan' not in h1_clean and 'Provincial' not in h1_clean:
                    new_header.append(f"{h1_clean}_{h2_clean}")
                else:
                    new_header.append(h1_clean)
            
            df.iloc[i] = new_header
            rows_to_drop.append(i + 1)

    df_cleaned = df.drop(rows_to_drop).reset_index(drop=True)
    
    df_cleaned.to_csv(output_filename, index=False, header=False)
    
    print("\n--- Header Fixing Complete ---")
    print(f"Processed and standardized {len(rows_to_drop)} header pairs.")
    print(f"Output saved to '{output_filename}'")

# --- Main script execution ---
MESSY_FILENAME = "Gymnastics_Meet_Results_MESSY.csv"
FIXED_FILENAME = "Gymnastics_Meet_Results_Headers_Fixed.csv"
fix_csv_headers(MESSY_FILENAME, FIXED_FILENAME)