import pandas as pd
import numpy as np

def unify_and_clean_data(input_filename, output_filename):
    """
    Reads a file with cleaned-but-repeated headers, verifies their uniformity
    (ignoring the last two columns), and produces a final clean CSV.
    """
    print(f"--- Starting final cleaning and unification for '{input_filename}' ---")

    try:
        df = pd.read_csv(input_filename, header=None, dtype=str)
    except FileNotFoundError:
        print(f"Error: The input file '{input_filename}' was not found.")
        return

    # 1. Identify all header rows and the master header
    header_rows = df[df[0].astype(str).str.strip() == '#']
    if header_rows.empty:
        print("Error: Could not find any header rows (containing '#' in the first column).")
        return
        
    master_header = header_rows.iloc[0].tolist()
    print(f"Master header identified with {len(master_header)} columns.")

    # 2. Verify all other header rows are identical (with your requested slicing)
    all_headers_match = True
    
    # --- THIS IS YOUR MODIFIED LOGIC ---
    # We will compare all columns EXCEPT the last two.
    num_cols_to_compare = len(master_header) - 2
    master_header_slice = master_header[:num_cols_to_compare]
    print(f"Verifying the first {num_cols_to_compare} columns of all headers...")
    # --- END MODIFICATION ---

    for index, row in header_rows.iloc[1:].iterrows():
        # Slice the current row's header to match the master slice
        current_header_slice = row.tolist()[:num_cols_to_compare]
        
        if current_header_slice != master_header_slice:
            print(f"Warning: Header mismatch found at row {index}. This may cause issues.")
            all_headers_match = False
            # For debugging:
            # print("Master Slice:", master_header_slice)
            # print("Current Slice:", current_header_slice)

    if all_headers_match:
        print("Verification complete: All core headers are identical.")

    # 3. Clean and Combine
    data_rows = df[df[0].astype(str).str.strip() != '#']
    
    clean_df = pd.DataFrame(data_rows.values)
    
    num_data_cols = clean_df.shape[1]
    clean_df.columns = master_header[:num_data_cols]

    clean_df = clean_df[pd.to_numeric(clean_df.get('Age'), errors='coerce').notna()]
    
    if '#' in clean_df.columns:
        clean_df = clean_df.drop(columns=['#'])
    
    clean_df = clean_df.reset_index(drop=True)
    
    clean_df.to_csv(output_filename, index=False)
    
    print("\n--- Final Cleaning Complete ---")
    print(f"Processed {len(clean_df)} athlete data rows.")
    print(f"Final clean data saved to '{output_filename}'")
    print("\nFinal Data Preview:")
    print(clean_df.head())

# --- Main script execution ---
HEADERS_FIXED_FILENAME = "Gymnastics_Meet_Results_Headers_Fixed.csv"
FINAL_CLEAN_FILENAME = "Gymnastics_Meet_Results_FINAL.csv"
unify_and_clean_data(HEADERS_FIXED_FILENAME, FINAL_CLEAN_FILENAME)