import pandas as pd
import os
import glob

def extract_unique_headers(input_dir, output_filename):
    """
    Finds all '*_FINAL_*.csv' files, extracts the header chunk starting from the
    first 'Result_' column, and saves all unique chunks to a text file.
    """
    print(f"--- Searching for final CSVs in directory: '{input_dir}' ---")
    
    # Use a more specific glob pattern to find files like '..._FINAL_1.csv'
    final_csv_files = glob.glob(os.path.join(input_dir, "*_FINAL_*.csv"))
    
    if not final_csv_files:
        print(f"Error: No files matching '*_FINAL_*.csv' were found in the directory '{input_dir}'.")
        return

    print(f"Found {len(final_csv_files)} final CSV files to process.")
    
    unique_headers = set()

    # Loop through each found file
    for filepath in final_csv_files:
        try:
            # We only need to read the header row.
            df = pd.read_csv(filepath, nrows=0)
            columns = df.columns.tolist() # Get the header as a Python list

            # --- THIS IS THE NEW LOGIC YOU REQUESTED ---
            start_index = -1
            # Find the index of the first column that starts with 'Result_'
            for i, col_name in enumerate(columns):
                if col_name.startswith("Result_"):
                    start_index = i
                    break # We only need the first one, so we stop here.

            # If a result column was found, slice the list from that point
            if start_index != -1:
                header_chunk = columns[start_index:]
            else:
                # If no 'Result_' column exists, we can skip this file or log it.
                print(f"Warning: No 'Result_' columns found in '{os.path.basename(filepath)}'. Skipping.")
                continue

            # Convert the list of column names back into a single string for the set
            header_string = ",".join(header_chunk)
            
            unique_headers.add(header_string)
            
        except Exception as e:
            print(f"Warning: Could not read or process header from '{os.path.basename(filepath)}'. Reason: {e}")
            
    if not unique_headers:
        print("Could not extract any unique header chunks from the found files.")
        return
        
    # --- Save the results ---
    print(f"\nFound {len(unique_headers)} unique result header formats.")
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write(f"--- Found {len(unique_headers)} unique result header formats across {len(final_csv_files)} files ---\n\n")
        
        # Write each unique header chunk to the file
        for i, header in enumerate(sorted(list(unique_headers))):
            f.write(f"--- RESULT FORMAT #{i+1} ---\n")
            f.write(header + "\n\n")
            
    print(f"Successfully saved all unique result header chunks to '{output_filename}'")

# --- Main script execution ---
if __name__ == "__main__":
    
    # The folder where your final, clean CSVs are located
    FINAL_CSV_DIR = "CSVs_final" 
    OUTPUT_TEXT_FILE = "unique_result_headers.txt"
    
    extract_unique_headers(FINAL_CSV_DIR, OUTPUT_TEXT_FILE)