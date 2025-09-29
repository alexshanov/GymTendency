import pandas as pd
import os
import glob

def extract_unique_headers(input_dir, output_filename):
    """
    Finds all '*_FINAL.csv' files in a directory, reads the header row from each,
    and saves all unique header rows to a text file.
    """
    print(f"--- Searching for final CSVs in directory: '{input_dir}' ---")
    
    # Use glob to find all files matching the pattern
    final_csv_files = glob.glob(os.path.join(input_dir, "*_FINAL.csv"))
    
    if not final_csv_files:
        print("Error: No files ending with '_FINAL.csv' were found in the directory.")
        return

    print(f"Found {len(final_csv_files)} final CSV files to process.")
    
    unique_headers = set()

    # Loop through each found file
    for filepath in final_csv_files:
        try:
            # We only need to read the very first line to get the header.
            # `nrows=0` is an efficient way to get just the columns.
            df = pd.read_csv(filepath, nrows=0)
            
            # The columns are a list of strings. We convert it to a single,
            # comma-separated string to make it storable in a set.
            header_string = ",".join(df.columns)
            
            unique_headers.add(header_string)
            
        except Exception as e:
            print(f"Warning: Could not read or process header from '{os.path.basename(filepath)}'. Reason: {e}")
            
    if not unique_headers:
        print("Could not extract any headers from the found files.")
        return
        
    # --- Save the results ---
    print(f"\nFound {len(unique_headers)} unique header formats.")
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write(f"--- Found {len(unique_headers)} unique header formats across {len(final_csv_files)} files ---\n\n")
        
        # Write each unique header to the file on a new line
        for i, header in enumerate(sorted(list(unique_headers))): # Sorted for consistent output
            f.write(f"--- HEADER FORMAT #{i+1} ---\n")
            f.write(header + "\n\n")
            
    print(f"Successfully saved all unique headers to '{output_filename}'")

# --- Main script execution ---
if __name__ == "__main__":
    
    CSV_SUBFOLDER = "CSVs"
    OUTPUT_TEXT_FILE = "unique_headers.txt"
    
    extract_unique_headers(CSV_SUBFOLDER, OUTPUT_TEXT_FILE)