import pandas as pd
import os
import glob

def extract_unique_headers(input_dir, output_filename):
    """
    Finds all '*_FINAL_*.csv' files, groups them by their unique result header format,
    and saves a summary file listing each format along with a count of
    the number of CSV files and total athletes for that format.
    """
    print(f"--- Searching for final CSVs in directory: '{input_dir}' ---")
    
    final_csv_files = glob.glob(os.path.join(input_dir, "*_FINAL_*.csv"))
    
    if not final_csv_files:
        print(f"Error: No files matching '*_FINAL_*.csv' were found in the directory '{input_dir}'.")
        return

    print(f"Found {len(final_csv_files)} final CSV files to process.")
    
    # --- PASS 1: Group files by their header format ---
    # This dictionary will map a header string to a list of files that have it.
    header_groups = {}

    for filepath in final_csv_files:
        try:
            # Quickly read just the header to categorize the file
            df_header = pd.read_csv(filepath, nrows=0)
            columns = df_header.columns.tolist()

            start_index = -1
            for i, col_name in enumerate(columns):
                if col_name.startswith("Result_"):
                    start_index = i
                    break

            if start_index != -1:
                header_chunk = columns[start_index:]
                header_string = ",".join(header_chunk)
                
                # If this header format is new, create an entry for it
                if header_string not in header_groups:
                    header_groups[header_string] = []
                
                # Add the current file to the list for its header format
                header_groups[header_string].append(filepath)
            else:
                print(f"Warning: No 'Result_' columns found in '{os.path.basename(filepath)}'. Skipping.")

        except Exception as e:
            print(f"Warning: Could not read header from '{os.path.basename(filepath)}'. Reason: {e}")
            
    if not header_groups:
        print("Could not extract any unique header chunks from the found files.")
        return
        
    # --- PASS 2: Calculate stats and save the results ---
    print(f"\nFound {len(header_groups)} unique result header formats. Now calculating stats...")
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write(f"--- Found {len(header_groups)} unique result header formats across {len(final_csv_files)} files ---\n\n")
        
        # Sort the headers for a consistent output file
        sorted_headers = sorted(header_groups.keys())
        
        for i, header_string in enumerate(sorted_headers):
            file_list = header_groups[header_string]
            file_count = len(file_list)
            total_athlete_count = 0
            
            # Now, read the full files for this group to count the athletes
            for filepath in file_list:
                try:
                    # Reading the full CSV to get the number of rows (athletes)
                    df_data = pd.read_csv(filepath)
                    total_athlete_count += len(df_data)
                except Exception as e:
                    print(f"Warning: Could not read data from '{os.path.basename(filepath)}' to count athletes. Reason: {e}")

            # Write the summary for this header format
            f.write(f"--- RESULT FORMAT #{i+1} ---\n")
            f.write(f"Files: {file_count}\n")
            f.write(f"Athletes: {total_athlete_count}\n")
            f.write("Columns: " + header_string + "\n\n")
            
    print(f"Successfully saved all unique headers and stats to '{output_filename}'")

# --- Main script execution ---
if __name__ == "__main__":
    
    # The folder where your final, clean CSVs are located
    FINAL_CSV_DIR = "CSVs_final" 
    OUTPUT_TEXT_FILE = "unique_result_headers_with_stats.txt"
    
    extract_unique_headers(FINAL_CSV_DIR, OUTPUT_TEXT_FILE)