import pandas as pd

def clean_scraped_csv(input_filename, output_filename):
    """
    Reads the messy, concatenated CSV file and produces a clean,
    properly formatted single CSV.
    """
    print(f"--- Starting the cleaning process for '{input_filename}' ---")

    try:
        df = pd.read_csv(input_filename)
    except FileNotFoundError:
        print(f"Error: The input file '{input_filename}' was not found.")
        return

    correct_headers = [
        '#', 'Name', 'Club', 'Level', 'Prov', 'Age',
        'Floor_SV', 'Floor_Score', 'Floor_Rnk',
        'Pommel Horse_SV', 'Pommel Horse_Score', 'Pommel Horse_Rnk',
        'Rings_SV', 'Rings_Score', 'Rings_Rnk',
        'Vault_SV', 'Vault_Score', 'Vault_Rnk',
        'Parallel Bars_SV', 'Parallel Bars_Score', 'Parallel Bars_Rnk',
        'High Bar_SV', 'High Bar_Score', 'High Bar_Rnk',
        'AllAround_SV', 'AllAround_Score', 'AllAround_Rnk',
        'Event'
    ]

    df.rename(columns={'Unnamed: 1_level_0': 'Name'}, inplace=True)
    df_filtered = df[df['Name'].notna() & (df['Name'] != 'Name')].copy()

    column_map = {
        '#': 'Unnamed: 0_level_0', 'Name': 'Name', 'Club': 'Unnamed: 2_level_0',
        'Level': 'Unnamed: 3_level_0', 'Prov': 'Unnamed: 4_level_0', 'Age': 'Unnamed: 5_level_0',
        'Floor_SV': 'Unnamed: 6_level_0', 'Floor_Score': 'Unnamed: 7_level_0', 'Floor_Rnk': 'Unnamed: 8_level_0',
        'Pommel Horse_SV': 'Unnamed: 9_level_0', 'Pommel Horse_Score': 'Unnamed: 10_level_0', 'Pommel Horse_Rnk': 'Unnamed: 11_level_0',
        'Rings_SV': 'Unnamed: 12_level_0', 'Rings_Score': 'Unnamed: 13_level_0', 'Rings_Rnk': 'Unnamed: 14_level_0',
        'Vault_SV': 'Unnamed: 15_level_0', 'Vault_Score': 'Unnamed: 16_level_0', 'Vault_Rnk': 'Unnamed: 17_level_0',
        'Parallel Bars_SV': 'Unnamed: 18_level_0', 'Parallel Bars_Score': 'Unnamed: 19_level_0', 'Parallel Bars_Rnk': 'Unnamed: 20_level_0',
        'High Bar_SV': 'Unnamed: 21_level_0', 'High Bar_Score': 'Unnamed: 22_level_0', 'High Bar_Rnk': 'Unnamed: 23_level_0',
        'AllAround_SV': 'Unnamed: 24_level_0', 'AllAround_Score': 'Unnamed: 25_level_0', 'AllAround_Rnk': 'Unnamed: 26_level_0',
        'Event': 'Event'
    }
    
    # Check which of the expected 'Unnamed' columns actually exist in the messy file
    existing_cols = [col for col in column_map.values() if col in df_filtered.columns]
    
    # Create a new map with only the columns that were found
    reversed_map = {v: k for k, v in column_map.items()}
    final_headers = [reversed_map[col] for col in existing_cols]

    # Extract only the columns we need
    data_to_process = df_filtered[existing_cols].copy()
    data_to_process.columns = final_headers

    # --- FINAL POLISHING STEPS ---
    
    # 1. Drop the '#' column as it's not useful
    if '#' in data_to_process.columns:
        data_to_process = data_to_process.drop(columns=['#'])
        
    # 2. Reset the DataFrame index to get clean, sequential row numbers
    data_to_process = data_to_process.reset_index(drop=True)
    
    # --- END POLISHING STEPS ---
    
    data_to_process.to_csv(output_filename, index=False)
    
    print(f"--- Cleaning complete! ---")
    print(f"Processed {len(data_to_process)} athlete entries.")
    print(f"Clean data saved to '{output_filename}'")
    print("\nFirst 5 rows of the final clean data:")
    print(data_to_process.head())

# --- Main script execution ---
MESSY_FILENAME = 'Gymnastics_Meet_Results_SUCCESS.csv'
CLEAN_FILENAME = 'Gymnastics_Meet_Results_Cleaned.csv'
clean_scraped_csv(MESSY_FILENAME, CLEAN_FILENAME)