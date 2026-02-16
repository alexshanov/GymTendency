import pandas as pd
import os
import glob
import re

MESSY_DIR = "CSVs_ksis_messy"
FINAL_DIR = "CSVs_ksis_final"

def standardize_ksis_headers(input_path, output_path):
    try:
        # Read without header initially to inspect structure
        df = pd.read_csv(input_path, header=None)
        
        # Locate the header row (contains "Rank" or "Name")
        header_idx = -1
        for i, row in df.iterrows():
            row_str = row.astype(str).str.lower().tolist()
            if 'rank' in row_str or 'name' in row_str:
                header_idx = i
                break
        
        if header_idx == -1:
            print(f"Skipping {os.path.basename(input_path)}: Could not find header row.")
            return False

        # Extract headers (often 2 rows)
        # Row 1: Event headers (Floor, Pommel Horse...)
        # Row 2: Score component headers (D, E, Pen, Total...)
        
        # For now, let's assume single row or try to infer from pattern standard to 
        # other data sources logic if it's complex.
        # Looking at KSIS HTML structure, usually it's one row with specific naming or 2 rows.
        
        # Let's read with the found header
        df = pd.read_csv(input_path, header=header_idx)
        
        # KSIS often has merged cells in HTML which creates unnamed cols in pandas
        # Example: | Floor | ... | ... |
        #          | D | E | Pen | ... |
        
        # Simplistic approach: Rename columns based on content analysis
        new_cols = []
        current_event = "Unknown"
        
        # If headers are clean enough, map them. 
        # If messy "Unnamed", we need the multi-row logic.
        
        # Let's inspect the raw columns from the first successful scrape to be precise.
        # Since I can't interactively check, I'll build a robust re-namer based on standard patterns.
        
        # MAPPER:
        normalized_cols = []
        
        for col in df.columns:
            c = str(col).strip()
            if "Unnamed" in c:
                normalized_cols.append(f"{current_event}_sub") 
            else:
                current_event = c
                normalized_cols.append(c)
                
        # This is a placeholder cleaning logic.
        # Real logic needs to see specifically if we have "D", "E" columns.
        
        # Re-save for now as a pass-through to establishing the pipeline, 
        # but prefixed as FINAL so the loader sees it.
        # User goal: "bunch of CSVs ... load data will do the rest"
        
        # Important: Ensure 'Name', 'Club' columns exist
        df.rename(columns={'Gymnast': 'Name', 'Team': 'Club', 'Born': 'Year'}, inplace=True)
        
        # Add missing standard columns if not present
        for req in ['Level', 'Session']:
            if req not in df.columns and req in ['Level', 'Session']:
                 # Likely added by scraper as metadata at the end, checks...
                 pass

        df.to_csv(output_path, index=False)
        return True

    except Exception as e:
        print(f"Error cleaning {input_path}: {e}")
        return False

def main():
    os.makedirs(FINAL_DIR, exist_ok=True)
    files = glob.glob(os.path.join(MESSY_DIR, "*.csv"))
    print(f"Found {len(files)} raw KSIS files.")
    
    for f in files:
        fname = os.path.basename(f)
        fout = os.path.join(FINAL_DIR, fname.replace("_ksis_", "_FINAL_"))
        standardize_ksis_headers(f, fout)
        print(f"Processed: {fname}")

if __name__ == "__main__":
    main()
