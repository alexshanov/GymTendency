import pandas as pd
from bs4 import BeautifulSoup
import io

# The exact HTML you provided is stored here for testing
HTML_CONTENT = """
<div id="results">
	<div id="results-table"><div>		
<table class="a-results">
		<thead>
			<tr>
				<th colspan="3"></th>
				<th style="display: none;"></th>
							<th colspan="3"><img class="apparatuslogo" data-event="1" src="images/apparatus/vt.png" height="60" alt="Vault"></th>
			<th colspan="3"><img class="apparatuslogo" data-event="2" src="images/apparatus/ub.png" height="60" alt="Uneven Bars"></th>
			<th colspan="3"><img class="apparatuslogo" data-event="3" src="images/apparatus/bb.png" height="60" alt="Balance Beam"></th>
			<th colspan="3"><img class="apparatuslogo" data-event="4" src="images/apparatus/fx.png" height="60" alt="Floor"></th>
			
			<th class="selected" colspan="3"><img class="apparatuslogo" data-event="0" src="images/apparatus/aa.png" height="60" alt="All-Around"></th>
			</tr>
			<tr>
				<th>#</th><th>Athlete</th><th>Club</th><th style="display: none;">Category</th>
				<th>D</th><th>Score</th><th>Rk</th><th>D</th><th>Score</th><th>Rk</th>
				<th>D</th><th>Score</th><th>Rk</th><th>D</th><th>Score</th><th>Rk</th>
				<th>D</th><th>Score</th><th>Rk</th>
				
			</tr>
		</thead>
		<tbody>	
	<tr data-id="18671">
			<td>18671</td><td>DHARAMSI Anemone</td><td>South Edmonton Gymnastique</td><td style="display: none;">Xcel Silver - B</td>
			<td class="first">10.00</td><td class="first">9.575</td><td class="first">1</td>
			<td class="first">10.00</td><td class="first">9.933</td><td class="first">1</td>
			<td class="first">10.00</td><td class="first">9.500</td><td class="first">1</td>
			<td class="first">10.00</td><td class="first">9.516</td><td class="first">1</td>
			<td class="first">40.00</td><td class="first">38.524</td><td class="first">1</td>
		</tr>	
	<tr data-id="15729">
			<td>15729</td><td>WATT Charley</td><td>Mountain Shadows Gym Club</td><td style="display: none;">Xcel Silver - B</td>
			<td>10.00</td><td>9.125</td><td>9</td>
			<td>10.00</td><td>9.450</td><td>3</td>
			<td>10.00</td><td>9.250</td><td>3</td>
			<td>10.00</td><td>9.316</td><td>2</td>
			<td>40.00</td><td>37.141</td><td>2</td>
		</tr>
</tbody></table></div></div></div>
"""

def create_proper_dataframe(html_content):
    """
    This function takes the raw HTML content of a Kscore results table
    and correctly constructs a pandas DataFrame with the proper headers.
    This is the definitive, correct logic.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # --- Step 1: Reliably extract header components from the <thead> ---
    header_rows = soup.select('thead > tr')
    if len(header_rows) < 2:
        print("FATAL: Expected two header rows, but found fewer.")
        return None

    # Part A: Get event names from the <img> alt text in the first header row.
    event_names = [img.get('alt', 'Unknown') for img in header_rows[0].select('img.apparatuslogo')]
    
    # Part B: Get VISIBLE info column names from the second header row.
    sub_header_cells = header_rows[1].find_all(['th', 'td'])
    info_headers_raw = [
        cell.get_text(strip=True) for cell in sub_header_cells 
        # THE KEY FIX: ONLY take the cell if it's NOT hidden and NOT a score metric
        if 'display: none;' not in cell.get('style', '') and cell.get_text(strip=True) not in ['D', 'Score', 'Rk']
    ]
    
    # --- Step 2: Build the final, correct header list ---
    final_columns = []
    
    # Standardize and add the info columns
    info_column_rename_map = {'Athlete': 'Name', '#': 'Rank'}
    final_columns.extend([info_column_rename_map.get(name, name) for name in info_headers_raw])
    
    # Add the event triples
    for event in event_names:
        clean_event = event.replace(' ', '_').replace('-', '_')
        final_columns.extend([f"Result_{clean_event}_D", f"Result_{clean_event}_Score", f"Result_{clean_event}_Rnk"])

    # --- Step 3: Extract VISIBLE data cells from the <tbody> ---
    data_rows = soup.select('tbody > tr')
    all_row_data = []
    for row in data_rows:
        cells = row.find_all('td')
        # This logic correctly takes only the visible cells
        row_data = [cell.get_text(strip=True) for cell in cells if 'display: none;' not in cell.get('style', '')]
        all_row_data.append(row_data)

    if not all_row_data:
        print("No data rows found.")
        return None

    # --- Step 4: Create the DataFrame and assign headers ---
    df = pd.DataFrame(all_row_data)

    # Final check: The number of constructed headers must match the number of visible data columns
    if len(final_columns) == df.shape[1]:
        df.columns = final_columns
        return df
    else:
        print(f"FATAL: Mismatch after parsing. Header count: {len(final_columns)}, Data column count: {df.shape[1]}")
        print("Constructed Header:", final_columns)
        return None


if __name__ == "__main__":
    print("--- Testing the header creation function ---")
    
    # Call the function with the sample HTML
    final_df = create_proper_dataframe(HTML_CONTENT)
    
    if final_df is not None:
        print("\n✅ SUCCESS! DataFrame created successfully.")
        print("\nGenerated Columns:")
        # Print the columns in a readable list
        for col in final_df.columns:
            print(f"- {col}")

        print("\nDataFrame Head:")
        print(final_df.head())
    else:
        print("\n❌ FAILED to create DataFrame.")