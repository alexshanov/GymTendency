
import sys
import os

filename = '/home/alex-shanov/OneDrive/AnalyticsProjects/GymTendency/livemeet_scraper.py'

with open(filename, 'r') as f:
    lines = f.readlines()

new_lines = []
skip_until = -1

# Find the markers
start_marker = "Step 1.1: Switch to \"Results by Session\" tab if possible" # Around line 99
old_loop_start = "--- GENDER TOGGLE DETECTION ---" # Around line 131
old_loop_end = "except (TimeoutException, UnexpectedAlertPresentException) as e:" # Around line 656

# We want to indent everything from start_marker to old_loop_end (exclusive)
# AND remove the old loop detection/iteration logic.

for i, line in enumerate(lines):
    if i < skip_until:
        continue
    
    # 1. Detect start of extraction block
    if start_marker in line:
        # Insert new loop here
        new_lines.append("            # --- GENDER TOGGLE DETECTION ---\n")
        new_lines.append("            # Use initial page_text captured at line 89\n")
        new_lines.append("            gender_toggle_available = \"ChangeDivGender\" in page_text\n")
        new_lines.append("            genders_to_scrape = ['F', 'M'] if gender_toggle_available else [None]\n")
        new_lines.append("\n")
        new_lines.append("            if gender_toggle_available:\n")
        new_lines.append("                print(f\"  -> Gender toggle DETECTED. Will scrape both Female and Male results.\")\n")
        new_lines.append("\n")
        new_lines.append("            for current_gender in genders_to_scrape:\n")
        new_lines.append("                gender_label = {'F': 'WAG', 'M': 'MAG'}.get(current_gender, '')\n")
        new_lines.append("                gender_suffix = f\"_{gender_label}\" if gender_label else \"\"\n")
        new_lines.append("\n")
        new_lines.append("                # Switch gender if toggle is available\n")
        new_lines.append("                if current_gender is not None:\n")
        new_lines.append("                    print(f\"  -> Switching to gender: {current_gender} ({gender_label})\")\n")
        new_lines.append("                    try:\n")
        new_lines.append("                        driver.execute_script(f\"ChangeDivGender('{current_gender}');\")\n")
        new_lines.append("                        time.sleep(5)\n")
        new_lines.append("                    except Exception as e:\n")
        new_lines.append("                        print(f\"    -> Warning: Failed to switch gender to {current_gender}: {e}\")\n")
        new_lines.append("                        continue\n")
        new_lines.append("\n")
        
        # Now we need to indent the subsequent lines until old_loop_end
        # BUT we must skip the old detection logic block
        
        # Let's find where the old loop started and ended.
        found_old_loop = False
        for j in range(i, len(lines)):
            if old_loop_start in lines[j]:
                old_loop_pos = j
                found_old_loop = True
                break
        
        if found_old_loop:
            # Lines from i to old_loop_pos should be indented
            for k in range(i, old_loop_pos):
                new_lines.append("    " + lines[k])
            
            # Now find the end of the old loop block (which was 'for current_gender in genders_to_scrape:')
            # and the lines that were already indented inside it.
            
            # The previous version had 'for current_gender in genders_to_scrape:' around line 139.
            # And 'if can_switch_level:' around line 166 (already indented).
            
            # Wait, this is tricky because I already indented some lines in the previous turn.
            
            # Let's just find the end of the extraction logic.
            end_pos = -1
            for j in range(i, len(lines)):
                if old_loop_end in lines[j]:
                    end_pos = j
                    break
            
            if end_pos != -1:
                # We want to take everything from the old loop's content and just re-indent it if needed?
                # Actually, the content from line 160 to 656 is already indented too much or too little?
                # It was indented by me with sed in Step 385.
                
                # Let's look at the current file's indentation.
                # Line 166: "                if can_switch_level:" (16 spaces)
                # It's already inside a loop!
                
                # So I just need to:
                # 1. Move the loop start EARLIER.
                # 2. Remove the OLD loop start.
                
                # If I move the loop start to 99, then the code between 99 and 130 needs to be indented.
                # The code after 131 is ALREADY indented.
                
                # Let's just do that.
                
                # 1. Replace lines 99-130 with indented version + new loop start.
                # 2. Remove lines 131-158 (the old loop stuff).
                
                skip_until = end_pos # Handled below
                
    # ... (Wait, I'll just do it manually with multi_replace_file_content, it's safer)
