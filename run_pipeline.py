import os
import subprocess
import sys
import time

def run_script(script_name):
    """
    Runs a python script and waits for it to finish.
    """
    print(f"\n{'='*60}")
    print(f"🚀 RUNNING: {script_name}")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    result = subprocess.run([sys.executable, script_name], capture_output=False)
    end_time = time.time()
    
    duration = end_time - start_time
    
    if result.returncode == 0:
        print(f"\n✅ SUCCESS: {script_name} completed in {duration:.2f} seconds.")
        return True
    else:
        print(f"\n❌ FAILED: {script_name} failed with return code {result.returncode}.")
        return False

def main():
    print("--- 🏋️‍♂️ GYM TENDENCY DATA PIPELINE ORCHESTRATOR 🏋️‍♀️ ---")
    print("This script will run all data loaders and refresh the analytics tables.")
    
    scripts = [
        "kscore_load_data.py",
        "livemeet_load_data.py",
        "mso_load_data.py",
        "create_gold_tables.py"
    ]
    
    overall_success = True
    
    for script in scripts:
        if not os.path.exists(script):
            print(f"⚠️  WARNING: Script '{script}' not found. Skipping.")
            continue
            
        if not run_script(script):
            overall_success = False
            # We continue even if one fails, as they are mostly independent until gold tables
            
    print(f"\n{'='*60}")
    if overall_success:
        print("✨ PIPELINE COMPLETED SUCCESSFULLY ✨")
    else:
        print("⚠️  PIPELINE COMPLETED WITH ERRORS")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
