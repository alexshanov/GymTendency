import os
import subprocess
import sys
import time
import argparse

def run_script(script_name, args=None):
    """
    Runs a python script and waits for it to finish.
    """
    print(f"\n{'='*60}")
    print(f"🚀 RUNNING: {script_name}")
    print(f"{'='*60}\n")
    
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
        
    start_time = time.time()
    result = subprocess.run(cmd, capture_output=False)
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
    
    parser = argparse.ArgumentParser(description="Run the GymTendency data pipeline.")
    parser.add_argument("--sample", type=int, help="Process every Nth file for loaders (e.g. 10)")
    args = parser.parse_args()
    
    scripts = [
        ("create_db.py", []),
        ("load_orchestrator.py", ["--sample", str(args.sample)] if args.sample else []),
        ("ksis_load_data.py", []),
        ("create_silver_tables.py", [])
    ]
    
    overall_success = True
    
    for script, script_args in scripts:
        if not os.path.exists(script):
            print(f"⚠️  WARNING: Script '{script}' not found. Skipping.")
            continue
            
        if not run_script(script, script_args):
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
