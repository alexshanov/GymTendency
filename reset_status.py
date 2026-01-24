#!/usr/bin/env python3
import os
import sys

TARGET = "scraped_meets_status.json"

def main():
    if os.path.exists(TARGET):
        try:
            os.remove(TARGET)
            print(f"✅ Successfully deleted '{TARGET}'.\nThe orchestrator will now reprocessing all meets from scratch.")
        except Exception as e:
            print(f"❌ Error deleting '{TARGET}': {e}")
    else:
        print(f"ℹ️  '{TARGET}' does not exist. Status is already clean.")

if __name__ == "__main__":
    main()
