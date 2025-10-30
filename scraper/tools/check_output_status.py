#!/usr/bin/env python3
"""
check_output_status.py
Quick utility to verify if LinkedIn scraper produced any CSV output.
Helps teammates debug missing output files safely.
"""

from pathlib import Path
import csv, datetime

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"

def main():
    if not OUTPUT_DIR.exists():
        print(" Output folder not found.")
        print(f"Expected: {OUTPUT_DIR}")
        return

    files = sorted(OUTPUT_DIR.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not files:
        print("⚠️ No CSV files found in output folder.")
        print(f"Checked: {OUTPUT_DIR}")
        return

    latest = files[0]
    mtime = datetime.datetime.fromtimestamp(latest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Found output file: {latest.name}")
    print(f"   Last modified: {mtime}")

    try:
        with latest.open("r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        print(f"   Rows (including header): {len(rows)}")
        if len(rows) > 1:
            print(f"   Sample columns: {rows[0]}")
        else:
            print(" File is empty (only header).")
    except Exception as e:
        print(f" Could not read CSV: {e}")

if __name__ == "__main__":
    main()

