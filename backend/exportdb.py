"""
Export the alumni table from MySQL (Railway) to a local CSV.

Usage:
    python scraper/exportdb.py
    python scraper/exportdb.py --output custom_name.csv
"""
import sys
import os
import csv
import argparse
from pathlib import Path
from dotenv import load_dotenv
import mysql.connector

# Add backend to path for database module
sys.path.insert(0, str(Path(__file__).parent / "backend"))

load_dotenv()

# Columns to export (no timestamps like scraped_at, created_at, updated_at)
EXPORT_COLUMNS = [
    "first_name",
    "last_name",
    "headline",
    "location",
    "current_job_title",
    "company",
    "major",
    "degree",
    "grad_year",
    "linkedin_url",
    "school_start_date",
    "job_start_date",
    "job_end_date",
    "working_while_studying",
    "exp2_title",
    "exp2_company",
    "exp2_dates",
    "exp3_title",
    "exp3_company",
    "exp3_dates",
    "education",
]


def get_mysql_connection():
    """Connect directly to MySQL (Railway)."""
    return mysql.connector.connect(
        host=os.getenv("MYSQLHOST"),
        user=os.getenv("MYSQLUSER"),
        password=os.getenv("MYSQLPASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQLPORT", 3306)),
    )


def export_alumni_to_csv(output_path: Path):
    """Fetch all alumni rows and write selected columns to CSV."""
    print("🔌 Connecting to Railway MySQL...")
    conn = get_mysql_connection()

    try:
        cursor = conn.cursor(dictionary=True)

        # Build SELECT with only the columns we want
        cols_sql = ", ".join(EXPORT_COLUMNS)
        query = f"SELECT {cols_sql} FROM alumni ORDER BY last_name, first_name"

        print("🔍 Fetching alumni data...")
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print("❌ No alumni records found in database.")
            return

        print(f"✅ Found {len(rows)} alumni records.")

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS)
            writer.writeheader()
            for row in rows:
                # Replace None with empty string for cleaner CSV
                cleaned = {k: (v if v is not None else "") for k, v in row.items()}
                writer.writerow(cleaned)

        print(f"💾 Exported {len(rows)} records to {output_path}")

    finally:
        conn.close()
        print("🔌 Connection closed.")


def main():
    parser = argparse.ArgumentParser(description="Export alumni table to CSV")
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output CSV file path (default: scraper/output/alumni_export.csv)",
    )
    args = parser.parse_args()

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(__file__).parent / "output" / "alumni_export.csv"

    export_alumni_to_csv(output_path)


if __name__ == "__main__":
    main()