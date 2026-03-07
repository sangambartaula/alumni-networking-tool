"""
Quick script to print scraper activity stats.
Run from project root: python scripts/scraper_stats.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

from database import get_scraper_activity, init_db


def main():
    # Ensure the scraper_activity table exists (creates if missing)
    try:
        init_db()
    except Exception:
        pass  # Table may already exist or DB may be unreachable

    rows = get_scraper_activity()
    if not rows:
        print("No scraper activity recorded yet.")
        return

    print("\n📊 Scraper Activity\n" + "─" * 50)
    for row in rows:
        email = row["email"]
        total = row["profiles_scraped"]
        last = row.get("last_scraped_at") or "Never"
        print(f"  {email}: {total} scraped (Last scraped: {last})")
    print("─" * 50 + "\n")


if __name__ == "__main__":
    main()
