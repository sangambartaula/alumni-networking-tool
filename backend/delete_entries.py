#!/usr/bin/env python3
import csv
import logging
import os
import sqlite3
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BACKEND_DIR.parent
DELETE_FILE = BACKEND_DIR / "delete_entries.txt"
SQLITE_PATH = BACKEND_DIR / "alumni_backup.db"
VISITED_HISTORY_PATH = PROJECT_ROOT / "scraper" / "output" / "visited_history.csv"
ALUMNI_CSV_PATH = PROJECT_ROOT / "scraper" / "output" / "UNT_Alumni_Data.csv"
FLAGGED_REVIEW_PATH = PROJECT_ROOT / "scraper" / "output" / "flagged_for_review.txt"


def normalize_url(url):
    if url is None:
        return None
    text = str(url).strip()
    if not text:
        return None
    return text.rstrip("/")


def load_delete_urls(path):
    urls = []
    if not path.exists():
        logger.warning("Delete list not found at %s", path)
        return urls

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        normalized = normalize_url(line)
        if normalized:
            urls.append(normalized)

    return sorted(set(urls))


def with_trailing_variants(urls):
    variants = set()
    for url in urls:
        variants.add(url)
        variants.add(url + "/")
    return sorted(variants)


def delete_from_mysql(urls):
    host = os.getenv("MYSQLHOST")
    user = os.getenv("MYSQLUSER")
    password = os.getenv("MYSQLPASSWORD")
    database = os.getenv("MYSQL_DATABASE")
    port = int(os.getenv("MYSQLPORT", "3306"))

    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
    )

    removed = {
        "mysql_alumni": 0,
        "mysql_visited_profiles": 0,
    }

    variants = with_trailing_variants(urls)
    placeholders = ",".join(["%s"] * len(variants))

    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                f"SELECT id FROM alumni WHERE linkedin_url IN ({placeholders})",
                tuple(variants),
            )
            alumni_rows = cur.fetchall() or []
            alumni_ids = [row["id"] for row in alumni_rows]

            if alumni_ids:
                alumni_id_placeholders = ",".join(["%s"] * len(alumni_ids))
                cur.execute(
                    f"DELETE FROM notes WHERE alumni_id IN ({alumni_id_placeholders})",
                    tuple(alumni_ids),
                )
                cur.execute(
                    f"DELETE FROM user_interactions WHERE alumni_id IN ({alumni_id_placeholders})",
                    tuple(alumni_ids),
                )

            cur.execute(
                f"DELETE FROM alumni WHERE linkedin_url IN ({placeholders})",
                tuple(variants),
            )
            removed["mysql_alumni"] = cur.rowcount

            cur.execute(
                f"DELETE FROM visited_profiles WHERE linkedin_url IN ({placeholders})",
                tuple(variants),
            )
            removed["mysql_visited_profiles"] = cur.rowcount

        conn.commit()
    finally:
        conn.close()

    return removed


def delete_from_sqlite(urls):
    removed = {
        "sqlite_alumni": 0,
        "sqlite_visited_profiles": 0,
    }

    if not SQLITE_PATH.exists():
        logger.warning("SQLite file not found at %s", SQLITE_PATH)
        return removed

    conn = sqlite3.connect(SQLITE_PATH)
    variants = with_trailing_variants(urls)
    placeholders = ",".join(["?"] * len(variants))

    try:
        cur = conn.cursor()
        cur.execute(f"SELECT id FROM alumni WHERE linkedin_url IN ({placeholders})", tuple(variants))
        alumni_ids = [row[0] for row in cur.fetchall()]

        if alumni_ids:
            alumni_id_placeholders = ",".join(["?"] * len(alumni_ids))
            cur.execute(f"DELETE FROM notes WHERE alumni_id IN ({alumni_id_placeholders})", tuple(alumni_ids))
            cur.execute(
                f"DELETE FROM user_interactions WHERE alumni_id IN ({alumni_id_placeholders})",
                tuple(alumni_ids),
            )

        cur.execute(f"DELETE FROM alumni WHERE linkedin_url IN ({placeholders})", tuple(variants))
        removed["sqlite_alumni"] = cur.rowcount

        cur.execute(f"DELETE FROM visited_profiles WHERE linkedin_url IN ({placeholders})", tuple(variants))
        removed["sqlite_visited_profiles"] = cur.rowcount

        conn.commit()
    finally:
        conn.close()

    return removed


def remove_from_csv(path, urls, url_columns):
    if not path.exists():
        return 0

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    if not fieldnames:
        return 0

    keep = []
    removed = 0
    for row in rows:
        match = False
        for col in url_columns:
            value = normalize_url(row.get(col))
            if value and value in urls:
                match = True
                break
        if match:
            removed += 1
        else:
            keep.append(row)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(keep)

    return removed


def remove_from_flagged_file(path, urls):
    if not path.exists():
        return 0

    lines = path.read_text(encoding="utf-8").splitlines()
    keep = []
    removed = 0

    for line in lines:
        if not line.strip():
            continue
        url = normalize_url(line.split("#", 1)[0])
        if url and url in urls:
            removed += 1
            continue
        keep.append(line)

    path.write_text("\n".join(keep) + ("\n" if keep else ""), encoding="utf-8")
    return removed


def main():
    urls = load_delete_urls(DELETE_FILE)
    if not urls:
        logger.info("No URLs to delete. Add one URL per line in %s", DELETE_FILE)
        return

    logger.info("Deleting %d URLs from DB and local files...", len(urls))
    for url in urls:
        logger.info(" - %s", url)

    summary = {}
    summary.update(delete_from_mysql(urls))
    summary.update(delete_from_sqlite(urls))
    summary["visited_history_csv"] = remove_from_csv(VISITED_HISTORY_PATH, set(urls), ["profile_url"])
    summary["alumni_csv"] = remove_from_csv(ALUMNI_CSV_PATH, set(urls), ["linkedin_url", "profile_url"])
    summary["flagged_for_review_txt"] = remove_from_flagged_file(FLAGGED_REVIEW_PATH, set(urls))

    logger.info("Done. Removed rows:")
    for key in sorted(summary.keys()):
        logger.info(" - %s: %s", key, summary[key])


if __name__ == "__main__":
    main()
