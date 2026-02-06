import sys
from pathlib import Path

# ============================================================
# Add PROJECT ROOT to PYTHONPATH (single, clean fix)
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import os
import time
import random
import urllib.parse
import threading
import pandas as pd

# Local Modules
import config
import utils
import database_handler
from scraper import LinkedInScraper
from config import logger

# Backend
from backend.sqlite_fallback import get_connection_manager


# ============================================================
# Resume / Scrape State
# ============================================================
def load_scrape_state():
    manager = get_connection_manager()
    conn = manager.get_sqlite_connection()
    try:
        row = conn.execute(
            "SELECT mode, search_url, page FROM scrape_state WHERE id = 1"
        ).fetchone()
        if row:
            return {
                "mode": row["mode"],
                "search_url": row["search_url"],
                "page": row["page"] or 1
            }
    finally:
        conn.close()
    return None


def save_scrape_state(mode, search_url, page):
    manager = get_connection_manager()
    conn = manager.get_sqlite_connection()
    try:
        conn.execute(
            """
            UPDATE scrape_state
            SET mode = ?, search_url = ?, page = ?, updated_at = datetime('now')
            WHERE id = 1
            """,
            (mode, search_url, page)
        )
        conn.commit()
    finally:
        conn.close()


# ============================================================
# Exit Control
# ============================================================
exit_requested = False
force_exit = False
_exit_listener_active = False
session_profiles_scraped = 0


def _exit_listener():
    global exit_requested, force_exit, _exit_listener_active
    logger.info("💡 Type 'exit' to finish current profile, or 'force exit' to stop immediately.")

    while _exit_listener_active:
        try:
            cmd = input().strip().lower()
            if cmd == "force exit":
                force_exit = True
                logger.warning("🔴 FORCE EXIT requested")
                break
            elif cmd == "exit":
                exit_requested = True
                logger.warning("🟡 Graceful exit requested")
                break
        except Exception:
            break


def start_exit_listener():
    global _exit_listener_active
    _exit_listener_active = True
    threading.Thread(target=_exit_listener, daemon=True).start()


def stop_exit_listener():
    global _exit_listener_active
    _exit_listener_active = False


def should_stop():
    return exit_requested or force_exit


def check_force_exit():
    return force_exit


# ============================================================
# Helpers
# ============================================================
def wait_between_profiles():
    delay = random.uniform(config.MIN_DELAY, config.MAX_DELAY)
    logger.info(f"\n⏳ Waiting {delay:.1f}s before next profile...\n")

    for _ in range(10):
        if force_exit:
            return
        time.sleep(delay / 10)


# ============================================================
# MODES
# ============================================================
def run_names_mode(scraper, history_mgr):
    input_csv = os.getenv("INPUT_CSV", os.path.join("backend", "engineering_graduate.csv"))
    csv_path = PROJECT_ROOT / input_csv

    logger.info(f"--- MODE: Names ({csv_path}) ---")
    names = utils.load_names_from_csv(csv_path)
    school_id = "6464"

    for name in names:
        if should_stop():
            return

        q = urllib.parse.quote_plus(f'"{name}"')
        search_url = (
            f"https://www.linkedin.com/search/results/people/?"
            f"keywords={q}&schoolFilter=%5B%22{school_id}%22%5D"
        )

        scraper.driver.get(search_url)
        time.sleep(5)
        scraper.scroll_full_page()

        urls = scraper.extract_profile_urls_from_page()
        for url in urls:
            if check_force_exit():
                return

            if history_mgr.should_skip(url):
                continue

            data = scraper.scrape_profile_page(url)
            if data and database_handler.save_profile_to_csv(data):
                history_mgr.mark_as_visited(url, saved=True)

            if should_stop():
                return

            wait_between_profiles()


def run_search_mode(scraper, history_mgr):
    base_url = (
        "https://www.linkedin.com/search/results/people/"
        "?network=%5B%22O%22%5D&schoolFilter=%5B%226464%22%5D"
    )

    page = 1
    while True:
        if should_stop():
            break

        url = base_url if page == 1 else f"{base_url}&page={page}"
        scraper.driver.get(url)
        time.sleep(5)
        scraper.scroll_full_page()

        urls = scraper.extract_profile_urls_from_page()
        if not urls:
            break

        for profile_url in urls:
            if check_force_exit():
                return

            if history_mgr.should_skip(profile_url):
                continue

            data = scraper.scrape_profile_page(profile_url)
            if data and database_handler.save_profile_to_csv(data):
                history_mgr.mark_as_visited(profile_url, saved=True)

            if should_stop():
                return

            wait_between_profiles()

        page += 1


# ============================================================
# MAIN
# ============================================================
def main():
    history_mgr = database_handler.HistoryManager()
    history_mgr.sync_with_db()

    scraper = LinkedInScraper()
    scraper.setup_driver()

    try:
        if not scraper.login():
            logger.error("Login failed")
            return

        start_exit_listener()

        if config.SCRAPER_MODE == "names":
            run_names_mode(scraper, history_mgr)
        else:
            run_search_mode(scraper, history_mgr)

    except KeyboardInterrupt:
        logger.warning("Stopped by user")
    finally:
        stop_exit_listener()
        scraper.quit()


if __name__ == "__main__":
    main()
