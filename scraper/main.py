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
from datetime import datetime, timedelta, timezone

# Local Modules
import config
import utils
import database_handler
from scraper import LinkedInScraper
from config import logger

# Backend
from backend.sqlite_fallback import get_connection_manager
from defense.navigator import SafeNavigator


# ============================================================
# Resume / Scrape State
# ============================================================
def load_scrape_state():
    """
    Load the last known scrape position (search URL and page number).
    This allows the scraper to resume from where it left off after an
    intentional exit or a crash, preventing redundant scraping of early pages.
    """
    manager = get_connection_manager()
    conn = manager.get_sqlite_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scrape_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                mode TEXT,
                search_url TEXT,
                page INTEGER DEFAULT 1,
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO scrape_state (id, mode, search_url, page, updated_at)
            VALUES (1, NULL, NULL, 1, datetime('now'))
            """
        )
        conn.commit()

        row = conn.execute(
            "SELECT mode, search_url, page, updated_at FROM scrape_state WHERE id = 1"
        ).fetchone()
        if row:
            return {
                "mode": row["mode"],
                "search_url": row["search_url"],
                "page": row["page"] or 1,
                "updated_at": row["updated_at"],
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
            CREATE TABLE IF NOT EXISTS scrape_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                mode TEXT,
                search_url TEXT,
                page INTEGER DEFAULT 1,
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO scrape_state (id, mode, search_url, page, updated_at)
            VALUES (1, NULL, NULL, 1, datetime('now'))
            """
        )
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


def _parse_state_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _is_recent_state(updated_at, max_age_days):
    state_time = _parse_state_timestamp(updated_at)
    if not state_time:
        return False
    if state_time.tzinfo is not None:
        now = datetime.now(state_time.tzinfo)
    else:
        # sqlite datetime('now') values are UTC strings without tz info.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
    return (now - state_time) <= timedelta(days=max_age_days)


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
    logger.info(f"Next profile in {delay:.0f}s")

    for _ in range(10):
        if force_exit:
            return
        time.sleep(delay / 10)


def _save_and_track(data, input_url, history_mgr):
    """
    Save profile to CSV, handling canonical URL dedup.
    
    LinkedIn often uses multiple URLs for the same profile (vanity vs ID-based).
    If redirected input_url → canonical_url:
      - We mark both URLs in history so neither is re-visited.
      - The CSV utility uses drop_duplicates to replace older entries 
        sharing the same canonical URL with the latest data.
    """
    if not data or data == "PAGE_NOT_FOUND":
        return False
    
    canonical_url = data.get("profile_url", input_url)
    original_url = data.pop("_original_url", None)  # Remove internal key before save
    
    # Check if canonical URL was already saved in this session under a different input URL
    if original_url and history_mgr.should_skip(canonical_url):
        logger.info(f"  ↩️  Profile Already Visited, Skipping: {canonical_url}")
        # Still mark the input URL so we don't try it again
        history_mgr.mark_as_visited(input_url, saved=True)
        return False
    
    if database_handler.save_profile_to_csv(data):
        # Mark canonical URL as visited
        history_mgr.mark_as_visited(canonical_url, saved=True)
        # Also mark the original input URL so it's not re-visited
        if original_url and original_url.rstrip('/') != canonical_url.rstrip('/'):
            history_mgr.mark_as_visited(original_url, saved=True)
        return True
    return False


# ============================================================
# MODES
# ============================================================
def run_names_mode(scraper, nav, history_mgr):
    input_csv = os.getenv("INPUT_CSV", "engineering_graduates.csv")
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

        ok = nav.get(search_url)
        if not ok:
            logger.warning("⚠️ Search page unhealthy. Skipping this name.")
            continue

        time.sleep(5)
        scraper.scroll_full_page()

        urls = scraper.extract_profile_urls_from_page()
        for url in urls:
            if check_force_exit():
                return

            if config.is_blocked_url(url):
                continue

            if history_mgr.should_skip(url):
                logger.info(f"  ↩️  Profile Already Visited, Skipping: {url}")
                continue

            # NOTE: scrape_profile_page likely handles its own navigation.
            data = scraper.scrape_profile_page(url)

            if data == "PAGE_NOT_FOUND":
                logger.warning(f"  💀 Dead URL skipped: {url}")
                continue

            if data and data != "PAGE_NOT_FOUND":
                _save_and_track(data, url, history_mgr)

            if should_stop():
                return

            wait_between_profiles()


def run_search_mode(scraper, nav, history_mgr):
    base_url = (
        "https://www.linkedin.com/search/results/people/"
        "?network=%5B%22O%22%5D&schoolFilter=%5B%226464%22%5D"
    )

    page = 1
    state = load_scrape_state()
    if (
        state
        and state.get("mode") == "search"
        and state.get("search_url") == base_url
        and _is_recent_state(state.get("updated_at"), config.SCRAPE_RESUME_MAX_AGE_DAYS)
    ):
        try:
            saved_page = int(state.get("page") or 1)
        except (TypeError, ValueError):
            saved_page = 1
        page = saved_page if saved_page >= 1 else 1
        logger.info(
            f"↪ Resuming search mode from page {page} "
            f"(state age <= {config.SCRAPE_RESUME_MAX_AGE_DAYS} days)"
        )
    else:
        logger.info("↪ Starting search mode from page 1")

    while True:
        if should_stop():
            break

        url = base_url if page == 1 else f"{base_url}&page={page}"
        save_scrape_state("search", base_url, page)
        logger.info(f"📄 Search page {page}")

        ok = nav.get(url)
        if not ok:
            logger.warning("Search page unhealthy. Stopping search loop.")
            break

        time.sleep(5)
        scraper.scroll_full_page()

        urls = scraper.extract_profile_urls_from_page()
        if not urls:
            # Reached the end of results; restart from page 1 next run.
            save_scrape_state("search", base_url, 1)
            break

        for profile_url in urls:
            if check_force_exit():
                return

            if config.is_blocked_url(profile_url):
                continue

            if history_mgr.should_skip(profile_url):
                logger.info(f"  ↩️  Profile Already Visited, Skipping: {profile_url}")
                continue

            # NOTE: scrape_profile_page likely handles its own navigation.
            data = scraper.scrape_profile_page(profile_url)

            if data == "PAGE_NOT_FOUND":
                logger.warning(f"  💀 Dead URL skipped: {profile_url}")
                continue

            if data and data != "PAGE_NOT_FOUND":
                _save_and_track(data, profile_url, history_mgr)

            if should_stop():
                return

            wait_between_profiles()

        page += 1
        save_scrape_state("search", base_url, page)


def run_review_mode(scraper, nav, history_mgr):
    """
    Process profiles from flagged_for_review.txt file.
    These are profiles that need to be re-scraped.
    Tracks dead/removed profiles and offers to clean them from DB at the end.
    """
    flagged_file = PROJECT_ROOT / "scraper" / "output" / "flagged_for_review.txt"
    
    if not flagged_file.exists():
        logger.error(f"❌ Flagged file not found: {flagged_file}")
        return
    
    # Read URLs from file, filtering out blocked profiles
    from config import is_blocked_url
    with open(flagged_file, 'r') as f:
        raw_urls = [line.strip() for line in f if line.strip() and line.strip().startswith('http')]
    
    # Strip comment suffixes (e.g. "url # flagged for review (bulk)")
    urls = []
    for raw in raw_urls:
        url = raw.split('#')[0].strip() if '#' in raw else raw
        if url and not is_blocked_url(url):
            urls.append(url)
    
    if not urls:
        logger.info("📋 No URLs in flagged_for_review.txt")
        return
    
    logger.info(f"📋 Review mode: {len(urls)} profiles to re-scrape")
    
    dead_urls = []  # Collect profiles that no longer exist
    
    for profile_url in urls:
        if should_stop():
            break
        
        if check_force_exit():
            break
        
        logger.debug(f"Re-scraping: {profile_url}")
        
        try:
            # Scrape the profile (bypassing history check since we're re-reviewing)
            data = scraper.scrape_profile_page(profile_url)
            
            # Handle dead/removed profiles
            if data == "PAGE_NOT_FOUND":
                dead_urls.append(profile_url)
                continue
            
            if data and data != "PAGE_NOT_FOUND":
                _save_and_track(data, profile_url, history_mgr)
                logger.debug(f"Updated: {data.get('name', 'Unknown')}")
        except Exception as e:
            msg = str(e).lower()
            if "invalid session id" in msg or "no such window" in msg or "target window already closed" in msg:
                logger.error(f"❌ WebDriver died ({e}). Restarting...")
                try:
                    scraper.quit()
                    time.sleep(2)
                    scraper.setup_driver()
                    scraper.login()
                    # Retry once
                    logger.info(f"  🔄 Retrying: {profile_url}")
                    data = scraper.scrape_profile_page(profile_url)
                    if data == "PAGE_NOT_FOUND":
                        dead_urls.append(profile_url)
                    elif data and data != "PAGE_NOT_FOUND":
                        _save_and_track(data, profile_url, history_mgr)
                except Exception as retry_e:
                    logger.error(f"❌ Retry failed: {retry_e}")
            else:
                logger.error(f"❌ Error processing {profile_url}: {e}")
        
        if should_stop():
            break
        
        wait_between_profiles()
    
    logger.info("✅ Review mode complete")
    
    # ── Report dead URLs ──────────────────────────────────────
    if dead_urls:
        print("\n" + "=" * 60)
        print(f"⚠️  {len(dead_urls)} DEAD / REMOVED PROFILES DETECTED:")
        print("=" * 60)
        for url in dead_urls:
            print(f"  💀 {url}")
        print("=" * 60)
        
        try:
            answer = input(f"\nRemove these {len(dead_urls)} profiles from database & history? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = "n"
        
        if answer == "y":
            _remove_dead_urls(dead_urls, flagged_file, history_mgr)
        else:
            logger.info("ℹ️  Dead URLs left untouched.")


def _remove_dead_urls(dead_urls, flagged_file, history_mgr):
    """Remove dead/changed URLs from database, CSV, flagged file, and visited history."""
    import csv
    dead_set = set(dead_urls)
    normalized_dead = {u.rstrip('/') for u in dead_set if u}
    
    # 1. Remove from SQLite / cloud DB
    try:
        import sys
        sys.path.insert(0, str(PROJECT_ROOT / "backend"))
        from database import get_connection
        conn = get_connection()
        cur = conn.cursor()
        for url in dead_urls:
            normalized = (url or "").strip().rstrip("/")
            if not normalized:
                continue
            # Delete only exact profile URL rows (with or without trailing slash).
            cur.execute(
                "DELETE FROM alumni WHERE linkedin_url = %s OR linkedin_url = %s",
                (normalized, f"{normalized}/"),
            )
        conn.commit()
        logger.info(f"🗑️  Removed {len(dead_urls)} dead profiles from database")
        try:
            conn.close()
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"⚠️  Could not clean database: {e}")
    
    # 2. Remove from flagged_for_review.txt
    try:
        if flagged_file.exists():
            with open(flagged_file, 'r') as f:
                lines = f.readlines()
            kept = [l for l in lines if l.split('#')[0].strip().rstrip('/') not in normalized_dead]
            with open(flagged_file, 'w') as f:
                f.writelines(kept)
            logger.info(f"🗑️  Removed {len(lines) - len(kept)} entries from flagged_for_review.txt")
    except Exception as e:
        logger.warning(f"⚠️  Could not clean flagged file: {e}")
    
    # 3. Remove from visited_history.csv
    try:
        visited_csv = PROJECT_ROOT / "scraper" / "output" / "visited_history.csv"
        if visited_csv.exists():
            rows = []
            removed = 0
            with open(visited_csv, 'r') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    profile_url = row.get('profile_url', '').rstrip('/')
                    if profile_url in normalized_dead:
                        removed += 1
                    else:
                        rows.append(row)
            with open(visited_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            if removed:
                logger.info(f"🗑️  Removed {removed} entries from visited_history.csv")
    except Exception as e:
        logger.warning(f"⚠️  Could not clean visited history: {e}")
    
    # 4. Remove from UNT_Alumni_Data.csv
    try:
        alumni_csv = PROJECT_ROOT / "scraper" / "output" / "UNT_Alumni_Data.csv"
        if alumni_csv.exists():
            rows = []
            removed = 0
            with open(alumni_csv, 'r') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                for row in reader:
                    url = (row.get('linkedin_url', '') or row.get('profile_url', '')).rstrip('/')
                    if url in normalized_dead:
                        removed += 1
                    else:
                        rows.append(row)
            with open(alumni_csv, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            if removed:
                logger.info(f"🗑️  Removed {removed} entries from UNT_Alumni_Data.csv")
    except Exception as e:
        logger.warning(f"⚠️  Could not clean alumni CSV: {e}")
    
    print(f"\n✅ Dead profiles cleaned from all data sources.")



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

        # ✅ Defense layer initialized here (minimal + safe)
        nav = SafeNavigator(scraper.driver)

        start_exit_listener()

        if config.SCRAPER_MODE == "names":
            run_names_mode(scraper, nav, history_mgr)
        elif config.SCRAPER_MODE == "review":
            run_review_mode(scraper, nav, history_mgr)
        else:
            run_search_mode(scraper, nav, history_mgr)

    except KeyboardInterrupt:
        logger.warning("Stopped by user")
    finally:
        stop_exit_listener()
        scraper.quit()


if __name__ == "__main__":
    main()
