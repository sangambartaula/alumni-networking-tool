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
import csv
import urllib.parse
import threading
from datetime import datetime, timedelta, timezone
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# Local Modules
import config
import utils
import database_handler
from scraper import LinkedInScraper
from config import logger

# Backend
from backend.sqlite_fallback import get_connection_manager
from backend.database import increment_scraper_activity, normalize_url, upsert_scraped_profile
from backend.geocoding import geocode_location
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

# GUI Limits
GUI_MAX_PROFILES = int(os.getenv("GUI_MAX_PROFILES", "0"))
GUI_MAX_RUNTIME_MINUTES = int(os.getenv("GUI_MAX_RUNTIME_MINUTES", "0"))
SCRIPT_START_TIME = datetime.now()
global_profiles_tracked_for_gui = 0

# Cloud write guards for this scraping run.
_CLOUD_UPSERT_MAX_CONSECUTIVE_FAILURES = 5
_cloud_upsert_consecutive_failures = 0
_cloud_upsert_disabled_for_run = False
_geocode_failures_this_run = 0
_geocode_failure_locations = set()


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
DEFAULT_SEARCH_BASE_URL = (
    "https://www.linkedin.com/search/results/people/"
    "?network=%5B%22O%22%5D&schoolFilter=%5B%226464%22%5D"
)
UNT_DISCIPLINE_SEARCH_BASE_URL = (
    "https://www.linkedin.com/search/results/people/?schoolFilter=%5B%226464%22%5D"
)
_TEMP_SEARCH_QUERY_KEYS = {"sid", "origin", "position", "trackingId", "searchId"}

DISCIPLINE_ALIAS_LABELS = {
    "software": "Software, Data, AI & Cybersecurity",
    "embedded": "Embedded, Electrical & Hardware Engineering",
    "mechanical": "Mechanical Engineering & Manufacturing",
    "construction": "Construction & Engineering Management",
    "biomedical": "Biomedical Engineering",
    "cybersecurity": "Software, Data, AI & Cybersecurity",
}

DISCIPLINE_ALIAS_REDIRECTS = {
    # Backward compatibility with previous alias taxonomy.
    "materials": "mechanical",
    "cyber": "cybersecurity",
    "infosec": "cybersecurity",
    "security": "cybersecurity",
}

DISCIPLINE_KEYWORD_BUCKETS = {
    "software": (
        "software, developer, programmer, software engineer, backend, frontend, full stack, web "
        "developer, computer science, computer engineering, information technology, cybersecurity, "
        "data, data engineer, data science, data scientist, analytics, machine learning, artificial "
        "intelligence, ai, python, java, c++, javascript, cloud, infosec, network security, "
        "penetration testing, security analyst, soc analyst, ethical hacker, vulnerability assessment"
    ),
    "cybersecurity": (
        "cybersecurity, infosec, information security, network security, security operations center, "
        "soc analyst, security analyst, penetration testing, pentest, ethical hacker, blue team, red "
        "team, vulnerability assessment, incident response, threat intelligence, iam, siem"
    ),
    "embedded": (
        "embedded, firmware, embedded systems, hardware, hardware engineer, electronics, electrical "
        "engineering, electrical engineer, pcb, circuit design, circuits, fpga, verilog, vhdl, "
        "semiconductor, microcontroller, arm, stm32, esp32, signal processing, power systems, "
        "matlab, simulink, c, c++"
    ),
    "construction": (
        "construction engineering, construction management, construction engineer, civil engineering, "
        "civil engineer, structural engineering, structural engineer, project engineer, site engineer, "
        "field engineer, infrastructure, transportation engineering, geotechnical engineering, "
        "surveying, land development, bim, revit, autocad, primavera, p6, ms project, project "
        "controls, estimating, cost estimation, osha"
    ),
    "biomedical": (
        "biomedical engineering, biomedical engineer, bioengineering, medical devices, biomaterials, "
        "bioinformatics, medical imaging, biosensors, prosthetics, healthcare engineering, biotech, "
        "biotechnology, mri, ct scan, ultrasound, clinical engineering, tissue engineering, neural "
        "engineering, rehabilitation engineering, fda, medical informatics, health informatics"
    ),
    "mechanical": (
        "mechanical engineering, mechanical engineer, mechanical design, cad, solidworks, autocad, "
        "ansys, manufacturing, manufacturing engineering, thermodynamics, heat transfer, fluid "
        "mechanics, hvac, energy, energy systems, renewable energy, finite element analysis, fea, "
        "structural analysis, stress analysis, machine design, robotics, matlab, simulink, catia, "
        "materials science, materials engineering, nanotechnology, nanomaterials, polymers, composites, "
        "metallurgy, ceramics, materials characterization, additive manufacturing, 3d printing, sem, "
        "tem, xrd, corrosion, heat treatment, thin films, crystallography, semiconductor materials, "
        "process engineering, quality engineering, six sigma, failure analysis, powder metallurgy"
    ),
}

_SEARCH_INPUT_SELECTORS = [
    (By.CSS_SELECTOR, "input[aria-label*='Search by name']"),
    (By.CSS_SELECTOR, "input[aria-label*='Search']"),
    (By.CSS_SELECTOR, "input[placeholder*='Search']"),
    (By.CSS_SELECTOR, "input.search-global-typeahead__input"),
    (By.CSS_SELECTOR, "input[role='combobox']"),
]


def _normalize_profile_url(url):
    normalized = normalize_url(url)
    if not normalized:
        return ""
    cleaned = normalized.strip().split("?", 1)[0].split("#", 1)[0]
    return cleaned.rstrip("/")


def _parse_search_disciplines(raw_value):
    selected = []
    seen = set()
    for token in (raw_value or "").split(","):
        alias = token.strip().lower()
        if not alias:
            continue
        alias = DISCIPLINE_ALIAS_REDIRECTS.get(alias, alias)
        if alias in DISCIPLINE_KEYWORD_BUCKETS:
            if alias not in seen:
                selected.append(alias)
                seen.add(alias)
        else:
            logger.warning(f"Unknown SEARCH_DISCIPLINES value ignored: '{token.strip()}'")
    return selected


def _get_selected_search_disciplines():
    raw_value = (getattr(config, "SEARCH_DISCIPLINES", "") or "").strip()
    if not raw_value:
        return []
    selected = _parse_search_disciplines(raw_value)
    if not selected:
        logger.warning(
            "SEARCH_DISCIPLINES is set, but no valid aliases were found. "
            "Falling back to default search mode."
        )
    return selected


def _canonicalize_search_base_url(current_url, fallback_url):
    parsed = urllib.parse.urlsplit((current_url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return fallback_url

    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    filtered_items = [
        (key, value)
        for key, value in query_items
        if key not in _TEMP_SEARCH_QUERY_KEYS and key != "page"
    ]
    if not any(key == "schoolFilter" for key, _ in filtered_items):
        filtered_items.append(("schoolFilter", '["6464"]'))

    path = parsed.path or "/search/results/people/"
    if "/search/results/people/" not in path:
        path = "/search/results/people/"
    if not path.endswith("/"):
        path = f"{path}/"

    query = urllib.parse.urlencode(filtered_items, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, query, ""))


def _build_discipline_search_base_url(current_url, keyword_query):
    base_url = _canonicalize_search_base_url(current_url, UNT_DISCIPLINE_SEARCH_BASE_URL)
    if "keywords=" in base_url:
        return base_url

    parsed = urllib.parse.urlsplit(base_url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query_items = [(k, v) for (k, v) in query_items if k != "keywords"]
    query_items.append(("keywords", keyword_query))
    query = urllib.parse.urlencode(query_items, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _find_visible_people_search_input(scraper, timeout_seconds=12):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        for by, selector in _SEARCH_INPUT_SELECTORS:
            try:
                elements = scraper.driver.find_elements(by, selector)
            except Exception:
                continue

            for element in elements:
                try:
                    if element.is_displayed() and element.is_enabled():
                        return element
                except Exception:
                    continue
        time.sleep(0.25)
    return None


def _submit_discipline_keywords(scraper, keyword_query):
    search_input = _find_visible_people_search_input(scraper)
    if not search_input:
        logger.warning("Could not find LinkedIn people search input on results page.")
        return False

    previous_url = (scraper.driver.current_url or "").strip()
    try:
        search_input.click()
        time.sleep(random.uniform(0.2, 0.5))
        search_input.send_keys(Keys.CONTROL, "a")
        search_input.send_keys(Keys.DELETE)
        time.sleep(random.uniform(0.2, 0.5))
        search_input.send_keys(keyword_query)
        time.sleep(random.uniform(0.3, 0.7))
        search_input.send_keys(Keys.ENTER)
    except Exception as e:
        logger.warning(f"Failed to submit discipline keywords via search bar: {e}")
        return False

    deadline = time.time() + 12
    while time.time() < deadline:
        current_url = (scraper.driver.current_url or "").strip()
        if current_url and current_url != previous_url and "/search/results/people/" in current_url:
            return True
        time.sleep(0.25)

    current_url = (scraper.driver.current_url or "").strip()
    return bool(current_url and "/search/results/people/" in current_url)


def wait_between_profiles():
    global global_profiles_tracked_for_gui
    global_profiles_tracked_for_gui += 1
    
    if GUI_MAX_PROFILES > 0 and global_profiles_tracked_for_gui >= GUI_MAX_PROFILES:
        logger.info(f"🛑 Reached GUI Max Profiles limit ({GUI_MAX_PROFILES}). Exiting gracefully.")
        sys.exit(0)
        
    if GUI_MAX_RUNTIME_MINUTES > 0:
        elapsed_mins = (datetime.now() - SCRIPT_START_TIME).total_seconds() / 60
        if elapsed_mins >= GUI_MAX_RUNTIME_MINUTES:
            logger.info(f"🛑 Reached GUI Max Runtime limit ({GUI_MAX_RUNTIME_MINUTES} mins). Exiting gracefully.")
            sys.exit(0)

    delay = random.uniform(config.MIN_DELAY, config.MAX_DELAY)
    logger.info(f"Next profile in {delay:.0f}s")

    for _ in range(10):
        if force_exit:
            return
        time.sleep(delay / 10)


def _canonicalize_redirect_url(original_url, canonical_url, history_mgr):
    """
    When LinkedIn redirects old URL -> canonical URL, keep canonical and remove old URL
    from persisted data sources to prevent duplicate profile records.
    """
    old = (original_url or "").strip().rstrip("/")
    new = (canonical_url or "").strip().rstrip("/")
    if not old or not new or old == new:
        return

    # 1) Remove stale old URL rows from DB tables.
    try:
        from backend.database import get_connection

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM alumni WHERE linkedin_url = %s OR linkedin_url = %s",
                    (old, f"{old}/"),
                )
                removed_alumni = cur.rowcount
                cur.execute(
                    "DELETE FROM visited_profiles WHERE linkedin_url = %s OR linkedin_url = %s",
                    (old, f"{old}/"),
                )
                removed_visited = cur.rowcount
            conn.commit()
            if removed_alumni or removed_visited:
                logger.info(
                    f"🔁 Canonicalized redirect URL: removed old URL rows "
                    f"(alumni={removed_alumni}, visited={removed_visited})"
                )
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"⚠️ Could not remove old redirected URL from DB ({old}): {e}")

    # 2) Remove old URL from UNT alumni CSV.
    try:
        alumni_csv = PROJECT_ROOT / "scraper" / "output" / "UNT_Alumni_Data.csv"
        if alumni_csv.exists():
            rows = []
            removed = 0
            with open(alumni_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or []
                for row in reader:
                    url = (row.get("linkedin_url", "") or row.get("profile_url", "")).strip().rstrip("/")
                    if url == old:
                        removed += 1
                        continue
                    rows.append(row)
            with open(alumni_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            if removed:
                logger.info(f"🔁 Canonicalized redirect URL: removed {removed} old row(s) from UNT_Alumni_Data.csv")
    except Exception as e:
        logger.warning(f"⚠️ Could not clean old redirected URL from UNT_Alumni_Data.csv ({old}): {e}")

    # 3) Remove old URL from flagged_for_review.txt.
    try:
        flagged_file = PROJECT_ROOT / "scraper" / "output" / "flagged_for_review.txt"
        if flagged_file.exists():
            with open(flagged_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            kept = [line for line in lines if line.split("#")[0].strip().rstrip("/") != old]
            if len(kept) != len(lines):
                with open(flagged_file, "w", encoding="utf-8") as f:
                    f.writelines(kept)
                logger.info(f"🔁 Canonicalized redirect URL: removed {len(lines) - len(kept)} old row(s) from flagged_for_review.txt")
    except Exception as e:
        logger.warning(f"⚠️ Could not clean old redirected URL from flagged_for_review.txt ({old}): {e}")

    # 4) Remove old URL from in-memory history + visited_history.csv.
    try:
        if old in history_mgr.visited_history:
            del history_mgr.visited_history[old]
            history_mgr.save_history_csv()
            logger.info("🔁 Canonicalized redirect URL: removed old row from visited history")
    except Exception as e:
        logger.warning(f"⚠️ Could not clean old redirected URL from visited history ({old}): {e}")


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

    global _cloud_upsert_consecutive_failures, _cloud_upsert_disabled_for_run
    global _geocode_failures_this_run, _geocode_failure_locations
    
    canonical_url = _normalize_profile_url(data.get("profile_url", input_url))
    input_url = _normalize_profile_url(input_url)
    if not canonical_url:
        canonical_url = input_url
    if canonical_url:
        data["profile_url"] = canonical_url

    if data.get("location") and (data.get("latitude") is None or data.get("longitude") is None):
        location_text = str(data.get("location", "")).strip()
        try:
            coords = geocode_location(location_text)
            if coords:
                data["latitude"], data["longitude"] = coords
            elif location_text:
                _geocode_failures_this_run += 1
                _geocode_failure_locations.add(location_text)
        except Exception as geocode_err:
            if location_text:
                _geocode_failures_this_run += 1
                _geocode_failure_locations.add(location_text)
            logger.debug(f"Auto-geocoding skipped for {canonical_url}: {geocode_err}")

    original_url = _normalize_profile_url(data.pop("_original_url", None))  # Remove internal key before save
    
    # Check if canonical URL was already saved in this session under a different input URL
    if original_url and history_mgr.should_skip(canonical_url):
        logger.info(f"  ↩️  Profile Already Visited, Skipping: {canonical_url}")
        # Canonical URL is authoritative; clean stale source URL records.
        _canonicalize_redirect_url(original_url, canonical_url, history_mgr)
        return False
    
    if database_handler.save_profile_to_csv(data):
        # Cloud-first persistence with SQLite mirror; CSV remains source backup.
        try:
            upsert_status = upsert_scraped_profile(
                data,
                allow_cloud=(not _cloud_upsert_disabled_for_run),
            )

            status = upsert_status if isinstance(upsert_status, dict) else {}
            cloud_attempted = bool(status.get("cloud_attempted"))
            cloud_written = bool(status.get("cloud_written"))
            if cloud_attempted:
                if cloud_written:
                    _cloud_upsert_consecutive_failures = 0
                else:
                    _cloud_upsert_consecutive_failures += 1
                    logger.warning(
                        "Cloud upsert failed for profile (%s/%s consecutive failures).",
                        _cloud_upsert_consecutive_failures,
                        _CLOUD_UPSERT_MAX_CONSECUTIVE_FAILURES,
                    )
                    if _cloud_upsert_consecutive_failures >= _CLOUD_UPSERT_MAX_CONSECUTIVE_FAILURES:
                        _cloud_upsert_disabled_for_run = True
                        logger.warning(
                            "Cloud upsert disabled for this scraping run after %s consecutive failures.",
                            _CLOUD_UPSERT_MAX_CONSECUTIVE_FAILURES,
                        )
        except Exception as upsert_err:
            _cloud_upsert_consecutive_failures += 1
            if _cloud_upsert_consecutive_failures >= _CLOUD_UPSERT_MAX_CONSECUTIVE_FAILURES:
                _cloud_upsert_disabled_for_run = True
            logger.warning(f"Profile DB upsert failed (kept CSV backup): {upsert_err}")
        # Mark canonical URL as visited
        history_mgr.mark_as_visited(canonical_url, saved=True)
        # If we came through a redirect, remove the old URL from persisted data sources.
        if original_url and original_url.rstrip('/') != canonical_url.rstrip('/'):
            _canonicalize_redirect_url(original_url, canonical_url, history_mgr)
        # Track scraper activity (who scraped this profile)
        increment_scraper_activity(config.LINKEDIN_EMAIL)
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
            url = _normalize_profile_url(url)
            if not url:
                continue
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


def _run_search_results_mode(scraper, nav, history_mgr, base_url, state_mode_key, mode_label):
    page = 1
    state = load_scrape_state()
    if (
        state
        and state.get("mode") == state_mode_key
        and state.get("search_url") == base_url
        and _is_recent_state(state.get("updated_at"), config.SCRAPE_RESUME_MAX_AGE_DAYS)
    ):
        try:
            saved_page = int(state.get("page") or 1)
        except (TypeError, ValueError):
            saved_page = 1
        page = saved_page if saved_page >= 1 else 1
        logger.info(
            f"↪ Resuming {mode_label} from page {page} "
            f"(state age <= {config.SCRAPE_RESUME_MAX_AGE_DAYS} days)"
        )
    else:
        logger.info(f"↪ Starting {mode_label} from page 1")

    while True:
        if should_stop():
            break

        url = base_url if page == 1 else f"{base_url}&page={page}"
        save_scrape_state(state_mode_key, base_url, page)
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
            save_scrape_state(state_mode_key, base_url, 1)
            break

        for profile_url in urls:
            profile_url = _normalize_profile_url(profile_url)
            if not profile_url:
                continue

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
        save_scrape_state(state_mode_key, base_url, page)


def run_search_mode(scraper, nav, history_mgr):
    _run_search_results_mode(
        scraper=scraper,
        nav=nav,
        history_mgr=history_mgr,
        base_url=DEFAULT_SEARCH_BASE_URL,
        state_mode_key="search",
        mode_label="search mode",
    )


def run_discipline_search_mode(scraper, nav, history_mgr, discipline_aliases):
    for alias in discipline_aliases:
        if should_stop():
            return

        label = DISCIPLINE_ALIAS_LABELS.get(alias, alias)
        keyword_query = DISCIPLINE_KEYWORD_BUCKETS[alias]
        logger.info(f"--- MODE: Discipline Search ({label}) ---")

        ok = nav.get(UNT_DISCIPLINE_SEARCH_BASE_URL)
        if not ok:
            logger.warning("UNT people search page unhealthy. Skipping this discipline.")
            continue

        time.sleep(3)
        if not _submit_discipline_keywords(scraper, keyword_query):
            logger.warning(f"Could not submit search keywords for discipline '{alias}'. Skipping.")
            continue

        discipline_base_url = _build_discipline_search_base_url(
            scraper.driver.current_url,
            keyword_query,
        )
        logger.info(f"Discipline base URL: {discipline_base_url}")

        _run_search_results_mode(
            scraper=scraper,
            nav=nav,
            history_mgr=history_mgr,
            base_url=discipline_base_url,
            state_mode_key=f"search_discipline:{alias}",
            mode_label=f"discipline search ({alias})",
        )

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
        url = _normalize_profile_url(url)
        if url and not is_blocked_url(url):
            urls.append(url)
    
    if not urls:
        logger.info("📋 No URLs in flagged_for_review.txt")
        return
    
    logger.info(f"📋 Review mode: {len(urls)} profiles to re-scrape")
    
    dead_urls = []  # Collect profiles that no longer exist
    
    for profile_url in urls:
        profile_url = _normalize_profile_url(profile_url)
        if not profile_url:
            continue
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
    
    # 4. Remove from UNT_Alumni_Data.csv (rewrite with canonical schema)
    try:
        alumni_csv = PROJECT_ROOT / "scraper" / "output" / "UNT_Alumni_Data.csv"
        if alumni_csv.exists():
            rows = []
            removed = 0
            with open(alumni_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = (row.get("linkedin_url", "") or row.get("profile_url", "")).rstrip("/")
                    if url in normalized_dead:
                        removed += 1
                    else:
                        rows.append({col: row.get(col, "") for col in config.CSV_COLUMNS})
            with open(alumni_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
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
    global _cloud_upsert_consecutive_failures, _cloud_upsert_disabled_for_run
    global _geocode_failures_this_run, _geocode_failure_locations
    _cloud_upsert_consecutive_failures = 0
    _cloud_upsert_disabled_for_run = False
    _geocode_failures_this_run = 0
    _geocode_failure_locations = set()

    history_mgr = database_handler.HistoryManager()
    history_mgr.sync_with_db()

    database_handler.ensure_alumni_output_csv()

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
        elif config.SCRAPER_MODE == "search":
            selected_disciplines = _get_selected_search_disciplines()
            if selected_disciplines:
                run_discipline_search_mode(scraper, nav, history_mgr, selected_disciplines)
            else:
                run_search_mode(scraper, nav, history_mgr)
        else:
            run_search_mode(scraper, nav, history_mgr)

    except KeyboardInterrupt:
        logger.warning("Stopped by user")
    finally:
        if _cloud_upsert_disabled_for_run:
            logger.warning(
                "WARNING: CLOUD DATABASE WAS UNREACHABLE. SCRAPED INFORMATION WAS STORED IN LOCAL SQLITE DATABASE AND CSV FILE."
            )
            logger.warning(
                "PRESS UPLOAD TO CLOUD TO UPLOAD WHEN YOU HAVE A SUITABLE CONNECTION."
            )

        if _geocode_failures_this_run > 0:
            logger.warning(
                "WARNING: SOME LOCATIONS COULD NOT BE GEOLOCATED DURING THIS SCRAPE RUN (%s profiles, %s unique locations).",
                _geocode_failures_this_run,
                len(_geocode_failure_locations),
            )
            logger.warning(
                "PRESS BACKFILL GEOCODE (OPTIONAL) WHEN YOU HAVE A SUITABLE CONNECTION TO RETRY THESE LOCATIONS."
            )
        stop_exit_listener()
        scraper.quit()


if __name__ == "__main__":
    main()
