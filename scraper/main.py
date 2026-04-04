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
import uuid
from datetime import datetime, timedelta, timezone
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


def _configure_utf8_stdio():
    """Use UTF-8 output streams so unicode log lines do not crash on Windows consoles."""
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


_configure_utf8_stdio()

# Local Modules
import config
import utils
import database_handler
from scraper import LinkedInScraper
from config import logger

# Backend
from backend.sqlite_fallback import get_connection_manager
from backend.database import (
    increment_scraper_activity,
    normalize_url,
    upsert_scraped_profile,
    create_scrape_run,
    finalize_scrape_run,
    record_scrape_run_flag,
    get_direct_mysql_connection,
)
from backend.geocoding import geocode_location_with_status
from defense.navigator import SafeNavigator
from groq_client import is_groq_available, _get_client, GROQ_MODEL, parse_groq_json_response


# ============================================================
# Resume / Scrape State
# ============================================================
def load_keyword_state(mode_key):
    """
    Load the last known scrape position for a specific keyword list.
    """
    if not mode_key: return None
    manager = get_connection_manager()
    conn = manager.get_sqlite_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_list_state (
                mode TEXT PRIMARY KEY,
                search_url TEXT,
                page INTEGER DEFAULT 1,
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.commit()

        row = conn.execute(
            "SELECT mode, search_url, page, updated_at FROM keyword_list_state WHERE mode = ?", (mode_key,)
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


def save_keyword_state(mode_key, search_url, page):
    if not mode_key: return
    manager = get_connection_manager()
    conn = manager.get_sqlite_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_list_state (
                mode TEXT PRIMARY KEY,
                search_url TEXT,
                page INTEGER DEFAULT 1,
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT INTO keyword_list_state (mode, search_url, page, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(mode) DO UPDATE SET
                search_url = excluded.search_url,
                page = excluded.page,
                updated_at = datetime('now')
            """,
            (mode_key, search_url, page)
        )
        conn.commit()
    finally:
        conn.close()

def load_discipline_rotation(discipline):
    manager = get_connection_manager()
    conn = manager.get_sqlite_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cluster_rotation_state (
                discipline TEXT PRIMARY KEY,
                active_cluster INTEGER DEFAULT 0,
                profiles_collected INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.commit()

        row = conn.execute(
            "SELECT active_cluster, profiles_collected, updated_at FROM cluster_rotation_state WHERE discipline = ?", (discipline,)
        ).fetchone()
        if row:
            return {
                "active_cluster": row["active_cluster"],
                "profiles_collected": row["profiles_collected"],
                "updated_at": row["updated_at"],
            }
    finally:
        conn.close()
    return None

def save_discipline_rotation(discipline, active_cluster, profiles_collected):
    manager = get_connection_manager()
    conn = manager.get_sqlite_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cluster_rotation_state (
                discipline TEXT PRIMARY KEY,
                active_cluster INTEGER DEFAULT 0,
                profiles_collected INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            INSERT INTO cluster_rotation_state (discipline, active_cluster, profiles_collected, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(discipline) DO UPDATE SET
                active_cluster = excluded.active_cluster,
                profiles_collected = excluded.profiles_collected,
                updated_at = datetime('now')
            """,
            (discipline, active_cluster, profiles_collected)
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
# Browser Session Recovery (handles display sleep disconnects)
# ============================================================
def _should_recover_from_session_error(error_msg):
    """Detect if error is a session loss (e.g., from display sleep)."""
    msg_lower = error_msg.lower() if error_msg else ""
    return any(pattern in msg_lower for pattern in [
        "invalid session id",
        "no such window",
        "target window already closed",
        "disconnected: not connected to devtools",
        "chrome has closed",
        "session deleted",
    ])


def _recover_browser_session(scraper, profile_url, nav=None):
    """
    Restart browser and retry profile scrape once. Used when display sleep kills the session.
    Returns (success: bool, data: dict or str)
    """
    logger.warning(f"⚠️  Browser session dropped. Restarting chrome and retrying {profile_url}...")
    
    try:
        # Kill old browser gracefully
        try:
            scraper.quit()
        except Exception:
            pass
        
        time.sleep(1)
        
        # Start fresh browser
        scraper.setup_driver()
        
        # Re-authenticate
        if not scraper.login():
            logger.error("❌ Could not re-login after session recovery. Skipping profile.")
            return False, "LOGIN_FAILED"
        
        # Re-initialize navigator if provided
        if nav is not None:
            nav.driver = scraper.driver
        
        time.sleep(1)
        
        # Single retry attempt
        logger.info(f"  🔄 Retrying: {profile_url}")
        data = scraper.scrape_profile_page(profile_url)
        
        if data == "PAGE_NOT_FOUND":
            logger.warning(f"  💀 Profile no longer available: {profile_url}")
            return True, "PAGE_NOT_FOUND"
        
        if data and data != "PAGE_NOT_FOUND":
            logger.info(f"  ✅ Recovery succeeded for {profile_url}")
            return True, data
        
        logger.warning(f"  ⚠️  Recovery retry returned no data for {profile_url}")
        return False, None
        
    except Exception as recovery_err:
        logger.error(f"❌ Recovery attempt failed: {recovery_err}")
        return False, None


# ============================================================
# Exit Control
# ============================================================
exit_requested = False
force_exit = False
_exit_listener_active = False
session_profiles_scraped = 0

# GUI Limits
GUI_MAX_PROFILES = int(getattr(config, "GUI_MAX_PROFILES", 0) or 0)
GUI_MAX_RUNTIME_MINUTES = int(getattr(config, "GUI_MAX_RUNTIME_MINUTES", 0) or 0)
SCRIPT_START_TIME = datetime.now()
global_profiles_tracked_for_gui = 0

# Cloud write guards for this scraping run.
_CLOUD_UPSERT_MAX_CONSECUTIVE_FAILURES = 5
_cloud_upsert_consecutive_failures = 0
_cloud_upsert_disabled_for_run = False
_geocode_failures_this_run = 0
_geocode_failure_locations = set()
_geocode_network_failures_this_run = 0
_geocode_success_this_run = 0
_cloud_upsert_successes_this_run = 0
_cloud_upsert_failures_this_run = 0
_sqlite_writes_this_run = 0
_flagged_urls_this_run = set()
_current_scrape_run_id = None
_current_scrape_run_uuid = None
_cloud_verify_semaphore = threading.Semaphore(4)
_ESTIMATED_NON_DELAY_SECONDS_PER_PROFILE = 25

_CLOUD_VARCHAR_LIMITS = {
    "school": 255,
    "school2": 255,
    "school3": 255,
    "degree": 255,
    "degree2": 255,
    "degree3": 255,
    "major": 255,
    "major2": 255,
    "major3": 255,
}


def _normalize_location_for_geocoding(location_text):
    """Use Groq once to convert ambiguous LinkedIn location text into a geocodable format."""
    if not location_text:
        return None
    if not getattr(config, "GEOCODE_USE_GROQ_FALLBACK", True):
        return None
    if not is_groq_available():
        return None

    client = _get_client()
    if not client:
        return None

    prompt = f"""Normalize this LinkedIn location to a geocodable value.

Input location: {location_text}

Rules:
- If confidently possible, output: "City, State, United States" for US locations.
- Do not guess a city/state that is not strongly implied by the input.
- If the input is too vague (for example: "Eastern Region", "Global", "Remote"), output "unknown".
- Keep output concise, no commentary.

Return strict JSON only:
{{"normalized_location": "..."}}
"""

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You normalize locations for geocoding. "
                        "Return valid JSON only with key normalized_location."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=120,
            response_format={"type": "json_object"},
        )
        parsed = parse_groq_json_response((response.choices[0].message.content or "").strip())
        if not isinstance(parsed, dict):
            return None

        normalized = str(parsed.get("normalized_location") or "").strip()
        if not normalized:
            return None
        if normalized.lower() in {"unknown", "null", "none", "n/a", "not sure"}:
            return "unknown"
        return normalized
    except Exception as err:
        logger.debug(f"Groq location normalization skipped for '{location_text}': {err}")
        return None


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
    # Avoid spawning a background stdin reader in GUI/non-interactive launches.
    stdin_is_tty = bool(getattr(sys.stdin, "isatty", lambda: False)())
    if not stdin_is_tty:
        logger.info("Exit listener disabled (non-interactive stdin).")
        _exit_listener_active = False
        return
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

DISCIPLINE_SEARCH_GROUPS = {
    "software": [
        "computer science, artificial intelligence, software engineer, software developer, full stack, front end, back end, machine learning",
        "data science, data scientist, cloud, cybersecurity, security analyst, devops engineer, python, react, aws, docker",
        "software architect, machine learning engineer, systems engineer, network engineer, site reliability engineer, penetration tester, backend developer, frontend developer, devops specialist, cloud architect",
        "c++, java, javascript, tensorflow, pytorch, sql, kubernetes, linux, github, matlab"
    ],
    "embedded": [
        "electrical engineer, embedded engineer, hardware engineer, firmware engineer, microcontroller, vlsi engineer, signal processing, semiconductor",
        "pcb design, fpga engineer, circuit design, robotics engineer, analog design, verilog, matlab, cad",
        "system architect, control engineer, test engineer, automation engineer, instrumentation engineer, electronics engineer, product engineer, embedded systems specialist, labview engineer, circuit board designer",
        "simulink, proteus, xilinx, altium, cadence, microchip, arm cortex, vhdl, python, matlab"
    ],
    "mechanical": [
        "mechanical engineer, manufacturing engineer, design engineer, robotics engineer, product design, hvac engineer, thermodynamics, cad engineer",
        "solidworks, ansys, matlab, automation engineer, machine design, control systems, cnc machining, 3d printing",
        "mechanical designer, project engineer, process engineer, production engineer, mechatronics engineer, structural engineer, quality engineer, thermal engineer, systems engineer, product engineer",
        "catia, inventor, fusion 360, nx unigraphics, comsol, cad, cam, fea, simulation, solid edge"
    ],
    "biomedical": [
        "biomedical engineer, clinical engineer, medical device engineer, bioinformatics engineer, biomaterials, medical imaging",
        "biosensors, biotechnology, health informatics, tissue engineering, biomedical instrumentation, labview"
    ],
    "construction": [
        "civil engineer, construction engineer, project engineer, structural engineer, construction management, site engineer, infrastructure",
        "bim engineer, revit, estimating, cost engineer, surveying, project management, construction planner",
        "design engineer, construction coordinator, planning engineer, scheduling engineer, quality engineer, safety engineer, site manager, construction supervisor, structural designer, construction analyst",
        "autocad, sketchup, primavera, navisworks, sap2000, microstation, project scheduling, cost estimation, cad, bim"
    ]
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
        if alias in DISCIPLINE_SEARCH_GROUPS:
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
    parsed = urllib.parse.urlsplit(base_url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query_items = [(k, v) for (k, v) in query_items if k != "keywords"]
    query_items.append(("keywords", keyword_query))
    query = urllib.parse.urlencode(query_items, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _format_linkedin_keyword_query(raw_cluster_query):
    """Normalize cluster terms for LinkedIn keyword input using comma-separated phrases."""
    terms = [term.strip() for term in (raw_cluster_query or "").split(",") if term.strip()]
    return ", ".join(terms)


def _is_legacy_boolean_keywords_url(url):
    text = (url or "").lower()
    return ("%22" in text) or ("+or+" in text) or ("%20or%20" in text)


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
        # Clear field robustly (handles Mac / Windows)
        search_input.clear()
        search_input.send_keys(Keys.COMMAND, "a")
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


def _format_duration_short(total_seconds):
    if total_seconds is None:
        return "0m"
    total_seconds = max(0, int(total_seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _emit_progress_line():
    elapsed_seconds = int((datetime.now() - SCRIPT_START_TIME).total_seconds())
    elapsed_label = _format_duration_short(elapsed_seconds)

    if GUI_MAX_PROFILES > 0:
        base = f"{session_profiles_scraped} out of {GUI_MAX_PROFILES} profiles scraped"
    else:
        base = f"{session_profiles_scraped} profiles scraped"

    progress_parts = [base, f"elapsed {elapsed_label}"]

    if GUI_MAX_RUNTIME_MINUTES > 0:
        remaining_seconds_time = max(0, GUI_MAX_RUNTIME_MINUTES * 60 - elapsed_seconds)
        progress_parts.append(f"time remaining {_format_duration_short(remaining_seconds_time)}")

    if GUI_MAX_PROFILES > 0 and session_profiles_scraped > 0:
        avg_seconds_per_profile = elapsed_seconds / max(1, session_profiles_scraped)
        remaining_profiles = max(0, GUI_MAX_PROFILES - session_profiles_scraped)
        est_seconds_profiles = int(avg_seconds_per_profile * remaining_profiles)
        progress_parts.append(f"est profile-limit remaining {_format_duration_short(est_seconds_profiles)}")

    if GUI_MAX_PROFILES > 0 and GUI_MAX_RUNTIME_MINUTES > 0:
        progress_parts.append("stopping at whichever limit hits first")

    logger.info("PROGRESS | %s", " | ".join(progress_parts))


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


def _collect_profile_flag_reasons(profile_data):
    """Return the same review reasons used for flagged_for_review.txt generation."""
    if not isinstance(profile_data, dict):
        return []

    reasons = []
    try:
        from config import (
            FLAG_MISSING_EXPERIENCE_DATA,
        )
    except Exception:
        return reasons

    def _as_text(key):
        value = profile_data.get(key, "")
        return str(value).strip() if value is not None else ""

    if FLAG_MISSING_EXPERIENCE_DATA:
        job_title = _as_text("job_title")
        company = _as_text("company")
        if job_title and not company:
            reasons.append("Missing Company but Job Title Present")
        elif company and not job_title:
            reasons.append("Missing Job Title but Company Present")

        exp2_title = _as_text("exp2_title")
        exp2_company = _as_text("exp2_company")
        if exp2_title and not exp2_company:
            reasons.append("Missing Company but Job Title Present for Experience 2")
        elif exp2_company and not exp2_title:
            reasons.append("Missing Job Title but Company Present for Experience 2")

        exp3_title = _as_text("exp3_title")
        exp3_company = _as_text("exp3_company")
        if exp3_title and not exp3_company:
            reasons.append("Missing Company but Job Title Present for Experience 3")
        elif exp3_company and not exp3_title:
            reasons.append("Missing Job Title but Company Present for Experience 3")

    return reasons


def _truncate_cloud_limited_fields(payload):
    """Trim known cloud VARCHAR fields to schema limits before DB upsert."""
    truncated = []
    if not isinstance(payload, dict):
        return truncated

    for key, max_len in _CLOUD_VARCHAR_LIMITS.items():
        value = payload.get(key)
        if value is None:
            continue
        text = str(value)
        if len(text) > max_len:
            payload[key] = text[:max_len].rstrip()
            truncated.append((key, len(text), max_len))
    return truncated


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
    global _geocode_network_failures_this_run
    global _geocode_success_this_run
    global _cloud_upsert_successes_this_run, _cloud_upsert_failures_this_run
    global _sqlite_writes_this_run
    global _flagged_urls_this_run
    global _current_scrape_run_id, session_profiles_scraped
    
    canonical_url = _normalize_profile_url(data.get("profile_url", input_url))
    input_url = _normalize_profile_url(input_url)
    if not canonical_url:
        canonical_url = input_url
    if canonical_url:
        data["profile_url"] = canonical_url

    if data.get("location") and (data.get("latitude") is None or data.get("longitude") is None):
        location_text = str(data.get("location", "")).strip()
        try:
            coords, geocode_status = geocode_location_with_status(location_text)
            if coords:
                data["latitude"], data["longitude"] = coords
                _geocode_success_this_run += 1
            elif geocode_status == "unknown_location" and location_text:
                normalized_location = _normalize_location_for_geocoding(location_text)
                if normalized_location and normalized_location != "unknown":
                    retry_coords, retry_status = geocode_location_with_status(normalized_location)
                    if retry_coords:
                        data["location"] = normalized_location
                        data["latitude"], data["longitude"] = retry_coords
                        _geocode_success_this_run += 1
                        logger.info(
                            "Location normalized via Groq for geocoding: '%s' -> '%s'",
                            location_text,
                            normalized_location,
                        )
                    else:
                        if retry_status == "unknown_location":
                            _geocode_failures_this_run += 1
                            _geocode_failure_locations.add(location_text)
                        elif retry_status in {"network_error", "parse_error"}:
                            _geocode_network_failures_this_run += 1
                elif normalized_location == "unknown":
                    logger.warning(
                        "Location unresolved after Groq normalization for %s: '%s'. Clearing location.",
                        canonical_url,
                        location_text,
                    )
                    data["location"] = None
                    _geocode_failures_this_run += 1
                    _geocode_failure_locations.add(location_text)
                else:
                    _geocode_failures_this_run += 1
                    _geocode_failure_locations.add(location_text)
            elif geocode_status in {"network_error", "parse_error"}:
                _geocode_network_failures_this_run += 1
        except Exception as geocode_err:
            _geocode_network_failures_this_run += 1
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
            upsert_payload = dict(data)
            truncated_fields = _truncate_cloud_limited_fields(upsert_payload)
            if truncated_fields:
                fields_text = ", ".join(
                    f"{name}({old_len}->{new_len})" for name, old_len, new_len in truncated_fields
                )
                logger.warning(
                    "Cloud payload truncated oversized field(s) for %s: %s",
                    canonical_url,
                    fields_text,
                )

            upsert_status = upsert_scraped_profile(
                upsert_payload,
                allow_cloud=(not _cloud_upsert_disabled_for_run),
                run_id=_current_scrape_run_id,
            )

            status = upsert_status if isinstance(upsert_status, dict) else {}
            cloud_attempted = bool(status.get("cloud_attempted"))
            cloud_written = bool(status.get("cloud_written"))
            cloud_routed_to_sqlite = bool(status.get("cloud_routed_to_sqlite"))
            cloud_queued = bool(status.get("cloud_queued"))
            cloud_reason = str(status.get("cloud_reason") or "").strip()
            sqlite_written = bool(status.get("sqlite_written"))
            if sqlite_written:
                _sqlite_writes_this_run += 1
            if cloud_attempted:
                if cloud_written:
                    _cloud_upsert_successes_this_run += 1
                    _cloud_upsert_consecutive_failures = 0
                    logger.info("PERSISTENCE: CSV updated | Cloud DB updated | SQLite mirror updated")
                    _verify_cloud_insert_after_delay(canonical_url)
                elif cloud_routed_to_sqlite:
                    logger.warning(
                        "PERSISTENCE: CSV updated | SQLite fallback updated | Cloud DB queued for retry"
                    )
                    if cloud_queued:
                        logger.warning("[red bold]FALLBACK MODE ACTIVE:[/red bold] Cloud is currently unreachable.")
                else:
                    _cloud_upsert_failures_this_run += 1
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
            else:
                if sqlite_written and cloud_reason:
                    logger.warning(
                        "PERSISTENCE: CSV updated | SQLite updated | Cloud not attempted (%s)",
                        cloud_reason,
                    )
                elif sqlite_written:
                    logger.warning("PERSISTENCE: CSV updated | SQLite updated | Cloud not attempted")
                else:
                    logger.warning("PERSISTENCE WARNING: CSV updated but no DB write was confirmed")
        except Exception as upsert_err:
            _cloud_upsert_failures_this_run += 1
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

        session_profiles_scraped += 1

        if _current_scrape_run_id:
            for reason in _collect_profile_flag_reasons(data):
                record_scrape_run_flag(_current_scrape_run_id, canonical_url, reason)
                _flagged_urls_this_run.add(canonical_url)

        _emit_progress_line()

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
            try:
                data = scraper.scrape_profile_page(url)
            except Exception as e:
                if _should_recover_from_session_error(str(e)):
                    success, data = _recover_browser_session(scraper, url, nav)
                    if not success:
                        logger.error(f"❌ Error processing {url}: recovery failed")
                        continue
                else:
                    logger.error(f"❌ Error processing {url}: {e}")
                    continue

            if data == "PAGE_NOT_FOUND":
                logger.warning(f"  💀 Dead URL skipped: {url}")
                continue

            if data and data != "PAGE_NOT_FOUND":
                _save_and_track(data, url, history_mgr)

            if should_stop():
                return

            wait_between_profiles()


def _run_search_results_mode(scraper, nav, history_mgr, base_url, state_mode_key, mode_label, max_profiles_for_mode=0):
    page = 1
    mode_start_count = session_profiles_scraped

    def _mode_quota_reached():
        if max_profiles_for_mode <= 0:
            return False
        return (session_profiles_scraped - mode_start_count) >= max_profiles_for_mode

    state = load_keyword_state(state_mode_key)
    state_url = (state or {}).get("search_url") if state else ""
    if state and _is_legacy_boolean_keywords_url(state_url) and "discipline:" in state_mode_key:
        logger.info("Resetting legacy discipline resume state to comma-style keywords.")
        save_keyword_state(state_mode_key, base_url, 1)
        state = None
    if (
        state
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
            return "stopped", (session_profiles_scraped - mode_start_count)
        if _mode_quota_reached():
            logger.info(f"Reached quota for {mode_label}: {max_profiles_for_mode} new profiles.")
            return "threshold_reached", (session_profiles_scraped - mode_start_count)

        url = base_url if page == 1 else f"{base_url}&page={page}"
        save_keyword_state(state_mode_key, base_url, page)
        logger.info(f"📄 Search page {page}")

        ok = nav.get(url)
        if not ok:
            logger.warning("Search page unhealthy. Stopping search loop.")
            return "network_error", (session_profiles_scraped - mode_start_count)

        time.sleep(5)
        scraper.scroll_full_page()

        urls = scraper.extract_profile_urls_from_page()
        if not urls:
            # Check for no results vs exhaustion
            save_keyword_state(state_mode_key, base_url, 1) # reset pagination since we reached end
            return "no_results" if page == 1 else "exhausted", (session_profiles_scraped - mode_start_count)

        for profile_url in urls:
            if _mode_quota_reached():
                logger.info(f"Reached quota for {mode_label}: {max_profiles_for_mode} new profiles.")
                return "threshold_reached", (session_profiles_scraped - mode_start_count)
            
            profile_url = _normalize_profile_url(profile_url)
            if not profile_url:
                continue

            if check_force_exit():
                return "stopped", (session_profiles_scraped - mode_start_count)

            if config.is_blocked_url(profile_url):
                continue

            if history_mgr.should_skip(profile_url):
                logger.info(f"  ↩️  Profile Already Visited, Skipping: {profile_url}")
                continue

            try:
                data = scraper.scrape_profile_page(profile_url)
            except Exception as e:
                if _should_recover_from_session_error(str(e)):
                    success, data = _recover_browser_session(scraper, profile_url, nav)
                    if not success:
                        logger.error(f"❌ Error processing {profile_url}: recovery failed")
                        continue
                else:
                    logger.error(f"❌ Error processing {profile_url}: {e}")
                    continue

            if data == "PAGE_NOT_FOUND":
                logger.warning(f"  💀 Dead URL skipped: {profile_url}")
                continue

            if data and data != "PAGE_NOT_FOUND":
                _save_and_track(data, profile_url, history_mgr)

            if should_stop():
                return "stopped", (session_profiles_scraped - mode_start_count)

            wait_between_profiles()

        page += 1
        save_keyword_state(state_mode_key, base_url, page)


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
    processed_any = False
    PROFILES_PER_CLUSTER_THRESHOLD = 100 

    logger.info(
        "DISCIPLINE QUEUE: %s",
        ", ".join(discipline_aliases) if discipline_aliases else "(none)",
    )

    for raw_alias in discipline_aliases:
        if should_stop():
            return processed_any
        
        alias = DISCIPLINE_ALIAS_REDIRECTS.get(raw_alias, raw_alias)
        
        if alias not in DISCIPLINE_SEARCH_GROUPS:
            logger.warning(f"Skipping discipline '{alias}' because no keyword group was found.")
            continue
            
        clusters = DISCIPLINE_SEARCH_GROUPS[alias]
        label = DISCIPLINE_ALIAS_LABELS.get(alias, alias)
        logger.info(f"--- MODE: Discipline Search ({label}) ---")

        # Load rotation state
        rot_state = load_discipline_rotation(alias)
        active_cluster = 0
        profiles_collected = 0
        if rot_state and _is_recent_state(rot_state.get("updated_at"), config.SCRAPE_RESUME_MAX_AGE_DAYS):
            active_cluster = rot_state.get("active_cluster", 0)
            profiles_collected = rot_state.get("profiles_collected", 0)
            
            # Rotation logic check: if we already hit 100, swap
            if profiles_collected >= PROFILES_PER_CLUSTER_THRESHOLD:
                active_cluster = 1 if active_cluster == 0 else 0
                profiles_collected = 0

        # Enforce valid primary bounds gracefully
        if active_cluster not in [0, 1]:
            active_cluster = 0

        logger.info(f"Resuming {label} at Cluster {active_cluster + 1} with {profiles_collected} profiles previously collected.")

        clusters_to_run = [active_cluster]
        other_primary = 1 if active_cluster == 0 else 0
        clusters_to_run.append(other_primary)
        
        primary_success = False

        for idx in clusters_to_run:
            if should_stop():
                return processed_any

            cluster_query = clusters[idx]
            cluster_label = f"Cluster {idx+1}"
            
            logger.info(f"[*] Processing {label} -> {cluster_label}")
            
            # Setup URL
            ok = nav.get(UNT_DISCIPLINE_SEARCH_BASE_URL)
            if not ok:
                logger.warning("UNT people search page unhealthy. Skipping.")
                break 

            linkedin_keyword_query = _format_linkedin_keyword_query(cluster_query)
            if not linkedin_keyword_query:
                logger.warning(f"{cluster_label} had no usable keywords after normalization. Skipping.")
                continue
            
            time.sleep(3)
            submitted = _submit_discipline_keywords(scraper, linkedin_keyword_query)
            if not submitted:
                logger.warning("Could not submit keywords via search box. Falling back to URL param.")

            discipline_base_url = _build_discipline_search_base_url(
                scraper.driver.current_url if submitted else UNT_DISCIPLINE_SEARCH_BASE_URL,
                linkedin_keyword_query,
            )
            
            if "keywords=" not in discipline_base_url:
                logger.warning("URL did not contain keywords. Skipping query.")
                continue
            
            mode_key = f"discipline:{alias}:cluster_{idx+1}"
            remaining_quota = max(0, PROFILES_PER_CLUSTER_THRESHOLD - profiles_collected)

            result, chunk_scraped = _run_search_results_mode(
                scraper=scraper,
                nav=nav,
                history_mgr=history_mgr,
                base_url=discipline_base_url,
                state_mode_key=mode_key,
                mode_label=f"discipline: {alias} | {cluster_label}",
                max_profiles_for_mode=remaining_quota,
            )
            
            processed_any = True
            profiles_collected += chunk_scraped
            
            save_discipline_rotation(alias, idx, profiles_collected)

            if chunk_scraped > 0 or profiles_collected >= PROFILES_PER_CLUSTER_THRESHOLD:
                primary_success = True

            if profiles_collected >= PROFILES_PER_CLUSTER_THRESHOLD:
                logger.info(f"Target of {PROFILES_PER_CLUSTER_THRESHOLD} reached for {cluster_label}.")
                break
                
            if result in ("no_results", "exhausted"):
                logger.info(f"{cluster_label} was exhausted. Resetting profile count and rotating to next cluster.")
                profiles_collected = 0
                save_discipline_rotation(alias, other_primary, 0)
                continue
                
            if result in ("stopped", "network_error"):
                break

        if not primary_success:
            logger.warning(f"Both primary clusters failed to yield new profiles for {label}. Triggering fallback clusters.")
            fallback_success = False
            for fb_idx in range(2, len(clusters)):
                if should_stop():
                    return processed_any
                
                cluster_query = clusters[fb_idx]
                cluster_label = f"Fallback Cluster {fb_idx+1}"
                
                logger.info(f"[*] Processing {label} -> {cluster_label}")
                
                ok = nav.get(UNT_DISCIPLINE_SEARCH_BASE_URL)
                if not ok: break 
                time.sleep(3)
                
                fb_keyword_query = _format_linkedin_keyword_query(cluster_query)
                if not fb_keyword_query:
                    logger.warning(f"{cluster_label} had no usable keywords after normalization. Skipping.")
                    continue
                
                submitted = _submit_discipline_keywords(scraper, fb_keyword_query)
                discipline_base_url = _build_discipline_search_base_url(
                    scraper.driver.current_url if submitted else UNT_DISCIPLINE_SEARCH_BASE_URL,
                    fb_keyword_query,
                )
                
                mode_key = f"discipline:{alias}:fallback_{fb_idx+1}"
                result, chunk_scraped = _run_search_results_mode(
                    scraper=scraper,
                    nav=nav,
                    history_mgr=history_mgr,
                    base_url=discipline_base_url,
                    state_mode_key=mode_key,
                    mode_label=f"fallback: {alias} | {cluster_label}",
                    max_profiles_for_mode=PROFILES_PER_CLUSTER_THRESHOLD,
                )
                
                processed_any = True
                
                if chunk_scraped > 0:
                    fallback_success = True

                if result in ("stopped", "network_error"):
                    break
                    
            if not fallback_success:
                logger.warning(f"No new results found for {label} across ALL clusters, possible LinkedIn cap or blocking.")

        logger.info(f"✅ Finished discipline search ({label})")

    return processed_any

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
            if _should_recover_from_session_error(msg):
                success, data = _recover_browser_session(scraper, profile_url, nav)
                if success:
                    if data == "PAGE_NOT_FOUND":
                        dead_urls.append(profile_url)
                    elif data and data != "PAGE_NOT_FOUND":
                        _save_and_track(data, profile_url, history_mgr)
                else:
                    logger.error(f"❌ Error processing {profile_url}: recovery failed")
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


def run_update_mode(scraper, nav, history_mgr):
    """
    Re-scrape existing alumni that are due for refresh, ordered by oldest
    last_updated first.
    """
    profiles, cutoff_date = database_handler.get_outdated_profiles_from_db()
    if cutoff_date:
        logger.info(f"📅 Update cutoff: last_updated older than {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")

    if not profiles:
        logger.info("📋 Update mode: no existing profiles currently require refresh.")
        return

    queue = []
    seen = set()
    for row in profiles:
        if isinstance(row, dict):
            raw_url = row.get("linkedin_url")
            last_updated = row.get("last_updated")
        else:
            raw_url = row[0] if len(row) > 0 else ""
            last_updated = row[3] if len(row) > 3 else None

        profile_url = _normalize_profile_url(raw_url)
        if not profile_url or profile_url in seen or config.is_blocked_url(profile_url):
            continue
        seen.add(profile_url)
        queue.append((profile_url, last_updated))

    if not queue:
        logger.info("📋 Update mode: no valid URLs available after filtering blocked/invalid entries.")
        return

    logger.info(
        "📋 Update mode: %s profiles queued (ordered oldest to newest by last_updated)",
        len(queue),
    )

    dead_urls = []
    for profile_url, last_updated in queue:
        if should_stop() or check_force_exit():
            break

        if last_updated:
            logger.info(f"🔄 Updating {profile_url} (last_updated={last_updated})")
        else:
            logger.info(f"🔄 Updating {profile_url}")

        try:
            data = scraper.scrape_profile_page(profile_url)
            if data == "PAGE_NOT_FOUND":
                dead_urls.append(profile_url)
                continue
            if data and data != "PAGE_NOT_FOUND":
                _save_and_track(data, profile_url, history_mgr)
        except Exception as e:
            if _should_recover_from_session_error(str(e)):
                success, data = _recover_browser_session(scraper, profile_url, nav)
                if success:
                    if data == "PAGE_NOT_FOUND":
                        dead_urls.append(profile_url)
                    elif data and data != "PAGE_NOT_FOUND":
                        _save_and_track(data, profile_url, history_mgr)
                else:
                    logger.error(f"❌ Error processing {profile_url}: recovery failed")
            else:
                logger.error(f"❌ Error processing {profile_url}: {e}")

        if should_stop():
            break

        wait_between_profiles()

    logger.info("✅ Update mode complete")
    if dead_urls:
        logger.warning(
            "Update mode detected %s dead/removed profiles. Run Review mode in terminal if you want interactive cleanup.",
            len(dead_urls),
        )


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
    global _geocode_network_failures_this_run
    global _geocode_success_this_run
    global _cloud_upsert_successes_this_run, _cloud_upsert_failures_this_run
    global _sqlite_writes_this_run
    global _flagged_urls_this_run
    global _current_scrape_run_id, _current_scrape_run_uuid, session_profiles_scraped
    _cloud_upsert_consecutive_failures = 0
    _cloud_upsert_disabled_for_run = False
    _geocode_failures_this_run = 0
    _geocode_failure_locations = set()
    _geocode_network_failures_this_run = 0
    _geocode_success_this_run = 0
    _cloud_upsert_successes_this_run = 0
    _cloud_upsert_failures_this_run = 0
    _sqlite_writes_this_run = 0
    _flagged_urls_this_run = set()
    session_profiles_scraped = 0
    _current_scrape_run_id = None
    _current_scrape_run_uuid = str(uuid.uuid4())

    selected_disciplines = _get_selected_search_disciplines() if config.SCRAPER_MODE == "search" else []
    try:
        _current_scrape_run_id = create_scrape_run(
            run_uuid=_current_scrape_run_uuid,
            scraper_email=config.LINKEDIN_EMAIL,
            scraper_mode=config.SCRAPER_MODE,
            selected_disciplines=selected_disciplines,
        )
    except Exception as create_run_err:
        logger.warning(f"Could not create scrape run metadata: {create_run_err}")

    history_mgr = database_handler.HistoryManager()
    history_mgr.sync_with_db()

    disable_db = os.getenv("DISABLE_DB", "0") == "1"
    logger.info(
        "PERSISTENCE CONFIG: DISABLE_DB=%s, USE_SQLITE_FALLBACK=%s",
        int(disable_db),
        os.getenv("USE_SQLITE_FALLBACK", "1"),
    )
    if disable_db:
        logger.warning("[red bold]CLOUD WRITE DISABLED:[/red bold] DISABLE_DB=1, so writes are local only.")

    try:
        fallback_manager = get_connection_manager()
        if fallback_manager.is_offline():
            logger.warning("[red bold]FALLBACK MODE ACTIVE:[/red bold] Using SQLite until cloud connectivity is restored.")
    except Exception:
        pass

    database_handler.ensure_alumni_output_csv()

    scraper = LinkedInScraper()
    scraper.setup_driver()
    run_status = "completed"

    try:
        if not scraper.login():
            diagnosis = scraper.diagnose_login_state()
            logger.error("LOGIN FAILURE")
            logger.error("MANUAL INTERVENTION NEEDED: %s", diagnosis.get("message", "Unknown login issue."))
            logger.error("If challenge persists, restart with HEADLESS=false and complete verification manually.")
            logger.error("ACTION|manual_intervention_needed=1")
            logger.error("ACTION|reason=%s", diagnosis.get("code", "unknown_login_failure"))
            logger.error("ACTION|suggest_restart_headed=1")
            run_status = "login_failed"
            return

        # ✅ Defense layer initialized here (minimal + safe)
        nav = SafeNavigator(scraper.driver)

        start_exit_listener()

        if config.SCRAPER_MODE in {"names", "connections"}:
            run_names_mode(scraper, nav, history_mgr)
        elif config.SCRAPER_MODE == "review":
            run_review_mode(scraper, nav, history_mgr)
        elif config.SCRAPER_MODE == "update":
            run_update_mode(scraper, nav, history_mgr)
        elif config.SCRAPER_MODE == "search":
            if selected_disciplines:
                logger.info("Selected discipline aliases: %s", ", ".join(selected_disciplines))
                used_discipline_search = run_discipline_search_mode(scraper, nav, history_mgr, selected_disciplines)
                if not used_discipline_search and not should_stop():
                    logger.warning(
                        "Discipline-specific search setup failed; falling back to all-engineering search filter."
                    )
                    run_search_mode(scraper, nav, history_mgr)
            else:
                run_search_mode(scraper, nav, history_mgr)
        else:
            run_search_mode(scraper, nav, history_mgr)

    except KeyboardInterrupt:
        run_status = "interrupted"
        logger.warning("Stopped by user")
    except Exception as unhandled_err:
        run_status = "failed"
        logger.exception(f"Unhandled scraping error: {unhandled_err}")
    finally:
        run_duration_seconds = int((datetime.now() - SCRIPT_START_TIME).total_seconds())
        logger.info("=" * 60)
        logger.info("RUN SUMMARY")
        logger.info("  user: %s", config.LINKEDIN_EMAIL or "unknown")
        logger.info("  mode: %s", config.SCRAPER_MODE)
        logger.info("  scraped count: %s", session_profiles_scraped)
        logger.info("  run duration: %s", _format_duration_short(run_duration_seconds))
        logger.info("  flagged count: %s", len(_flagged_urls_this_run))
        logger.info("  review source: scraper/output/flagged_for_review.txt")
        logger.info(
            "  cloud upload success/fail: %s/%s",
            _cloud_upsert_successes_this_run,
            _cloud_upsert_failures_this_run,
        )
        logger.info("  sqlite mirror writes: %s", _sqlite_writes_this_run)
        logger.info(
            "  geocode success/fail/unknown: %s/%s/%s",
            _geocode_success_this_run,
            _geocode_network_failures_this_run,
            _geocode_failures_this_run,
        )
        if _geocode_failure_locations:
            logger.info("  unknown locations: %s", "; ".join(sorted(_geocode_failure_locations)[:10]))
        logger.info("=" * 60)

        # Machine-readable lines for GUI summary parsing.
        logger.info("SUMMARY|user=%s", config.LINKEDIN_EMAIL or "unknown")
        logger.info("SUMMARY|mode=%s", config.SCRAPER_MODE)
        logger.info("SUMMARY|scraped_count=%s", session_profiles_scraped)
        logger.info("SUMMARY|run_duration=%s", _format_duration_short(run_duration_seconds))
        logger.info("SUMMARY|flagged_count=%s", len(_flagged_urls_this_run))
        logger.info("SUMMARY|review_path=scraper/output/flagged_for_review.txt")
        logger.info("SUMMARY|cloud_success=%s", _cloud_upsert_successes_this_run)
        logger.info("SUMMARY|cloud_fail=%s", _cloud_upsert_failures_this_run)
        logger.info("SUMMARY|geocode_success=%s", _geocode_success_this_run)
        logger.info("SUMMARY|geocode_fail=%s", _geocode_network_failures_this_run)
        logger.info("SUMMARY|geocode_unknown=%s", _geocode_failures_this_run)
        if _geocode_failure_locations:
            logger.info("SUMMARY|unknown_locations=%s", "; ".join(sorted(_geocode_failure_locations)[:10]))

        if _current_scrape_run_id:
            finalize_scrape_run(
                run_id=_current_scrape_run_id,
                status=run_status,
                profiles_scraped=session_profiles_scraped,
                cloud_disabled=_cloud_upsert_disabled_for_run,
                geocode_unknown_count=_geocode_failures_this_run,
                geocode_network_failure_count=_geocode_network_failures_this_run,
                notes=f"run_uuid={_current_scrape_run_uuid}",
            )

        if _cloud_upsert_disabled_for_run:
            logger.warning(
                "WARNING: CLOUD DATABASE WAS UNREACHABLE. SCRAPED INFORMATION WAS STORED IN LOCAL SQLITE DATABASE AND CSV FILE."
            )
            logger.warning(
                "PRESS UPLOAD TO CLOUD TO UPLOAD WHEN YOU HAVE A SUITABLE CONNECTION."
            )

        if _geocode_network_failures_this_run > 0:
            logger.warning(
                "WARNING: GEOCODING SERVICE OR NETWORK WAS UNAVAILABLE FOR PART OF THIS SCRAPE RUN (%s attempts).",
                _geocode_network_failures_this_run,
            )
            logger.warning(
                "PRESS BACKFILL GEOCODE (OPTIONAL) WHEN YOU HAVE A SUITABLE CONNECTION TO RETRY THESE LOCATIONS."
            )
            logger.warning(
                "ACTION: RETRY GEO-CODING LATER USING BACKFILL GEOCODE AFTER NETWORK/SERVICE RECOVERS."
            )

        if _geocode_failures_this_run > 0:
            logger.warning(
                "WARNING: SOME LOCATIONS WERE UNKNOWN AND COULD NOT BE GEOLOCATED (%s profiles, %s unique locations).",
                _geocode_failures_this_run,
                len(_geocode_failure_locations),
            )
            sample_unknown_locations = sorted(_geocode_failure_locations)[:10]
            if sample_unknown_locations:
                logger.warning("Unknown Location(s):")
                for unknown_location in sample_unknown_locations:
                    logger.warning("  - %s", unknown_location)
            logger.warning(
                "REVIEW OR CLEAN LOCATION TEXT (E.G., REGION-ONLY LABELS) AND RUN BACKFILL GEOCODE (OPTIONAL) TO RETRY."
            )
            logger.warning("UNKNOWN_LOCATIONS_LIST: %s", "; ".join(sample_unknown_locations))
        stop_exit_listener()
        scraper.quit()


def _verify_cloud_insert_after_delay(profile_url, delay_seconds=5):
    if not profile_url:
        return

    def _worker():
        acquired = _cloud_verify_semaphore.acquire(timeout=0.1)
        if not acquired:
            return
        try:
            time.sleep(max(0, int(delay_seconds)))
            conn = None
            try:
                conn = get_direct_mysql_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM alumni WHERE linkedin_url = %s OR linkedin_url = %s LIMIT 1",
                        (profile_url, f"{profile_url}/"),
                    )
                    found = bool(cur.fetchone())
                if found:
                    logger.info(f"VERIFY: Cloud DB confirmed insert for {profile_url}")
                else:
                    logger.warning(f"VERIFY WARNING: Cloud DB did not contain {profile_url} after {delay_seconds}s")
            except Exception as verify_err:
                logger.warning(f"VERIFY WARNING: Cloud insert check skipped for {profile_url}: {verify_err}")
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
        finally:
            _cloud_verify_semaphore.release()

    threading.Thread(target=_worker, daemon=True, name="CloudInsertVerify").start()


if __name__ == "__main__":
    main()
