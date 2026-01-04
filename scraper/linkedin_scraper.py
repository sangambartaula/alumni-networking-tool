import os
import sys
import time
import json
import logging
import random
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException
from bs4 import BeautifulSoup
import pandas as pd
import re
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'backend'))
from database import save_visited_profile, get_all_visited_profiles


# -------------------------------
# CLEAN JOB TITLE FUNCTION
# -------------------------------
def clean_job_title(raw_title: str) -> str:
    """
    Cleans job titles by removing employment-type labels like:
    Full-time, Part-time, Internship, Volunteer, Contract, etc.
    """
    if not raw_title:
        return ""

    raw = " ".join(raw_title.strip().split())

    banned_exact = {
        "Full-time", "Part-time", "Internship", "Contract", "Temporary",
        "Volunteer", "Apprenticeship", "Self-employed", "Freelance"
    }

    if raw in banned_exact:
        return ""

    for bad in banned_exact:
        raw = raw.replace(f"· {bad}", "")
        raw = raw.replace(bad, "")

    return " ".join(raw.split()).strip()


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
logger.info(f"Loading .env from: {env_path}")

# Configuration
LINKEDIN_EMAIL = os.getenv("LINKEDIN_EMAIL")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
TESTING = os.getenv("TESTING", "false").lower() == "true"
USE_COOKIES = os.getenv("USE_COOKIES", "false").lower() == "true"
LINKEDIN_COOKIES_PATH = os.getenv("LINKEDIN_COOKIES_PATH", "linkedin_cookies.json")
SCRAPER_MODE = os.getenv("SCRAPER_MODE", "names").lower()  # 'names', 'search', or 'connections'
OUTPUT_CSV_ENV = os.getenv("OUTPUT_CSV", "UNT_Alumni_Data.csv")
UPDATE_FREQUENCY = os.getenv("UPDATE_FREQUENCY", "6 months")
CONNECTIONS_CSV_PATH = os.getenv("CONNECTIONS_CSV", "connections.csv")

# Settle time after opening profile pages (keep 0 for now; bump to 5 if still flaky)
PAGE_SETTLE_SECONDS = int(os.getenv("PAGE_SETTLE_SECONDS", "0"))
POST_SECTION_WAIT_SECONDS = float(os.getenv("POST_SECTION_WAIT_SECONDS", "0"))

# Wait max for Education to be parse-ready (when tabbed out / throttled)
EDU_READY_TIMEOUT_SECONDS = int(os.getenv("EDU_READY_TIMEOUT_SECONDS", "30"))

# Set delay between profiles based on TESTING mode
if TESTING:
    MIN_DELAY = 15
    MAX_DELAY = 60
else:
    MIN_DELAY = 120
    MAX_DELAY = 600

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
OUTPUT_CSV = OUTPUT_DIR / OUTPUT_CSV_ENV
COOKIES_FILE = OUTPUT_DIR / LINKEDIN_COOKIES_PATH

VISITED_HISTORY_FILE = OUTPUT_DIR / "visited_history.csv"
VISITED_HISTORY_COLUMNS = ['profile_url', 'saved', 'visited_at', 'update_needed', 'last_db_update']

# Added columns:
# - school_start_date: start of latest UNT education (supports year only or month+year)
# - job_start_date, job_end_date: for latest experience entry (supports year only or month+year)
# - working_while_studying: yes/no (based on overlap rules described)
CSV_COLUMNS = [
    'name', 'headline', 'location',
    'job_title', 'company', 'job_start_date', 'job_end_date',
    'education', 'major', 'school_start_date', 'graduation_year',
    'working_while_studying',
    'profile_url', 'scraped_at'
]

logger.info(f"SCRAPER_MODE: {SCRAPER_MODE}")
logger.info(f"TESTING MODE: {TESTING}")
logger.info(f"DELAY RANGE: {MIN_DELAY}s - {MAX_DELAY}s")
logger.info(f"PAGE_SETTLE_SECONDS: {PAGE_SETTLE_SECONDS}s")
logger.info(f"POST_SECTION_WAIT_SECONDS: {POST_SECTION_WAIT_SECONDS}s")
logger.info(f"EDU_READY_TIMEOUT_SECONDS: {EDU_READY_TIMEOUT_SECONDS}s")
logger.info(f"OUTPUT_CSV full path: {OUTPUT_CSV.absolute()}")
logger.info(f"VISITED_HISTORY_FILE: {VISITED_HISTORY_FILE.absolute()}")


def parse_frequency(frequency_str: str) -> timedelta:
    """Parse frequency string like '6 months', '1 year', '2 years' into a timedelta."""
    try:
        parts = frequency_str.strip().lower().split()
        if len(parts) != 2:
            return timedelta(days=180)

        amount = int(parts[0])
        unit = parts[1].rstrip('s')

        if unit == "day":
            return timedelta(days=amount)
        if unit == "month":
            return timedelta(days=amount * 30)
        if unit == "year":
            return timedelta(days=amount * 365)
        return timedelta(days=180)
    except Exception:
        return timedelta(days=180)


def get_outdated_profiles():
    """Get alumni profiles that need updating based on UPDATE_FREQUENCY."""
    try:
        import mysql.connector

        conn = mysql.connector.connect(
            host=os.getenv('MYSQLHOST'),
            user=os.getenv('MYSQLUSER'),
            password=os.getenv('MYSQLPASSWORD'),
            database=os.getenv('MYSQL_DATABASE'),
            port=int(os.getenv('MYSQLPORT', 3306))
        )

        frequency_delta = parse_frequency(UPDATE_FREQUENCY)
        cutoff_date = datetime.now() - frequency_delta

        with conn.cursor() as cur:
            cur.execute("""
                SELECT linkedin_url, first_name, last_name, last_updated
                FROM alumni
                WHERE last_updated < %s
                ORDER BY last_updated ASC
            """, (cutoff_date,))
            profiles = cur.fetchall()

        conn.close()
        return profiles, cutoff_date
    except Exception as e:
        logger.error(f"Error fetching outdated profiles: {e}")
        return [], None


def load_names_from_csv(csv_path: Path):
    """Read a list of names from a CSV file."""
    try:
        df = pd.read_csv(csv_path)

        if 'name' in df.columns:
            return [str(n).strip() for n in df['name'].dropna().unique() if str(n).strip()]

        if 'first_name' in df.columns and 'last_name' in df.columns:
            names = [
                f"{str(r).strip()} {str(l).strip()}".strip()
                for r, l in zip(df['first_name'].fillna(''), df['last_name'].fillna(''))
                if (str(r).strip() or str(l).strip())
            ]
            # de-dupe preserving order
            seen = set()
            uniq = []
            for n in names:
                if n not in seen:
                    seen.add(n)
                    uniq.append(n)
            return uniq

        raise ValueError("Input CSV must contain either 'name' or ('first_name','last_name').")
    except Exception as e:
        logger.error(f"Failed to read names from {csv_path}: {e}")
        return []


class LinkedInSearchScraper:
    # ============================================================
    # Regex/constants
    # ============================================================
    _MONTHS_RE = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"

    # Existing: month-year range used for Experience
    _EXPERIENCE_RANGE_RE = re.compile(
        rf"^{_MONTHS_RE}\s+\d{{4}}\s*[-–—]\s*(Present|{_MONTHS_RE}\s+\d{{4}})",
        re.IGNORECASE
    )

    # New: "year - year/Present" and "Mon YYYY - Mon YYYY/Present" and mix (Mon YYYY - YYYY)
    _DATE_RANGE_RE = re.compile(
        rf"(?P<start>(?:{_MONTHS_RE}\s+\d{{4}})|(?:\d{{4}}))\s*[-–—]\s*(?P<end>(?:Present)|(?:{_MONTHS_RE}\s+\d{{4}})|(?:\d{{4}}))",
        re.IGNORECASE
    )

    _YEAR_RANGE_RE = re.compile(r"(\d{4})\s*[-–—]\s*(\d{4}|Present)", re.IGNORECASE)
    _YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")

    _UNT_KEYWORDS = ("unt", "university of north texas", "north texas")

    _DEGREE_LEVELS = {
        'ph.d': 100, 'phd': 100, 'doctor': 100, 'doctorate': 100, 'd.phil': 100,
        'master': 80, 'ms': 80, 'm.s': 80, 'mba': 80, 'm.b.a': 80, 'ma': 80, 'm.a': 80,
        'bachelor': 60, 'bs': 60, 'b.s': 60, 'ba': 60, 'b.a': 60, 'bba': 60,
        'associate': 40,
    }

    _ENGINEERING_KEYWORDS = (
        'engineering', 'engineer', 'computer science', 'computer engineering',
        'mechanical', 'electrical', 'civil', 'chemical', 'aerospace',
        'software', 'hardware', 'materials', 'industrial', 'manufacturing',
        'biomedical', 'petroleum', 'environmental', 'systems',
        'technology', 'physics', 'mathematics', 'math',
        'data science', 'cybersecurity', 'information technology',
        'electronics', 'robotics', 'mechatronics', 'energy',
    )

    def __init__(self):
        self.driver = None
        self.wait = None
        self.visited_history = {}
        self.scraper_mode = SCRAPER_MODE

        self.ensure_csv_headers()
        self._ensure_visited_history_headers()

    # ============================================================
    # Date parsing + overlap logic
    # ============================================================

    def _month_to_num(self, m: str) -> int:
        month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
        }
        return month_map.get((m or "").strip().title(), 0)

    def _parse_date_token(self, token: str):
        """
        Parse a token that may be:
        - "2024"
        - "Aug 2022"
        - "Present"
        Returns dict:
          {
            "raw": str,
            "is_present": bool,
            "year": int|None,
            "month": int|None,
            "has_month": bool
          }
        """
        raw = (token or "").strip()
        if not raw:
            return {"raw": "", "is_present": False, "year": None, "month": None, "has_month": False}

        if raw.lower().startswith("present"):
            return {"raw": "Present", "is_present": True, "year": None, "month": None, "has_month": False}

        mm = re.match(rf"^(?P<m>{self._MONTHS_RE})\s+(?P<y>\d{{4}})$", raw, re.I)
        if mm:
            m = mm.group("m").title()
            y = int(mm.group("y"))
            return {"raw": f"{m} {y}", "is_present": False, "year": y, "month": self._month_to_num(m), "has_month": True}

        yy = re.match(r"^(?P<y>\d{4})$", raw)
        if yy:
            y = int(yy.group("y"))
            return {"raw": f"{y}", "is_present": False, "year": y, "month": None, "has_month": False}

        # Unknown format; keep raw but mark unparsable
        return {"raw": raw, "is_present": False, "year": None, "month": None, "has_month": False}

    def _parse_date_range_line(self, line: str):
        """
        Extract a start/end date from a line like:
        - "2022 – 2024"
        - "Aug 2022 - May 2024"
        - "Aug 2022 - 2024"
        - "2024 - Present"
        Returns (start_dict, end_dict) or (None, None)
        """
        if not line:
            return None, None

        m = self._DATE_RANGE_RE.search(line)
        if not m:
            return None, None

        start = self._parse_date_token(m.group("start"))
        end = self._parse_date_token(m.group("end"))

        # If we couldn't parse year, treat as invalid
        if (not start.get("is_present")) and (start.get("year") is None):
            return None, None
        if (not end.get("is_present")) and (end.get("year") is None):
            return None, None

        return start, end

    def _format_date_for_storage(self, d: dict) -> str:
        """
        Store exactly as either:
        - "YYYY"
        - "Mon YYYY"
        - "Present"
        """
        if not d:
            return ""
        if d.get("is_present"):
            return "Present"
        y = d.get("year")
        if not y:
            return ""
        if d.get("has_month") and d.get("month"):
            # use raw's normalized "Mon YYYY"
            return d.get("raw") or ""
        return str(y)

    def _latest_end_sort_key(self, end_dict: dict):
        """
        For sorting date ranges by end date (latest first).
        Present should sort as max.
        For year-only, month=0.
        """
        if not end_dict:
            return (0, 0)
        if end_dict.get("is_present"):
            return (9999, 12)
        y = end_dict.get("year") or 0
        m = end_dict.get("month") or 0
        return (y, m)

    def _date_to_comparable(self, d: dict, bound: str):
        """
        Convert a parsed date dict to a comparable (year, month, granularity) tuple.
        granularity: "month" or "year"
        bound affects how to interpret year-only dates:
          - for "start": treat year-only as (year, 1)
          - for "end": treat year-only as (year, 12)
        Present -> (9999, 12, "month")
        """
        if not d:
            return None
        if d.get("is_present"):
            return (9999, 12, "month")

        y = d.get("year")
        if not y:
            return None

        if d.get("has_month") and d.get("month"):
            return (y, int(d.get("month")), "month")

        # year-only
        if bound == "start":
            return (y, 1, "year")
        return (y, 12, "year")

    def _working_while_studying(self, school_start: dict, school_end: dict, job_start: dict, job_end: dict) -> bool:
        """
        Rules from prompt:
        - Assume no overlap if the end of one and start of one are the same year.
        - Check for month if present in BOTH.
          Example:
            School: Aug 2022 - May 2025
            Job:   April 2025 - Present  => overlap (months provided for both)
            School: Aug 2022 - May 2025
            Job:   2025 - Present         => NOT overlap (job is year-only; same year boundary doesn't count)
        Approach:
        - If both sides have month granularity for the boundary comparison, do month-level overlap.
        - Otherwise do year-level overlap, but treat same-year boundary as NOT overlapping.
        """
        ss = self._date_to_comparable(school_start, "start")
        se = self._date_to_comparable(school_end, "end")
        js = self._date_to_comparable(job_start, "start")
        je = self._date_to_comparable(job_end, "end")

        if not (ss and se and js and je):
            return False

        # Determine if we can do month-precise checks: only if both ranges have month granularity on their endpoints.
        # We'll consider month-precise overlap if BOTH:
        # - school has months for start+end
        # - job has months for start and (end is month OR present)
        school_month_precise = bool(school_start.get("has_month") and school_end.get("has_month"))
        job_month_precise = bool(job_start.get("has_month") and (job_end.get("has_month") or job_end.get("is_present")))

        if school_month_precise and job_month_precise:
            # Standard interval overlap with month precision
            # overlap if js <= se AND ss <= je
            return (js[0], js[1]) <= (se[0], se[1]) and (ss[0], ss[1]) <= (je[0], je[1])

        # Year-level overlap with "same-year boundary is NOT overlap"
        ss_y, se_y, js_y, je_y = ss[0], se[0], js[0], je[0]

        # If job starts after school ends -> no overlap
        if js_y > se_y:
            return False

        # If school starts after job ends -> no overlap
        if ss_y > je_y:
            return False

        # Boundary condition: "Assume no overlap if the end of one and start of one are the same year."
        # If job starts in same year school ends, do NOT count as overlap (unless we handled month-precise above).
        if js_y == se_y:
            return False

        # If school starts in same year job ends, do NOT count as overlap
        if ss_y == je_y:
            return False

        return True

    # ============================================================
    # Render helpers (focus + readiness waits)
    # ============================================================

    def _force_focus(self):
        """Attempt to focus the Selenium tab/window (helps when background throttled)."""
        try:
            self.driver.switch_to.window(self.driver.current_window_handle)
            self.driver.execute_script("window.focus();")
        except Exception:
            pass

    def _wait_for_top_card(self, timeout: int = 25) -> bool:
        """
        Wait until the profile top card has rendered enough to parse.
        This helps when Chrome/LinkedIn throttles rendering while the window is backgrounded.
        """
        end = time.time() + timeout
        while time.time() < end:
            try:
                ok = self.driver.execute_script("""
                    const m = document.querySelector('main');
                    if (!m) return false;

                    const h = m.querySelector('h1, h2');
                    const name = h ? (h.innerText || '').trim() : '';
                    return name.length >= 2;
                """)
                if ok:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    def _wait_for_education_ready(self, timeout: int = 30) -> bool:
        """
        Wait until the Education section contains at least one plausible school line.
        Returns True if ready, False if timed out.
        """
        end = time.time() + timeout
        while time.time() < end:
            try:
                ok = self.driver.execute_script("""
                    const m = document.querySelector('main');
                    if (!m) return false;

                    const headings = Array.from(m.querySelectorAll('h2,h3'));
                    const h = headings.find(x => (x.innerText || '').trim().toLowerCase() === 'education');
                    if (!h) return false;

                    const root = h.closest('section') || h.closest('div');
                    if (!root) return false;

                    const ps = Array.from(root.querySelectorAll('p'))
                      .map(p => (p.innerText || '').trim())
                      .filter(Boolean);

                    // "school-like": contains common institution keywords, or is at least a reasonable text line
                    const schoolish = ps.some(t =>
                        /university|college|institute|school|academy/i.test(t) || t.length >= 6
                    );
                    return schoolish;
                """)
                if ok:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    # ============================================================
    # Debug helpers
    # ============================================================

    def _save_debug_dump(self, profile_url: str, label: str):
        """Save HTML + metadata under output/debug for troubleshooting (TESTING only)."""
        if not TESTING:
            return
        try:
            debug_dir = OUTPUT_DIR / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)

            safe_label = re.sub(r"[^a-zA-Z0-9_.-]+", "_", label).strip("_")
            safe_profile = re.sub(r"[^a-zA-Z0-9]+", "_", profile_url).strip("_")[:80]

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            html_path = debug_dir / f"{ts}__{safe_label}__{safe_profile}.html"
            meta_path = debug_dir / f"{ts}__{safe_label}__{safe_profile}.json"

            html = self.driver.page_source if self.driver else ""
            html_path.write_text(html or "", encoding="utf-8")

            meta = {
                "label": label,
                "profile_url": profile_url,
                "current_url": getattr(self.driver, "current_url", None),
                "title": getattr(self.driver, "title", None),
                "saved_at": datetime.now().isoformat(),
            }
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        except Exception as e:
            logger.debug(f"Debug dump failed: {e}")

    def _page_looks_blocked(self) -> bool:
        """Heuristic: detect authwall/checkpoint/robot pages or empty shell renders."""
        try:
            url = (self.driver.current_url or "").lower()
            title = (self.driver.title or "").lower()
            html = (self.driver.page_source or "").lower()

            if any(x in url for x in ("checkpoint", "login", "authwall", "/uas/login", "challenge")):
                return True
            if any(x in title for x in ("sign in", "log in", "security verification", "challenge")):
                return True
            if any(m in html for m in ("authwall", "checkpoint", "captcha", "security verification", "unusual activity")):
                return True
            if len(html.strip()) < 2000:
                return True
            return False
        except Exception:
            return False

    def wait_for_profile_sections_to_load(self, timeout: int = 30) -> bool:
        """Wait for Experience/Education text to appear in DOM (weak signal; keep for logging)."""
        try:
            end = time.time() + timeout
            while time.time() < end:
                html = self.driver.page_source or ""
                if "Experience" in html or "Education" in html:
                    return True
                time.sleep(0.5)
            return False
        except Exception:
            return False

    def _get_main_html(self) -> str:
        """Return <main> innerHTML if present, else empty string."""
        try:
            return self.driver.execute_script(
                "const m=document.querySelector('main'); return m ? m.innerHTML : '';"
            ) or ""
        except Exception:
            return ""

    # ============================================================
    # Top card parsing (Name / Headline / Location)
    # ============================================================

    def _looks_like_person_name(self, text: str) -> bool:
        if not text:
            return False
        t = " ".join(text.split())
        if len(t) < 2 or len(t) > 60:
            return False
        tl = t.lower()
        if any(b in tl for b in ("linkedin", "contact info", "connections", "followers", "open to work")):
            return False
        tokens = [x for x in re.split(r"\s+", t) if x]
        alpha_tokens = [x for x in tokens if re.search(r"[A-Za-z]", x)]
        return len(alpha_tokens) >= 2

    def _is_pronouns_line(self, text: str) -> bool:
        tl = text.lower().strip()
        return tl in {"he/him", "she/her", "they/them"} or ("/" in tl and len(tl) <= 12)

    def _is_network_line(self, text: str) -> bool:
        return re.fullmatch(r"·\s*(1st|2nd|3rd)", text.strip().lower()) is not None

    def _is_junk_topcard_line(self, text: str) -> bool:
        tl = text.strip().lower()
        if not tl or tl == "·":
            return True
        if "contact info" in tl:
            return True
        if "connections" in tl or "followers" in tl:
            return True
        return False

    def _looks_like_location(self, text: str) -> bool:
        if not text:
            return False
        if self._is_junk_topcard_line(text) or self._is_pronouns_line(text) or self._is_network_line(text):
            return False

        tl = text.lower().strip()
        if any(x in tl for x in ("metro", "metroplex", "metropolitan", "greater", " area")):
            return True

        if "," in text and 3 <= len(text) <= 80:
            return True

        if tl in {"united states", "india", "canada", "united kingdom"}:
            return True

        return False

    def _extract_topcard_name_headline_location(self, soup: BeautifulSoup):
        """
        Extract name/headline/location from the profile top card.
        Works with HTML where name may be in <h2>.
        Returns: (name, headline, location)
        """
        name = ""
        name_node = None

        for tag_name in ("h1", "h2"):
            for h in soup.find_all(tag_name):
                t = h.get_text(" ", strip=True)
                if self._looks_like_person_name(t):
                    name = t
                    name_node = h
                    break
            if name_node:
                break

        if not name_node:
            return "", "", ""

        container = name_node.find_parent("div") or name_node.parent
        for _ in range(8):
            if not container:
                break
            if len(container.find_all("p")) >= 3:
                break
            container = container.find_parent("div")

        if not container:
            container = soup

        p_lines = []
        for p in container.find_all("p"):
            txt = p.get_text(" ", strip=True)
            if txt and txt not in p_lines:
                p_lines.append(txt)

        headline = ""
        for line in p_lines:
            if self._is_junk_topcard_line(line):
                continue
            if self._is_pronouns_line(line) or self._is_network_line(line):
                continue
            if self._looks_like_location(line):
                continue
            headline = line
            break

        location = ""
        for line in p_lines:
            if self._looks_like_location(line):
                location = line
                break

        return name, headline, location

    # ============================================================
    # Section-finding and <p> parsing helpers (Experience/Education)
    # ============================================================

    def _clean_company_line(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(
            r"\s*·\s*(Full-time|Part-time|Contract|Internship|Temporary|Remote|Hybrid|On-site).*$",
            "",
            text,
            flags=re.I
        ).strip()

    def _find_section_root_by_heading(self, soup: BeautifulSoup, heading_text: str):
        heading_text_norm = heading_text.strip().lower()
        for tag_name in ("h2", "h3"):
            for h in soup.find_all(tag_name):
                if h.get_text(" ", strip=True).lower() == heading_text_norm:
                    return h.find_parent("section") or h.find_parent("div")
        return None

    def _p_texts_excluding_skills_and_descriptions(self, container):
        if not container:
            return []

        for desc in container.select("[data-testid='expandable-text-box']"):
            desc.decompose()

        lines = []
        for p in container.find_all("p"):
            if p.select_one("svg#skills-small"):
                continue
            t = p.get_text(" ", strip=True)
            if t:
                lines.append(t)

        seen = set()
        uniq = []
        for l in lines:
            if l not in seen:
                seen.add(l)
                uniq.append(l)
        return uniq

    def _extract_best_experience(self, soup: BeautifulSoup):
        """
        Returns:
          (job_title, company, job_start_dict, job_end_dict)
        based on the latest experience entry by end date (Present wins, then latest end date).
        Supports date lines with:
          - "YYYY - YYYY/Present"
          - "Mon YYYY - Mon YYYY/Present"
          - "Mon YYYY - YYYY"
        """
        exp_root = self._find_section_root_by_heading(soup, "Experience")
        if not exp_root:
            return "", "", None, None

        candidates = []
        for div in exp_root.find_all("div"):
            lines = self._p_texts_excluding_skills_and_descriptions(div)
            # collect date lines that have ANY supported range format
            if any(self._DATE_RANGE_RE.search(t) for t in lines):
                candidates.append(lines)

        parsed = []
        for lines in candidates:
            date_idx = next((i for i, t in enumerate(lines) if self._DATE_RANGE_RE.search(t)), None)
            if date_idx is None:
                continue

            start_d, end_d = self._parse_date_range_line(lines[date_idx])
            if not (start_d and end_d):
                continue

            context = [t for t in lines[max(0, date_idx - 3):date_idx] if t]

            company_candidate = ""
            for t in context:
                if re.search(r"\s*·\s*(Full-time|Part-time|Contract|Internship|Temporary)", t, re.I):
                    company_candidate = t
                    break
            if not company_candidate and context:
                company_candidate = context[-1]

            title_candidate = ""
            for t in context:
                if t == company_candidate:
                    continue
                if "·" not in t:
                    title_candidate = t
                    break
            if not title_candidate:
                for t in context:
                    if t != company_candidate:
                        title_candidate = t
                        break

            jt = clean_job_title(title_candidate)
            co = self._clean_company_line(company_candidate)

            end_key = self._latest_end_sort_key(end_d)
            if jt or co:
                parsed.append((end_key, jt, co, start_d, end_d))

        if not parsed:
            return "", "", None, None

        parsed.sort(key=lambda x: x[0], reverse=True)
        _, job_title, company, start_d, end_d = parsed[0]
        return job_title or "", company or "", start_d, end_d

    def _looks_like_invalid_degree_text(self, degree_text: str) -> bool:
        """
        If degree has something like "2023 - Jul 2024", filter it out.
        We treat degree lines that contain a date range (or are mostly dates) as invalid.
        """
        t = (degree_text or "").strip()
        if not t:
            return True

        # Contains a date range -> invalid as "major/degree"
        if self._DATE_RANGE_RE.search(t):
            return True

        # If it's basically only years and punctuation
        only_datey = re.sub(r"[0-9\s\-–—/().,]", "", t)
        if only_datey.strip() == "":
            return True

        return False

    def _extract_education_entries(self, soup: BeautifulSoup):
        """
        Returns list of entries:
          {
            "school": str,
            "degree": str,
            "graduation_year": str,  # keep existing behavior (end year if present)
            "school_start": dict|None,
            "school_end": dict|None
          }
        """
        edu_root = self._find_section_root_by_heading(soup, "Education")
        if not edu_root:
            return []

        entries = []
        for div in edu_root.find_all("div"):
            lines = self._p_texts_excluding_skills_and_descriptions(div)
            if len(lines) < 2:
                continue

            school = lines[0].strip()
            degree = lines[1].strip()

            if not school or len(school) < 3:
                continue

            # Find a date range line within remaining lines
            school_start = None
            school_end = None
            grad_year = ""

            for t in lines[2:]:
                s_d, e_d = self._parse_date_range_line(t)
                if s_d and e_d:
                    school_start, school_end = s_d, e_d
                    # grad_year stays as "end year" if present and not Present
                    if not e_d.get("is_present") and e_d.get("year"):
                        grad_year = str(e_d.get("year"))
                    break

                # fallback: if any year appears, keep existing behavior (last year in line)
                if self._YEAR_RANGE_RE.search(t) or self._YEAR_RE.search(t):
                    years = re.findall(r"\d{4}", t)
                    if years and not grad_year:
                        grad_year = years[-1]

            school_hint = bool(re.search(r"(university|college|institute|school|academy)", school, re.I))
            degree_hint = bool(re.search(r"(degree|bachelor|master|phd|mba|\bbs\b|\bms\b|\bba\b|\bma\b)", degree, re.I))
            if not (school_hint or degree_hint):
                continue

            # Filter invalid degree text like "2023 - Jul 2024"
            if self._looks_like_invalid_degree_text(degree):
                logger.warning(f"    ⚠️ No Major Detected. Filtered out invalid text: {degree}")
                degree = ""

            entries.append({
                "school": school,
                "degree": degree,
                "graduation_year": grad_year,
                "school_start": school_start,
                "school_end": school_end
            })

        seen = set()
        uniq = []
        for e in entries:
            key = (e["school"], e["degree"], e["graduation_year"], self._format_date_for_storage(e.get("school_start")), self._format_date_for_storage(e.get("school_end")))
            if key not in seen:
                seen.add(key)
                uniq.append(e)
        return uniq

    def _pick_best_unt_education(self, entries):
        """
        Pick the best *UNT* education entry.
        UNT must always win over non-UNT if present.
        """

        def degree_score(text: str) -> int:
            if not text:
                return 0
            tl = text.lower()
            for k, score in self._DEGREE_LEVELS.items():
                if k in tl:
                    return score
            return 30

        def is_engineering(text: str) -> bool:
            if not text:
                return False
            tl = text.lower()
            return any(k in tl for k in self._ENGINEERING_KEYWORDS)

        best = None
        best_score = -1

        for e in entries:
            school_lower = (e.get("school") or "").lower()
            if not any(k in school_lower for k in self._UNT_KEYWORDS):
                continue

            degree_text = e.get("degree") or ""
            year = e.get("graduation_year") or ""

            score = 0
            score += degree_score(degree_text)
            if is_engineering(degree_text):
                score += 100

            # Prefer later year; still allow blank-degree UNT to win
            if year and year.isdigit():
                score += int(year)
            elif year:
                score += 25

            if score > best_score:
                best = e
                best_score = score

        return best

    # ============================================================
    # Visited history & CSV helpers
    # ============================================================

    def initialize_visited_history_from_db(self):
        logger.info("\n📊 Initializing visited history from database...")

        visited_profiles = get_all_visited_profiles()
        if not visited_profiles:
            logger.warning("⚠️ No visited profiles found in database")
            self.load_visited_history()
            return

        frequency_delta = parse_frequency(UPDATE_FREQUENCY)
        now = datetime.now()

        self.visited_history = {}
        for profile in visited_profiles:
            url = (profile.get('linkedin_url') or "").strip()
            if not url:
                continue

            is_unt = bool(profile.get('is_unt_alum'))
            last_checked = profile.get('last_checked')
            needs_update_db = bool(profile.get('needs_update'))

            update_needed = 'no'
            if needs_update_db:
                update_needed = 'yes'
            elif is_unt and last_checked:
                if isinstance(last_checked, str):
                    try:
                        last_checked_dt = datetime.fromisoformat(last_checked.replace('Z', '+00:00'))
                    except Exception:
                        last_checked_dt = now
                else:
                    last_checked_dt = last_checked

                if (now - last_checked_dt) > frequency_delta:
                    update_needed = 'yes'

            self.visited_history[url] = {
                'saved': 'yes' if is_unt else 'no',
                'visited_at': str(profile.get('visited_at') or ''),
                'update_needed': update_needed,
                'last_db_update': str(last_checked or '')
            }

        logger.info(f"✓ Loaded {len(self.visited_history)} visited profiles from database")
        self._save_visited_history()

    def _ensure_visited_history_headers(self):
        try:
            if VISITED_HISTORY_FILE.exists():
                df = pd.read_csv(VISITED_HISTORY_FILE)
                if list(df.columns) != VISITED_HISTORY_COLUMNS:
                    raise ValueError("Visited history CSV columns mismatch")
            else:
                pd.DataFrame(columns=VISITED_HISTORY_COLUMNS).to_csv(VISITED_HISTORY_FILE, index=False)
        except Exception:
            pd.DataFrame(columns=VISITED_HISTORY_COLUMNS).to_csv(VISITED_HISTORY_FILE, index=False)

    def load_visited_history(self):
        if VISITED_HISTORY_FILE.exists():
            try:
                df = pd.read_csv(VISITED_HISTORY_FILE)
                self.visited_history = {}
                for _, row in df.iterrows():
                    url = str(row.get('profile_url', '')).strip()
                    if not url:
                        continue
                    self.visited_history[url] = {
                        'saved': str(row.get('saved', 'no')).strip().lower(),
                        'visited_at': str(row.get('visited_at', '')).strip(),
                        'update_needed': str(row.get('update_needed', 'yes')).strip().lower(),
                        'last_db_update': str(row.get('last_db_update', '')).strip()
                    }
                logger.info(f"📜 Loaded {len(self.visited_history)} URLs from visited history")
            except Exception as e:
                logger.error(f"Error loading visited history: {e}")
                self.visited_history = {}
        else:
            self.visited_history = {}

    def _save_visited_history(self):
        try:
            rows = []
            for url, data in self.visited_history.items():
                rows.append({
                    'profile_url': url,
                    'saved': data.get('saved', 'no'),
                    'visited_at': data.get('visited_at', ''),
                    'update_needed': data.get('update_needed', 'yes'),
                    'last_db_update': data.get('last_db_update', '')
                })
            pd.DataFrame(rows).to_csv(VISITED_HISTORY_FILE, index=False)
        except Exception as e:
            logger.error(f"Error saving visited history: {e}")

    def mark_as_visited(self, url, saved=False, update_needed=False):
        if not url:
            return
        url = url.strip()

        save_visited_profile(url, is_unt_alum=bool(saved))

        self.visited_history[url] = {
            'saved': 'yes' if saved else 'no',
            'visited_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'update_needed': 'yes' if update_needed else 'no',
            'last_db_update': self.visited_history.get(url, {}).get('last_db_update', '')
        }
        self._save_visited_history()

    def should_skip_profile(self, url):
        if url not in self.visited_history:
            return False

        entry = self.visited_history[url]
        saved = entry.get('saved', 'no').lower()
        update_needed = entry.get('update_needed', 'no').lower()

        if saved == 'yes' and update_needed == 'yes':
            logger.info("    🔄 Re-visiting UNT alum (update needed)")
            return False

        if saved == 'yes':
            return True

        logger.debug("    ⊘ Skipping non-UNT profile (previously visited)")
        return True

    def ensure_csv_headers(self):
        try:
            if OUTPUT_CSV.exists():
                df = pd.read_csv(OUTPUT_CSV)
                if list(df.columns) != CSV_COLUMNS:
                    raise ValueError("CSV columns mismatch")
            else:
                pd.DataFrame(columns=CSV_COLUMNS).to_csv(OUTPUT_CSV, index=False)
        except Exception:
            pd.DataFrame(columns=CSV_COLUMNS).to_csv(OUTPUT_CSV, index=False)

    # ============================================================
    # Selenium setup + auth
    # ============================================================

    def setup_driver(self):
        logger.info("Setting up Chrome WebDriver...")
        chrome_options = webdriver.ChromeOptions()

        if HEADLESS:
            chrome_options.add_argument("--headless")

        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 15)
        logger.info("✓ WebDriver initialized")

    def load_cookies(self):
        try:
            if not COOKIES_FILE.exists():
                return False

            logger.info("Loading saved cookies...")
            self.driver.get("https://www.linkedin.com")
            time.sleep(2)

            cookies = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))

            for cookie in cookies:
                try:
                    if 'expiry' in cookie:
                        cookie['expiry'] = int(cookie['expiry'])
                    cookie.pop('sameSite', None)
                    self.driver.add_cookie(cookie)
                except Exception:
                    pass

            logger.info(f"✓ Loaded {len(cookies)} cookies")
            self.driver.get("https://www.linkedin.com/feed")
            time.sleep(3)
            return "feed" in (self.driver.current_url or "")
        except Exception as e:
            logger.warning(f"Error loading cookies: {e}")
            return False

    def save_cookies(self):
        try:
            cookies = self.driver.get_cookies()
            COOKIES_FILE.write_text(json.dumps(cookies, indent=2), encoding="utf-8")
            logger.info(f"✓ Saved {len(cookies)} cookies")
        except Exception as e:
            logger.error(f"Error saving cookies: {e}")

    def login(self):
        logger.info("Logging in to LinkedIn...")

        if USE_COOKIES and self.load_cookies():
            return True

        try:
            self.driver.get("https://www.linkedin.com/login")
            time.sleep(2)

            email_field = self.wait.until(EC.presence_of_element_located((By.ID, "username")))
            email_field.send_keys(LINKEDIN_EMAIL)
            time.sleep(random.uniform(0.5, 1))

            password_field = self.driver.find_element(By.ID, "password")
            password_field.send_keys(LINKEDIN_PASSWORD)
            time.sleep(random.uniform(0.5, 1))

            password_field.send_keys(Keys.RETURN)

            self.wait.until(EC.url_contains("feed"))
            time.sleep(3)

            logger.info("✓ Logged in successfully")
            self.save_cookies()
            return True
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    # ============================================================
    # Navigation helpers
    # ============================================================

    def scroll_full_page(self):
        logger.info("Scrolling down to load all profiles...")

        def get_scroll_container():
            for sel in [
                "div.scaffold-layout__content",
                "div.search-results-container",
                "main",
                "body",
            ]:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    if el:
                        return el
                except Exception:
                    continue
            return None

        container = get_scroll_container()

        for _ in range(12):
            try:
                if container:
                    self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", container)
                else:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                pass
            time.sleep(random.uniform(1.0, 2.0))

        logger.info("Scrolling back up...")
        try:
            if container:
                self.driver.execute_script("arguments[0].scrollTop = 0;", container)
            else:
                self.driver.execute_script("window.scrollTo(0, 0);")
        except Exception:
            pass
        time.sleep(1.0)

    def extract_profile_urls_from_page(self):
        logger.info("Extracting profile URLs…")

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        profile_urls = set()

        selectors = [
            "a.app-aware-link[href*='/in/']",
            "a[href*='/in/'][data-view-name='entity_result']",
            "a[href*='/in/'][aria-label]",
            "a[href*='/in/']:not([tabindex='-1'])"
        ]

        for selector in selectors:
            for a in soup.select(selector):
                url = a.get("href", "")
                if "/in/" in url:
                    url = url.split("?")[0]
                    if not url.startswith("http"):
                        url = "https://www.linkedin.com" + url
                    profile_urls.add(url)

        logger.info(f"Extracted {len(profile_urls)} profile URLs using updated selectors")
        return list(profile_urls)

    # ============================================================
    # Core scrape
    # ============================================================

    def scrape_profile_page(self, profile_url):
        """Open the full profile page and extract data. Returns None if UNT not found or blocked."""
        profile_data = {
            "name": "",
            "headline": "",
            "location": "",
            "job_title": "",
            "company": "",
            "job_start_date": "",
            "job_end_date": "",
            "education": "",
            "major": "",
            "school_start_date": "",
            "graduation_year": "",  # keep grad date var the same
            "working_while_studying": "",
            "profile_url": profile_url,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "all_education": []
        }

        try:
            logger.info(f"  Opening profile: {profile_url}")
            self.driver.get(profile_url)
            time.sleep(0.8)

            # Focus helps a lot when tabbed out
            self._force_focus()

            if PAGE_SETTLE_SECONDS > 0:
                time.sleep(PAGE_SETTLE_SECONDS)

            top_ready = self._wait_for_top_card(timeout=25)
            logger.info(f"🪪 top card ready: {top_ready}")

            # Old signal kept for logging/diagnostics (weak)
            loaded = self.wait_for_profile_sections_to_load(timeout=30)
            logger.info(f"🧩 profile sections loaded: {loaded}")

            edu_ready = self._wait_for_education_ready(timeout=EDU_READY_TIMEOUT_SECONDS)
            if not edu_ready:
                logger.warning(
                    "🚨🚨🚨 Education section did NOT become parse-ready within 30s. "
                    "Likely background throttling / LinkedIn hydration delay. "
                    "Try keeping Chrome focused, or set PAGE_SETTLE_SECONDS=5 and POST_SECTION_WAIT_SECONDS=5."
                )

            if POST_SECTION_WAIT_SECONDS > 0:
                time.sleep(POST_SECTION_WAIT_SECONDS)

            logger.info(f"🔎 current_url after get(): {self.driver.current_url}")
            logger.info(f"🔎 title: {self.driver.title}")

            self._save_debug_dump(profile_url, "00_after_waits")

            if self._page_looks_blocked():
                logger.warning("⚠️ Page looks blocked (checkpoint/authwall/empty shell). Skipping safely.")
                return None

            main_html = self._get_main_html()
            logger.info(f"🧪 main innerHTML length: {len(main_html)}")

            if len((main_html or "").strip()) < 2000:
                logger.warning("⚠️ Main DOM is empty/too small. Likely blocked or not rendered. Skipping safely.")
                return None

            soup = BeautifulSoup(main_html, "html.parser")

            # ===== Name / Headline / Location =====
            name, headline, location = self._extract_topcard_name_headline_location(soup)
            profile_data["name"] = name
            profile_data["headline"] = headline
            profile_data["location"] = location if location else "Not Found"

            # ===== Experience (latest, including date range) =====
            jt, co, job_start_d, job_end_d = self._extract_best_experience(soup)
            profile_data["job_title"] = jt
            profile_data["company"] = co
            profile_data["job_start_date"] = self._format_date_for_storage(job_start_d)
            profile_data["job_end_date"] = self._format_date_for_storage(job_end_d)

            # ===== Education =====
            edu_entries = self._extract_education_entries(soup)

            schools = [e["school"] for e in edu_entries if e.get("school")]
            profile_data["all_education"] = list(dict.fromkeys(schools))  # preserve order, unique

            best_unt = self._pick_best_unt_education(edu_entries)
            if best_unt:
                profile_data["education"] = best_unt.get("school", "")
                profile_data["major"] = best_unt.get("degree", "")
                profile_data["graduation_year"] = best_unt.get("graduation_year", "")

                school_start_d = best_unt.get("school_start")
                school_end_d = best_unt.get("school_end")
                profile_data["school_start_date"] = self._format_date_for_storage(school_start_d)

                # Determine overlap flag (only if we have date ranges for both)
                is_overlap = self._working_while_studying(
                    school_start=school_start_d,
                    school_end=school_end_d,
                    job_start=job_start_d,
                    job_end=job_end_d
                )
                profile_data["working_while_studying"] = "yes" if is_overlap else "no"
            else:
                logger.info("    ❌ No UNT education found in main profile. Trying expanded education page...")
                all_education_expanded, unt_details = self.scrape_all_education(profile_url)

                if all_education_expanded:
                    profile_data["all_education"] = list(dict.fromkeys(all_education_expanded))

                if unt_details:
                    profile_data["education"] = unt_details.get("education", "") or profile_data["education"]
                    profile_data["major"] = unt_details.get("major", "") or profile_data["major"]
                    profile_data["graduation_year"] = unt_details.get("graduation_year", "") or profile_data["graduation_year"]
                    profile_data["school_start_date"] = unt_details.get("school_start_date", "") or profile_data["school_start_date"]

                    # Overlap can only be computed if expanded page gave dates (it may not)
                    if unt_details.get("school_start") and unt_details.get("school_end"):
                        is_overlap = self._working_while_studying(
                            school_start=unt_details.get("school_start"),
                            school_end=unt_details.get("school_end"),
                            job_start=job_start_d,
                            job_end=job_end_d
                        )
                        profile_data["working_while_studying"] = "yes" if is_overlap else "no"
                    else:
                        profile_data["working_while_studying"] = ""
                else:
                    logger.info("    ❌ No UNT found after expanding. Skipping profile.")
                    return None

            logger.info(f"    ✓ Name: {profile_data['name']}")
            logger.info(f"    ✓ Headline: {profile_data['headline']}")
            logger.info(f"    ✓ Location: {profile_data['location']}")
            logger.info(f"    ✓ Job: {profile_data['job_title']} @ {profile_data['company']}")
            logger.info(f"    ✓ Job Dates: {profile_data['job_start_date']} - {profile_data['job_end_date']}")
            logger.info(
                f"    ✓ Education: {profile_data['education']} | Major: {profile_data['major']} | "
                f"Start: {profile_data['school_start_date']} | End: {profile_data['graduation_year']} | "
                f"Working While Studying: {profile_data['working_while_studying']}"
            )
            if len(profile_data.get('all_education', [])) > 1:
                logger.info(f"    ✓ All Education: {profile_data['all_education']}")

            return profile_data

        except Exception as e:
            logger.error(f"Error scraping profile {profile_url}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def scrape_all_education(self, profile_url):
        """
        Click 'Show all X educations' link and scrape ALL education entries.
        Returns tuple: (list of school names, dict of UNT details if found)
        """
        all_education = []
        unt_details = None

        education_keywords = (
            'university', 'college', 'institute', 'school', 'academy',
            'polytechnic', 'conservatory', 'seminary', 'of technology',
            'of science', 'of arts', 'of engineering', 'of business',
            'of medicine', 'of law', 'community college', 'state university',
            'technical college', 'vocational'
        )

        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            show_all_link = None
            for a in soup.find_all('a'):
                text = a.get_text(strip=True).lower()
                if 'show all' in text and 'education' in text:
                    show_all_link = a.get('href')
                    logger.info("    📚 Found 'Show all educations' link")
                    break

            if not show_all_link:
                return all_education, unt_details

            if not show_all_link.startswith('http'):
                show_all_link = f"https://www.linkedin.com{show_all_link}"

            logger.info("    📚 Opening full education page...")
            self.driver.get(show_all_link)
            time.sleep(3)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            main_content = soup.find('main') or soup

            for div in main_content.find_all("div"):
                lines = self._p_texts_excluding_skills_and_descriptions(div)
                if len(lines) < 2:
                    continue

                school = lines[0].strip()
                degree = lines[1].strip()
                if not school:
                    continue

                school_lower = school.lower()
                is_education = any(keyword in school_lower for keyword in education_keywords)
                has_degree_info = bool(re.search(r"(degree|bachelor|master|phd|mba|\bbs\b|\bms\b|\bba\b|\bma\b)", degree, re.I))

                if not is_education and not has_degree_info:
                    continue

                # Filter invalid degree text like "2023 - Jul 2024"
                if self._looks_like_invalid_degree_text(degree):
                    logger.warning(f"    ⚠️ No Major Detected. Filtered out invalid text: {degree}")
                    degree = ""

                all_education.append(school)

                if unt_details is None and any(k in school_lower for k in self._UNT_KEYWORDS):
                    unt_details = {
                        'education': school,
                        'major': degree,
                        'graduation_year': '',
                        'school_start_date': '',
                        'school_start': None,
                        'school_end': None
                    }

                    for t in lines[2:]:
                        s_d, e_d = self._parse_date_range_line(t)
                        if s_d and e_d:
                            unt_details['school_start'] = s_d
                            unt_details['school_end'] = e_d
                            unt_details['school_start_date'] = self._format_date_for_storage(s_d)
                            if not e_d.get("is_present") and e_d.get("year"):
                                unt_details['graduation_year'] = str(e_d.get("year"))
                            break

                        year_matches = re.findall(r"\d{4}", t)
                        if year_matches and not unt_details['graduation_year']:
                            unt_details['graduation_year'] = year_matches[-1]

            all_education = list(dict.fromkeys(all_education))
            logger.info(f"    📚 Scraped {len(all_education)} unique education entries")

            self.driver.get(profile_url)
            time.sleep(2)

        except Exception as e:
            logger.error(f"    Error scraping all education: {e}")
            import traceback
            traceback.print_exc()

        return all_education, unt_details

    # ============================================================
    # Save output
    # ============================================================

    def save_profile(self, profile_data):
        """Save a single profile to CSV."""
        try:
            if not profile_data.get('profile_url'):
                return False
            if not profile_data.get('name'):
                return False

            has_meaningful_data = any([
                profile_data.get('headline'),
                profile_data.get('location'),
                profile_data.get('job_title'),
                profile_data.get('education')
            ])
            if not has_meaningful_data:
                return False

            existing_df = pd.read_csv(OUTPUT_CSV) if OUTPUT_CSV.exists() else pd.DataFrame(columns=CSV_COLUMNS)

            save_data = {k: v for k, v in profile_data.items() if k in CSV_COLUMNS}
            save_data['job_title'] = clean_job_title(save_data.get('job_title', ''))

            for col in CSV_COLUMNS:
                save_data.setdefault(col, "")

            new_row = pd.DataFrame([save_data])[CSV_COLUMNS]
            combined_df = pd.concat([existing_df, new_row], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['profile_url'], keep='first')

            combined_df.to_csv(OUTPUT_CSV, index=False)
            return True
        except Exception as e:
            logger.error(f"❌ Error saving profile: {e}")
            import traceback
            traceback.print_exc()
            return False

    # ============================================================
    # Rate limiting
    # ============================================================

    def wait_between_profiles(self):
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        logger.info(f"\n⏳ Waiting {delay:.1f}s before next profile (avoiding detection)...\n")

        increment = delay / 10
        for i in range(10):
            time.sleep(increment)
            remaining = delay - (increment * (i + 1))
            if remaining > 0:
                logger.info(f"   ... {remaining:.0f}s remaining")

    # ============================================================
    # Modes
    # ============================================================

    def run_update_mode(self, outdated_profiles):
        profiles_updated = 0
        logger.info(f"Processing {len(outdated_profiles)} outdated profiles...\n")

        for idx, profile_info in enumerate(outdated_profiles, start=1):
            profile_url, first_name, last_name, last_updated = profile_info
            full_name = f"{first_name} {last_name}".strip()

            logger.info(f"\n{'='*60}")
            logger.info(f"PROFILE {idx}/{len(outdated_profiles)}: {full_name}")
            logger.info(f"Last updated: {last_updated}")
            logger.info(f"{'='*60}")

            if self.should_skip_profile(profile_url):
                logger.info("⊘ Profile not marked for update, skipping...")
                continue

            try:
                profile_data = self.scrape_profile_page(profile_url)
                if not profile_data:
                    logger.info("❌ No profile data returned, marking as visited")
                    self.mark_as_visited(profile_url, saved=False, update_needed=False)
                    continue

                profile_data['name'] = full_name

                if self.save_profile(profile_data):
                    profiles_updated += 1
                    logger.info(f"✅ Updated profile for {full_name}")
                    self.mark_as_visited(profile_url, saved=True, update_needed=False)
                else:
                    self.mark_as_visited(profile_url, saved=False, update_needed=False)

                if idx < len(outdated_profiles):
                    self.wait_between_profiles()

            except NoSuchWindowException:
                logger.error("Browser window closed, restarting driver...")
                self.setup_driver()
                if not self.login():
                    logger.error("Failed to login again after browser restart")
                    return
            except Exception as e:
                logger.error(f"Error updating profile: {e}")
                if idx < len(outdated_profiles):
                    self.wait_between_profiles()

        logger.info(f"\n{'='*60}")
        logger.info("Update Complete!")
        logger.info(f"Total profiles updated: {profiles_updated}/{len(outdated_profiles)}")
        logger.info(f"{'='*60}\n")

    def run(self):
        try:
            self.setup_driver()
            self.load_visited_history()
            self.initialize_visited_history_from_db()

            if not self.login():
                logger.error("Failed to login")
                return

            outdated_profiles, cutoff_date = get_outdated_profiles()

            logger.info(f"\n{'='*60}")
            logger.info("🔄 UPDATE CHECK")
            logger.info(f"{'='*60}")

            if outdated_profiles:
                logger.info(f"You have {len(outdated_profiles)} alumni records that were last updated")
                logger.info(f"over {UPDATE_FREQUENCY} ago (before {cutoff_date.strftime('%Y-%m-%d')})")
                logger.info(f"{'='*60}\n")

                response = input("Would you like to run the scraper to update their info now? (y/n): ").strip().lower()
                if response in ('y', 'yes'):
                    logger.info(f"\n🔄 Starting update of {len(outdated_profiles)} profiles...\n")
                    self.run_update_mode(outdated_profiles)
                    return
                logger.info("Skipping update. Running normal scraping mode...\n")
            else:
                logger.info(f"✅ All alumni records are up to date (last updated within {UPDATE_FREQUENCY})")
                logger.info(f"{'='*60}\n")

            if self.scraper_mode == "names":
                self.run_names_mode()
            elif self.scraper_mode == "search":
                self.run_search_mode()
            elif self.scraper_mode == "connections":
                self.run_connections_mode()
            else:
                logger.error(f"Invalid SCRAPER_MODE: {self.scraper_mode}. Use 'names', 'search', or 'connections'.")

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            try:
                if self.driver:
                    self.driver.quit()
                    logger.info("✓ WebDriver closed")
            except Exception:
                pass

    def run_names_mode(self):
        input_csv_path = os.getenv("INPUT_CSV", "backend/engineering_graduate.csv")
        full_input_path = Path(__file__).resolve().parent.parent / input_csv_path

        names = load_names_from_csv(full_input_path)
        if not names:
            logger.warning(f"No names found in {input_csv_path}; defaulting to search mode.")
            self.run_search_mode()
            return

        profiles_scraped = 0

        for name_idx, name in enumerate(names, start=1):
            logger.info(f"\n{'='*60}\nNAME {name_idx}/{len(names)}: {name}\n{'='*60}")

            q = urllib.parse.quote_plus(f'"{name}"')
            school_id = "6464"
            search_url = (
                f"https://www.linkedin.com/search/results/people/?"
                f"keywords={q}&schoolFilter=%5B%22{school_id}%22%5D&origin=FACETED_SEARCH"
            )

            self.driver.get(search_url)
            time.sleep(5)

            self.scroll_full_page()
            profile_urls = self.extract_profile_urls_from_page()
            if not profile_urls:
                logger.info(f"No profiles found for '{name}'.")
                continue

            limit = int(os.getenv("RESULTS_PER_SEARCH", "5") or 5)
            profile_urls = profile_urls[:limit]

            logger.info(f"\nProcessing {len(profile_urls)} profiles for '{name}'...\n")

            for idx, profile_url in enumerate(profile_urls, start=1):
                if self.should_skip_profile(profile_url):
                    logger.info(f"[{idx}/{len(profile_urls)}] ⊘ Already processed: {profile_url}")
                    continue

                logger.info(f"[{idx}/{len(profile_urls)}] Extracting profile: {profile_url}")

                profile_data = self.scrape_profile_page(profile_url)
                if not profile_data:
                    logger.info("❌ No profile data returned (blocked or no UNT). Not marking as visited here.")
                    continue

                profile_data['name'] = profile_data.get('name') or name

                if self.save_profile(profile_data):
                    profiles_scraped += 1
                    self.mark_as_visited(profile_url, saved=True, update_needed=False)
                else:
                    self.mark_as_visited(profile_url, saved=False, update_needed=False)

                if idx < len(profile_urls):
                    self.wait_between_profiles()

        logger.info(f"\nDone! Total profiles scraped: {profiles_scraped}\nSaved to: {OUTPUT_CSV}\n")

    def run_search_mode(self):
        base_search_url = "https://www.linkedin.com/search/results/people/?origin=FACETED_SEARCH&network=%5B%22O%22%5D&industry=%5B%221594%22%2C%226%22%2C%2296%22%2C%224%22%2C%22109%22%2C%22118%22%2C%22147%22%2C%22256%22%2C%22313%22%2C%2243%22%2C%22485%22%2C%22518%22%2C%2255%22%2C%2263%22%2C%2279%22%5D&schoolFilter=%5B%226464%22%5D"

        page = 1
        profiles_scraped = 0

        while True:
            logger.info(f"\n{'='*60}")
            logger.info(f"PAGE {page}")
            logger.info(f"{'='*60}\n")

            search_url = base_search_url if page == 1 else f"{base_search_url}&page={page}"

            logger.info(f"🔗 SEARCH URL: {search_url}")
            self.driver.get(search_url)
            time.sleep(5)

            self.scroll_full_page()
            profile_urls = self.extract_profile_urls_from_page()

            if not profile_urls:
                logger.info("No more profiles. Done!")
                break

            logger.info(f"\nProcessing {len(profile_urls)} profiles...\n")

            for idx, profile_url in enumerate(profile_urls):
                if self.should_skip_profile(profile_url):
                    logger.info(f"[{idx + 1}/{len(profile_urls)}] ⊘ Already processed")
                    continue

                logger.info(f"[{idx + 1}/{len(profile_urls)}] Scraping profile page...")

                profile_data = self.scrape_profile_page(profile_url)

                if not profile_data:
                    logger.info("❌ No profile data returned (blocked or no UNT). Not marking as visited here.")
                    if idx < len(profile_urls) - 1:
                        self.wait_between_profiles()
                    continue

                if self.save_profile(profile_data):
                    profiles_scraped += 1
                    self.mark_as_visited(profile_url, saved=True, update_needed=False)
                else:
                    self.mark_as_visited(profile_url, saved=False, update_needed=False)

                if idx < len(profile_urls) - 1:
                    self.wait_between_profiles()

            page += 1

        logger.info(f"\nScraping Complete! Total profiles scraped: {profiles_scraped}. Results saved to: {OUTPUT_CSV}\n")

    def run_connections_mode(self):
        connections_csv_path = Path(__file__).resolve().parent.parent / CONNECTIONS_CSV_PATH

        logger.info(f"\n{'='*60}")
        logger.info("CONNECTIONS MODE")
        logger.info(f"Reading from: {connections_csv_path}")
        logger.info(f"{'='*60}\n")

        try:
            df = pd.read_csv(connections_csv_path, skiprows=3)
        except Exception as e:
            logger.error(f"Failed to read connections CSV: {e}")
            return

        df = df.dropna(subset=['URL'])
        df = df[df['URL'].str.contains('linkedin.com/in/', na=False)]

        total_connections = len(df)
        logger.info(f"Found {total_connections} connections with valid LinkedIn URLs")

        profiles_saved = 0
        skipped_already_processed = 0
        skipped_not_saved = 0

        for idx, (_, row_data) in enumerate(df.iterrows(), start=1):
            profile_url = str(row_data.get('URL', '')).strip()
            if not profile_url:
                continue

            first_name = str(row_data.get('First Name', '') or '').strip()
            last_name = str(row_data.get('Last Name', '') or '').strip()
            full_name = f"{first_name} {last_name}".strip()

            logger.info(f"\n{'='*60}")
            logger.info(f"CONNECTION {idx}/{total_connections}: {full_name}")
            logger.info(f"URL: {profile_url}")
            logger.info(f"{'='*60}")

            if self.should_skip_profile(profile_url):
                logger.info("⊘ Already processed, skipping...")
                skipped_already_processed += 1
                continue

            profile_data = self.scrape_profile_page(profile_url)

            if not profile_data:
                logger.info("❌ Profile returned no valid data (blocked or no UNT). Not marking as visited here.")
                continue

            profile_data['name'] = profile_data.get('name') or full_name

            if self.save_profile(profile_data):
                profiles_saved += 1
                logger.info(f"✅ Saved: {full_name}")
                self.mark_as_visited(profile_url, saved=True, update_needed=False)
            else:
                skipped_not_saved += 1
                self.mark_as_visited(profile_url, saved=False, update_needed=False)

            if idx < total_connections:
                self.wait_between_profiles()

        logger.info(f"\n{'='*60}")
        logger.info("Connections Mode Complete!")
        logger.info(f"Total connections processed: {total_connections}")
        logger.info(f"Profiles saved: {profiles_saved}")
        logger.info(f"Skipped (already processed): {skipped_already_processed}")
        logger.info(f"Skipped (not saved): {skipped_not_saved}")
        logger.info(f"Results saved to: {OUTPUT_CSV}")
        logger.info(f"{'='*60}\n")


if __name__ == "__main__":
    scraper = LinkedInSearchScraper()
    scraper.run()