import time
import json
import random
import re
import urllib.parse
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException

# Local imports
import html

# Local imports
import scraper_utils as utils
import settings as config
from settings import logger, print_profile_summary
from entity_classifier import classify_entity, is_location, is_university, get_classifier
from groq_client import is_groq_available, verify_location, parse_groq_date, _clean_doubled
from groq_extractor_experience import extract_experiences_with_groq, strip_seniority_prefixes_from_title
from groq_extractor_education import extract_education_with_groq
from scraper_utils import determine_work_study_status

try:
    from job_title_normalization import normalize_title_deterministic, normalize_title_with_groq
    from company_normalization import normalize_company_deterministic, normalize_company_with_groq
    from degree_normalization import standardize_degree
    from major_normalization import standardize_major, standardize_major_list
    from discipline_classification import infer_discipline
    _NORM_AVAILABLE = True
except ImportError as err:
    logger.warning(f"Normalization modules missing: {err}")
    _NORM_AVAILABLE = False


def normalize_scraped_data(data):
    """
    Normalize all scraped data fields:
    - Strip leading/trailing whitespace from all string fields
    - Remove trailing slashes from URLs
    - Unescape HTML entities (e.g. & -> &)
    """
    if not data:
        return data
    
    for key, value in data.items():
        if isinstance(value, str):
            # Strip whitespace
            value = value.strip()
            # Remove trailing slashes from URLs
            if 'url' in key.lower() and value:
                value = value.rstrip('/')
            
            # Unescape HTML entities
            value = html.unescape(value)
            
            data[key] = value
        elif isinstance(value, list):
            # Handle list fields like all_education
            data[key] = [html.unescape(v.strip()) if isinstance(v, str) else v for v in value]
    
    return data


def _canonical_entity_text(text: str) -> str:
    """Normalize strings for lightweight title/company collision checks."""
    normalized = re.sub(r"[^a-z0-9]+", " ", (text or "").casefold())
    return re.sub(r"\s+", " ", normalized).strip()


def _is_company_title_collision(title: str, company: str) -> bool:
    """True when title and company resolve to the same label."""
    title_key = _canonical_entity_text(title)
    company_key = _canonical_entity_text(company)
    if not title_key or not company_key:
        return False
    return title_key == company_key


# LinkedIn company line: "Employer · Full-time" / "… · Internship"
_EMP_LINE_TAIL = re.compile(
    r"^(Full-time|Part-time|Contract|Internship|Seasonal|Temporary|Freelance|Self-employed|Apprenticeship|Intern)$",
    re.IGNORECASE,
)


_NAME_SUFFIX_TOKENS = {"ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x"}
_UNT_SCHOOL_ID = "6464"


def _normalize_person_name(raw_name: str) -> str:
    """
    Normalize person-name casing while preserving punctuation and nicknames.
    Examples:
    - "sanjana madharapu" -> "Sanjana Madharapu"
    - "ZOHREH FARAHMANDPOUR," -> "Zohreh Farahmandpour"
    - "KENNETH (\"KENNY\") WELLS" -> "Kenneth (\"Kenny\") Wells"
    """
    text = re.sub(r"\s+", " ", (raw_name or "").strip())
    if not text:
        return ""

    # Remove trailing punctuation noise like dangling commas.
    text = re.sub(r"[,\s]+$", "", text).strip()

    def _cap_word(match: re.Match) -> str:
        word = match.group(0)
        low = word.lower()
        if low in _NAME_SUFFIX_TOKENS:
            return low.upper()
        if len(word) == 1:
            return word.upper()
        return word[0].upper() + word[1:].lower()

    normalized = re.sub(r"[A-Za-z]+", _cap_word, text)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


_STANDARDIZED_TITLE_LOOKUP_CACHE = None


def _title_lookup_key(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).casefold()


def _load_standardized_title_lookup() -> dict[str, str]:
    """
    Load known standardized job titles from scraper/data/companies.json.
    Returns case-insensitive lookup: normalized text -> canonical display text.
    """
    global _STANDARDIZED_TITLE_LOOKUP_CACHE
    if _STANDARDIZED_TITLE_LOOKUP_CACHE is not None:
        return _STANDARDIZED_TITLE_LOOKUP_CACHE

    db_path = Path(__file__).parent / "data" / "companies.json"
    try:
        if not db_path.exists():
            _STANDARDIZED_TITLE_LOOKUP_CACHE = {}
            return _STANDARDIZED_TITLE_LOOKUP_CACHE

        payload = json.loads(db_path.read_text(encoding="utf-8"))
        raw_titles = payload.get("job_titles", []) if isinstance(payload, dict) else []
        lookup = {}
        for title in raw_titles:
            text = str(title or "").strip()
            if not text:
                continue
            key = _title_lookup_key(text)
            if key and key not in lookup:
                lookup[key] = text

        _STANDARDIZED_TITLE_LOOKUP_CACHE = lookup
        return _STANDARDIZED_TITLE_LOOKUP_CACHE
    except Exception as exc:
        logger.debug(f"Could not load standardized title lookup: {exc}")
        _STANDARDIZED_TITLE_LOOKUP_CACHE = {}
        return _STANDARDIZED_TITLE_LOOKUP_CACHE


def _resolve_standardized_title(raw_title: str, title_lookup: dict[str, str] | None = None) -> tuple[str, int]:
    """
    Resolve title with local rules:
    1) Deterministic normalization.
    2) Preserve exact lookup casing only when raw title is already canonical.
    3) Deterministic normalization mapped back into lookup.
    4) Deterministic normalization fallback.
    Returns: (standardized_title, quality_score)
    """
    raw = (raw_title or "").strip()
    if not raw:
        return "", 0

    lookup = title_lookup if title_lookup is not None else _load_standardized_title_lookup()
    raw_key = _title_lookup_key(raw)

    if _NORM_AVAILABLE:
        try:
            normalized = normalize_title_deterministic(raw) or raw
        except Exception:
            normalized = raw
    else:
        normalized = raw

    norm_key = _title_lookup_key(normalized)

    # Preserve exact lookup casing only when the raw title is already canonical.
    # This prevents raw seniority variants (e.g., "Jr. DevOps Engineer") from
    # bypassing deterministic normalization.
    if raw_key in lookup and raw_key == norm_key:
        return lookup[raw_key], 3

    if norm_key in lookup:
        return lookup[norm_key], 2

    return normalized, 1


class LinkedInScraper:
    """
    Core scraper class for navigating LinkedIn and extracting alumni data.
    
    Design Strategy:
    1. Persistent Sessions: Uses cookie-based login to avoid frequent auth hurdles.
    2. Resilience: Implements multiple fallbacks (Groq LLM -> CSS Selectors -> Top Card)
       to handle LinkedIn's frequent UI A/B testing and layout shifts.
    3. Performance: Uses headless mode and minimal waits where safe, but prioritizes
       human-like behavior (random delays, scrolling) to avoid rate limits.
    """
    def __init__(self):
        self.driver = None
        self.wait = None

    # ============================================================
    # Selenium Setup & Auth
    # ============================================================
    def setup_driver(self):
        logger.info("Setting up Chrome WebDriver...")
        chrome_options = webdriver.ChromeOptions()

        if config.HEADLESS:
            chrome_options.add_argument("--headless")

        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # Blocks some automated detection by disabling the 'navigator.webdriver' flag.
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        logger.info("✓ WebDriver initialized")

    def _load_cookies(self):
        try:
            if not config.COOKIES_FILE.exists():
                return False

            logger.info("Loading saved cookies...")
            self.driver.get("https://www.linkedin.com")
            time.sleep(2)

            cookies = json.loads(config.COOKIES_FILE.read_text(encoding="utf-8"))

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

    def _save_cookies(self):
        try:
            cookies = self.driver.get_cookies()
            config.COOKIES_FILE.write_text(json.dumps(cookies, indent=2), encoding="utf-8")
            logger.info(f"✓ Saved {len(cookies)} cookies")
        except Exception as e:
            logger.error(f"Error saving cookies: {e}")

    def login(self):
        logger.info("Logging in to LinkedIn...")

        if config.USE_COOKIES and self._load_cookies():
            return True

        try:
            self.driver.get("https://www.linkedin.com/login")
            time.sleep(2)

            email_field = self.wait.until(EC.presence_of_element_located((By.ID, "username")))
            email_field.send_keys(config.LINKEDIN_EMAIL)
            time.sleep(random.uniform(0.5, 1))

            password_field = self.driver.find_element(By.ID, "password")
            password_field.send_keys(config.LINKEDIN_PASSWORD)
            time.sleep(random.uniform(0.5, 1))

            password_field.send_keys(Keys.RETURN)

            self.wait.until(EC.url_contains("feed"))
            time.sleep(3)

            logger.info("✓ Logged in successfully")
            self._save_cookies()
            return True
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    def diagnose_login_state(self):
        """
        Diagnose current browser state after a failed login attempt.
        Returns a dict with fields: code, message, manual_intervention_required.
        """
        try:
            current_url = (self.driver.current_url or "").lower()
            title = (self.driver.title or "").lower()
            page_source = (self.driver.page_source or "").lower()
        except Exception:
            return {
                "code": "unknown_login_failure",
                "message": "Login failed and browser state could not be inspected.",
                "manual_intervention_required": True,
            }

        challenge_markers = [
            "checkpoint",
            "challenge",
            "verify your identity",
            "security verification",
            "two-step verification",
            "authwall",
            "captcha",
            "are you human",
        ]

        if any(marker in current_url for marker in ("checkpoint", "challenge", "authwall")) or any(
            marker in page_source or marker in title for marker in challenge_markers
        ):
            return {
                "code": "challenge_detected",
                "message": (
                    "LinkedIn challenge/checkpoint detected (2FA/security verification likely required)."
                ),
                "manual_intervention_required": True,
            }

        if "login" in current_url or "sign in" in title:
            return {
                "code": "invalid_credentials_or_login_block",
                "message": "Still on LinkedIn login page after submit (credentials or login block issue).",
                "manual_intervention_required": True,
            }

        return {
            "code": "unknown_login_failure",
            "message": "Login failed for an unknown reason.",
            "manual_intervention_required": True,
        }

    def quit(self):
        if self.driver:
            self.driver.quit()
            logger.info("✓ WebDriver closed")

    # ============================================================
    # Navigation & Waits
    # ============================================================
    def _scroll_active_surfaces(self, delta):
        self.driver.execute_script(
            """
            const delta = Number(arguments[0] || 0);
            const scrollables = Array.from(document.querySelectorAll('main, section, div, ul')).filter((el) => {
                const style = window.getComputedStyle(el);
                const overflowY = (style.overflowY || '').toLowerCase();
                const canScroll = ['auto', 'scroll', 'overlay'].includes(overflowY);
                return canScroll && (el.scrollHeight - el.clientHeight) > 200;
            }).sort((left, right) => (
                (right.scrollHeight - right.clientHeight) - (left.scrollHeight - left.clientHeight)
            ));

            window.scrollBy(0, delta);
            for (const el of scrollables.slice(0, 6)) {
                const maxTop = Math.max(0, el.scrollHeight - el.clientHeight);
                const nextTop = Math.max(0, Math.min(maxTop, el.scrollTop + delta));
                if (Math.abs(nextTop - el.scrollTop) > 0) {
                    el.scrollTop = nextTop;
                }
            }
            return scrollables.length;
            """,
            int(delta),
        )

    def _scroll_surfaces_to_edge(self, edge):
        self.driver.execute_script(
            """
            const edge = (arguments[0] || 'top').toLowerCase();
            const toTop = edge === 'top';
            const scrollables = Array.from(document.querySelectorAll('main, section, div, ul')).filter((el) => {
                const style = window.getComputedStyle(el);
                const overflowY = (style.overflowY || '').toLowerCase();
                const canScroll = ['auto', 'scroll', 'overlay'].includes(overflowY);
                return canScroll && (el.scrollHeight - el.clientHeight) > 200;
            });

            window.scrollTo(0, toTop ? 0 : Math.max(document.body.scrollHeight, document.documentElement.scrollHeight));
            for (const el of scrollables.slice(0, 6)) {
                el.scrollTop = toTop ? 0 : Math.max(0, el.scrollHeight - el.clientHeight);
            }
            return scrollables.length;
            """,
            edge,
        )

    def scroll_full_page(self):
        """
        Scroll the full LinkedIn page down and back up.
        This is critical for LinkedIn because many profile sections (like Education
        and Experience) are lazy-loaded and only appear in the DOM when scrolled into view.
        """
        logger.debug("Scrolling page...")
        try:
            for _ in range(5):
                self._scroll_active_surfaces(900)
                time.sleep(random.uniform(0.5, 0.9))

            self._scroll_surfaces_to_edge("bottom")
            time.sleep(random.uniform(0.8, 1.2))

            for _ in range(2):
                self._scroll_active_surfaces(-1200)
                time.sleep(random.uniform(0.4, 0.7))

            self._scroll_surfaces_to_edge("top")
            time.sleep(0.5)
        except Exception:
            pass

    def extract_profile_urls_from_page(self):
        logger.debug("Extracting profile URLs...")
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        profile_urls = []
        seen = set()

        # Keep DOM order so the scraper processes links as they appear on the page.
        for anchor in soup.select("a[href*='/in/']"):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue

            # Canonicalize to the public profile slug and drop tracking params.
            # This avoids queue churn from miniProfile and query-variant links.
            match = re.search(r"/in/([^/?#]+)", href)
            if not match:
                continue
            url = f"https://www.linkedin.com/in/{match.group(1).strip()}".rstrip("/")

            if url in seen:
                continue
            seen.add(url)
            profile_urls.append(url)

        return profile_urls

    def _force_focus(self):
        try:
            self.driver.switch_to.window(self.driver.current_window_handle)
            self.driver.execute_script("window.focus();")
        except Exception:
            pass

    def _wait_for_top_card(self, timeout=10):
        """Wait for the Name to appear (h1/h2)."""
        end = time.time() + timeout
        while time.time() < end:
            try:
                ok = self.driver.execute_script("""
                    const main = document.querySelector('main');
                    if (!main) return false;

                    const h1 = main.querySelector('h1');
                    if (!h1) return false;

                    const text = (h1.innerText || '').trim().toLowerCase();
                    if (text.length < 2) return false;
                    if (text.includes('notification')) return false;
                    return true;
                """)
                if ok: return True
            except Exception: pass
            time.sleep(0.5)
        return False

    @staticmethod
    def _looks_like_profile_url(url):
        text = (url or "").strip().lower()
        if not text:
            return False
        return "/linkedin.com/in/" in text or "/www.linkedin.com/in/" in text

    def _wait_for_education_ready(self, timeout=15):
        """Wait for the Education section to become available."""
        end = time.time() + timeout
        while time.time() < end:
            try:
                # Scroll a bit if not found yet to trigger render
                self._scroll_active_surfaces(300)
                
                ok = self.driver.execute_script("""
                    const m = document.querySelector('main') || document.body;
                    const headings = Array.from(m.querySelectorAll('h2,h3,span')); 
                    // Added 'span' because sometimes headers are inside spans now
                    
                    const h = headings.find(x => (x.innerText || '').trim().toLowerCase().includes('education'));
                    if (!h && !m.querySelector('a[href*="/school/"]')) return false;
                    
                    return true;
                """)
                if ok: return True
            except Exception: pass
            time.sleep(0.5)
        return False

    def _page_looks_blocked(self):
        return self._page_block_reason() is not None

    def _page_block_reason(self):
        try:
            url = (self.driver.current_url or "").lower()
            title = (self.driver.title or "").lower()
            html = (self.driver.page_source or "").lower()
            
            if any(x in url for x in ("checkpoint", "authwall", "challenge")):
                return "checkpoint_or_authwall_detected"
            if any(x in title for x in ("sign in", "security verification")):
                return "signin_or_security_title_detected"
            overlay_markers = (
                "blurred-overlay",
                "blurred-content",
                "blurred-list",
                "sign-in-modal",
                "public_profile_sign-in-modal",
                "public_profile_v3_desktop-public_profile_sign-in-modal",
            )
            if sum(1 for marker in overlay_markers if marker in html) >= 2:
                return "blurred_signin_overlay_detected"
            if len(html.strip()) < 1000:
                return "page_too_small"
            return None
        except Exception:
            return "page_health_check_failed"

    def _page_not_found(self):
        """Detect LinkedIn's 'This page doesn't exist' error page."""
        try:
            html = (self.driver.page_source or "").lower()
            markers = [
                "this page doesn't exist",
                "this page doesn\u2019t exist",
                "page not found",
                "please check your url or return to linkedin home",
            ]
            return any(m in html for m in markers)
        except Exception:
            return False

    # ============================================================
    # Core Scraping Logic
    # ============================================================
    @staticmethod
    def _education_entries_exceed_cloud_limits(entries, max_len=255):
        if not entries:
            return False
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            school = str(entry.get("school") or "")
            degree = str(entry.get("degree_raw") or entry.get("raw_degree") or "")
            major = str(entry.get("major_raw") or "")
            if len(school) > max_len or len(degree) > max_len or len(major) > max_len:
                return True
        return False

    def _initialize_profile_data(self, profile_url):
        """Return the normalized data envelope used for every profile scrape."""
        return {
            "name": "", "headline": "", "location": "",
            "job_title": "", "company": "", "job_employment_type": "",
            "job_start_date": "", "job_end_date": "",
            "education": "", "major": "", "school_start_date": "", "graduation_year": "",
            "working_while_studying": "",
            "profile_url": profile_url,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "all_education": []
        }

    def scrape_profile_page(self, profile_url):
        data = self._initialize_profile_data(profile_url)

        try:
            logger.debug(f"Opening profile: {profile_url}")
            self.driver.get(profile_url)
            self._force_focus()
            
            # Initial settle
            time.sleep(2)

            current_url = (self.driver.current_url or "").strip()
            if not self._looks_like_profile_url(current_url):
                # Occasionally LinkedIn redirects to feed/notifications even after a profile GET.
                # Retry once before giving up so we don't scrape unrelated pages.
                logger.warning(
                    "Navigation diverted away from profile (%s). Retrying target URL once.",
                    current_url or "unknown",
                )
                self.driver.get(profile_url)
                self._force_focus()
                time.sleep(2)
                current_url = (self.driver.current_url or "").strip()
                if not self._looks_like_profile_url(current_url):
                    logger.warning("Could not reach profile page after retry: %s", profile_url)
                    return None

            # Check if blocked
            block_reason = self._page_block_reason()
            if block_reason:
                if block_reason == "blurred_signin_overlay_detected":
                    logger.warning(
                        "⚠️ LinkedIn likely detected automation or requires sign-in. "
                        "Waiting for human input; restart the scraper if you want to continue."
                    )
                    return "MANUAL_INTERVENTION_REQUIRED"
                logger.warning("⚠️ Page looks blocked or empty.")
                return None

            # Check if page/profile no longer exists
            if self._page_not_found():
                logger.warning(f"⚠️ PAGE NOT FOUND: {profile_url}")
                return "PAGE_NOT_FOUND"

            # Capture canonical URL (LinkedIn may redirect vanity → generated or vice versa).
            # We track redirects to ensure that we don't treat identical profiles
            # with different URL formats as separate entities in our database.
            canonical_url = self.driver.current_url.split("?")[0].rstrip("/")
            if canonical_url != profile_url.rstrip("/"):
                logger.info(f"URL redirect: {profile_url} → {canonical_url}")
                data["profile_url"] = canonical_url
                data["_original_url"] = profile_url  # Keep original for history tracking

            # 1. Trigger full-page lazy loading using the same scroll routine on every OS.
            self.scroll_full_page()

            if not self._wait_for_top_card(timeout=10):
                logger.debug("Top card name not detected quickly for %s", canonical_url or profile_url)

            # 2. Wait for Education specifically
            found_edu = self._wait_for_education_ready(timeout=10)
            if not found_edu:
                logger.debug("Education section not detected quickly (might be missing or different layout).")

            soup = BeautifulSoup(self.driver.execute_script("return document.body.innerHTML;"), "html.parser")

            # 3. Top Card
            name, headline, location = self._extract_top_card(soup)
            if not name:
                name = self._fallback_name_from_profile_source(soup, canonical_url or profile_url)

            # JS-based location fallback: if BeautifulSoup couldn't find the
            # location (common on Windows due to DOM/class differences), try
            # extracting directly from the live browser DOM.
            if not location:
                js_location = self._extract_location_via_js()
                if js_location:
                    logger.debug("Location found via JS fallback: %s", js_location)
                    location = js_location

            data["name"] = name
            data["headline"] = headline
            data["location"] = location or "Not Found"

            # 4. Experience - Get up to 3 entries
            all_experiences = self._extract_all_experiences(soup, max_entries=3, profile_name=name)
            _total_tokens = getattr(self, '_last_exp_tokens', 0)  # From Groq experience extraction
            self._apply_experience_entries(data, all_experiences)

            # 5. Education — Try Groq first, CSS fallback.
            # We prioritize Groq for education because it can intelligently separate
            # degree from major and parse inconsistent date formats that CSS alone 
            # often misses or mangles.
            edu_entries = []

            # Prefer expanded education page (latest entries) when available.
            detailed_entries, detail_tokens = self._extract_education_entries_from_detailed_view(profile_url, soup)
            _total_tokens += detail_tokens
            if detailed_entries:
                edu_entries = self._merge_education_entries(edu_entries, detailed_entries)

            if is_groq_available():
                edu_root = self._find_section_root(soup, "Education")
                if edu_root:
                    edu_html = str(edu_root)
                    groq_results, edu_tokens = extract_education_with_groq(
                        edu_html,
                        profile_name=data.get("name", "unknown"),
                    )
                    if groq_results and self._education_entries_exceed_cloud_limits(groq_results):
                        logger.warning(
                            "Groq education output exceeded cloud field limits for %s. Retrying once in strict mode.",
                            data.get("name", "unknown"),
                        )
                        strict_results, strict_tokens = extract_education_with_groq(
                            edu_html,
                            profile_name=data.get("name", "unknown"),
                            strict_mode=True,
                        )
                        edu_tokens += strict_tokens
                        if strict_results:
                            groq_results = strict_results
                    _total_tokens += edu_tokens
                    if groq_results:
                        logger.debug(f"Using Groq education results ({len(groq_results)} entries)")
                        edu_entries = self._merge_education_entries(
                            self._build_education_entries_from_groq(groq_results),
                            edu_entries,
                        )

            elif not is_groq_available():
                css_entries = self._extract_education_entries(soup)
                if css_entries:
                    edu_entries = self._merge_education_entries(edu_entries, css_entries)
            
            if not self._has_unt_education(edu_entries):
                top_card_entries = self._extract_education_from_top_card(soup)
                if top_card_entries:
                    edu_entries = self._merge_education_entries(edu_entries, top_card_entries)

            edu_entries = self._sort_education_entries(edu_entries)

            # Filter out entries where Groq mistakenly used activities text as a school name
            # (activities text contains "UNT" which falsely passes _is_unt_school_name)
            edu_entries = [
                e for e in edu_entries
                if not re.match(r'^\s*Activities and societies:', e.get("school", ""), re.IGNORECASE)
            ]

            data["all_education"] = list(dict.fromkeys([e["school"] for e in edu_entries if e.get("school")]))

            # --- Pick best UNT education as primary entry ---
            best_unt = self._pick_best_unt_education(edu_entries)
            
            if best_unt:
                data["education"] = best_unt.get("school", "")
                data["school"] = best_unt.get("school", "")
                data["degree"] = best_unt.get("degree", "").strip()
                data["major"] = best_unt.get("major", "").strip()
                # Fallback: if CSS gave combined degree without major, keep degree as-is
                if not data["major"] and data["degree"]:
                    data["major"] = data["degree"]
                data["graduation_year"] = best_unt.get("graduation_year", "")
                data["raw_degree"] = best_unt.get("raw_degree", "")
                
                school_start_d = best_unt.get("school_start")
                school_end_d = best_unt.get("school_end")
                data["school_start_date"] = utils.format_date_for_storage(school_start_d)
                data["school_end_date"] = utils.format_date_for_storage(school_end_d)
                if not data["graduation_year"] and school_end_d and school_end_d.get("year") and school_end_d.get("year") != 9999:
                    data["graduation_year"] = str(school_end_d.get("year"))
                if data.get("school_start_date") or data.get("school_end_date"):
                    data["school_dates"] = f"{data.get('school_start_date', '')} - {data.get('school_end_date', '')}".strip(" -")
                else:
                    data["school_dates"] = ""

                # Determine working_while_studying using graduation-year comparison.
                # Priority order: "currently" > "yes" > "no" > ""
                wws_priority = {"": 0, "no": 1, "yes": 2, "currently": 3}
                is_expected = bool(school_end_d and school_end_d.get("is_expected"))

                best_wws = ""
                for exp in all_experiences:
                    job_start_d = exp.get("start")
                    job_end_d = exp.get("end")
                    status = determine_work_study_status(
                        school_end_d, job_start_d, job_end_d, is_expected=is_expected
                    )
                    if wws_priority.get(status, 0) > wws_priority.get(best_wws, 0):
                        best_wws = status
                        if best_wws == "currently":
                            break  # Can't get higher

                best_wws = self._apply_missing_dates_unt_ga_fallback(
                    best_wws=best_wws,
                    all_experiences=all_experiences,
                    edu_entries=edu_entries,
                )

                data["working_while_studying"] = best_wws
            else:
                # If we have NO education entries, or just no UNT, try expanding
                if not edu_entries or not self._has_unt_education(edu_entries):
                    logger.debug("No UNT education found in main profile. Expanding...")
                    expanded_edus, unt_details = self.scrape_all_education(profile_url)
                    
                    if expanded_edus:
                        data["all_education"] = list(dict.fromkeys(expanded_edus))
                    
                    if unt_details:
                        data["education"] = unt_details.get("education", "")
                        data["school"] = unt_details.get("education", "")
                        data["degree"] = unt_details.get("degree", "")
                        data["major"] = unt_details.get("major", "")
                        data["graduation_year"] = unt_details.get("graduation_year", "")
                        data["school_start_date"] = unt_details.get("school_start_date", "")
                        
                        if unt_details.get("school_end"):
                            school_end_exp = unt_details["school_end"]
                            is_expected = bool(school_end_exp.get("is_expected"))

                            wws_priority = {"": 0, "no": 1, "yes": 2, "currently": 3}
                            best_wws = ""
                            for exp in all_experiences:
                                status = determine_work_study_status(
                                    school_end_exp, exp.get("start"), exp.get("end"),
                                    is_expected=is_expected
                                )
                                if wws_priority.get(status, 0) > wws_priority.get(best_wws, 0):
                                    best_wws = status
                                    if best_wws == "currently":
                                        break

                            fallback_edu_entries = list(edu_entries)
                            if unt_details.get("education"):
                                fallback_edu_entries.append({"school": unt_details.get("education", "")})
                            best_wws = self._apply_missing_dates_unt_ga_fallback(
                                best_wws=best_wws,
                                all_experiences=all_experiences,
                                edu_entries=fallback_edu_entries,
                            )

                            data["working_while_studying"] = best_wws
                    else:
                        # This pipeline intentionally stores UNT alumni only.
                        # If neither inline extraction nor expanded education view finds UNT,
                        # skip persisting this profile.
                        return {
                            "__status__": "NOT_UNT_ALUM",
                            "profile_url": data.get("profile_url", profile_url),
                            "_original_url": data.get("_original_url", ""),
                            "name": data.get("name", ""),
                        }

            # --- Store up to 3 education entries (school2/degree2/major2, etc.) ---
            # Trust the extraction layer (Groq/CSS) to return clean entries.
            # Only exclude the primary entry itself.
            primary_entry = best_unt if best_unt else None
            other_entries = [
                e for e in edu_entries
                if e is not primary_entry
                and not re.match(r'^\s*Activities and societies:', e.get("school", ""), re.IGNORECASE)
            ]
            for i, entry in enumerate(other_entries[:2], start=2):
                data[f"school{i}"] = entry.get("school", "")
                data[f"degree{i}"] = entry.get("degree", "").strip()
                data[f"major{i}"] = entry.get("major", "").strip()
                start_label = utils.format_date_for_storage(entry.get("school_start"))
                end_label = utils.format_date_for_storage(entry.get("school_end"))
                if (not end_label) and entry.get("graduation_year"):
                    end_label = str(entry.get("graduation_year"))
                if start_label or end_label:
                    data[f"school{i}_dates"] = f"{start_label} - {end_label}".strip(" -")
                else:
                    data[f"school{i}_dates"] = ""

            self._apply_education_and_discipline_normalization(data)
            self._apply_experience_display_normalization(data)

            # --- Clean summary block ---
            print_profile_summary(data, token_count=_total_tokens, status="Saved")
            self._log_missing_data_warnings(data, all_experiences, edu_entries)

            # Normalize all fields before returning
            return normalize_scraped_data(data)

        except Exception as e:
            logger.error(f"Error scraping profile {profile_url}: {e}")
            return None

    def _apply_experience_entries(self, data, all_experiences):
        """
        Populate primary + secondary experience slots.
        Keep this isolated so scrape_profile_page stays readable during handoffs.
        """
        if all_experiences:
            best_exp = all_experiences[0]
            data["job_title"] = strip_seniority_prefixes_from_title(best_exp["title"])
            data["company"] = best_exp["company"]
            data["job_employment_type"] = (best_exp.get("employment_type") or "").strip()
            data["job_start_date"] = utils.format_date_for_storage(best_exp["start"])
            data["job_end_date"] = utils.format_date_for_storage(best_exp["end"])
        else:
            data["job_title"] = ""
            data["company"] = ""
            data["job_employment_type"] = ""
            data["job_start_date"] = ""
            data["job_end_date"] = ""

        for i, exp in enumerate(all_experiences[1:3], start=2):
            data[f"exp{i}_title"] = strip_seniority_prefixes_from_title(exp["title"])
            data[f"exp{i}_company"] = exp["company"]
            data[f"exp{i}_employment_type"] = (exp.get("employment_type") or "").strip()
            data[f"exp{i}_dates"] = (
                f"{utils.format_date_for_storage(exp['start'])} - "
                f"{utils.format_date_for_storage(exp['end'])}"
            )

        for i in range(2, 4):
            if f"exp{i}_title" not in data:
                data[f"exp{i}_title"] = ""
                data[f"exp{i}_company"] = ""
                data[f"exp{i}_employment_type"] = ""
                data[f"exp{i}_dates"] = ""

    @staticmethod
    def _append_standardization_log(filename, raw, standardized):
        if raw and standardized and raw != standardized:
            try:
                with open(f"scraper/output/{filename}", "a", encoding="utf-8") as f:
                    f.write(f"{raw} -> {standardized}\n")
            except Exception:
                pass

    def _apply_education_and_discipline_normalization(self, data):
        """
        Apply degree/major normalization and infer discipline once education is set.
        """
        try:
            for suffix in ("", "2", "3"):
                deg_key = f"degree{suffix}" if suffix else "degree"
                maj_key = f"major{suffix}" if suffix else "major"
                std_deg_key = f"standardized_degree{suffix}" if suffix else "standardized_degree"
                std_maj_key = f"standardized_major{suffix}" if suffix else "standardized_major"

                raw_deg = data.get(deg_key, "")
                raw_maj = data.get(maj_key, "")

                # Some profiles hide degree tokens in major text (e.g. "B.S. Computer Science").
                if not raw_deg and raw_maj:
                    from degree_normalization import extract_hidden_degree
                    extracted_deg, cleaned_maj = extract_hidden_degree(raw_maj)
                    if extracted_deg:
                        raw_deg = extracted_deg
                        raw_maj = cleaned_maj
                        logger.info(f"✓ Extracted hidden degree '{extracted_deg}' from major field.")

                if raw_deg:
                    std_deg = standardize_degree(raw_deg)
                    data[std_deg_key] = std_deg
                    self._append_standardization_log("standardized_degree.txt", raw_deg, std_deg)

                if raw_maj:
                    majors = standardize_major_list(raw_maj, data.get("job_title", ""))
                    data[std_maj_key] = majors[0]
                    if suffix == "" and len(majors) > 1:
                        data["standardized_major_alt"] = majors[1]
                    self._append_standardization_log("standardized_major.txt", raw_maj, data[std_maj_key])

            education_entries = []
            for suffix in ("", "2", "3"):
                school_key = f"school{suffix}" if suffix else "school"
                deg_key = f"degree{suffix}" if suffix else "degree"
                maj_key = f"major{suffix}" if suffix else "major"
                std_deg_key = f"standardized_degree{suffix}" if suffix else "standardized_degree"
                std_maj_key = f"standardized_major{suffix}" if suffix else "standardized_major"

                school = data.get(school_key, "")
                degree = data.get(deg_key, "")
                major = data.get(maj_key, "")
                standardized_degree = data.get(std_deg_key, "")
                standardized_major = data.get(std_maj_key, "")

                if not any([school, degree, major, standardized_degree, standardized_major]):
                    continue

                entry = {
                    "school": school,
                    "degree": degree,
                    "major": major,
                    "standardized_degree": standardized_degree,
                    "standardized_major": standardized_major,
                }
                if suffix == "":
                    alt = data.get("standardized_major_alt", "")
                    if alt:
                        entry["standardized_major_alt"] = alt
                education_entries.append(entry)

            data["discipline"] = infer_discipline(
                f"{data.get('degree', '')} {data.get('major', '')}",
                data.get("job_title", ""),
                data.get("headline", ""),
                use_llm=True,
                education_entries=education_entries,
                older_job_titles=[data.get("exp2_title", ""), data.get("exp3_title", "")],
            )

            if data.get("discipline") and data.get("discipline") != "Unknown":
                with open("scraper/output/inferred_disciplines.txt", "a", encoding="utf-8") as f:
                    f.write(
                        f"{data.get('name')} | {data.get('degree')} | "
                        f"{data.get('job_title')} -> {data['discipline']}\n"
                    )
        except Exception as norm_err:
            logger.debug(f"    ⚠️ Education normalization/discipline failed: {norm_err}")

    def _apply_experience_display_normalization(self, data):
        """Populate normalized experience fields used by summary output and downstream UI."""
        if not _NORM_AVAILABLE:
            return

        title_lookup = _load_standardized_title_lookup()
        best_title_by_entry = {}
        slot_details = []

        for idx, suffix in enumerate(["", "2", "3"], start=1):
            title_key = "job_title" if not suffix else f"exp{idx}_title"
            comp_key = "company" if not suffix else f"exp{idx}_company"
            raw_title = data.get(title_key, "")
            raw_comp = data.get(comp_key, "")

            if not suffix:
                start_date = (data.get("job_start_date") or "").strip()
                end_date = (data.get("job_end_date") or "").strip()
            else:
                date_range = (data.get(f"exp{idx}_dates") or "").strip()
                if " - " in date_range:
                    start_date, end_date = [part.strip() for part in date_range.split(" - ", 1)]
                else:
                    start_date, end_date = "", ""

            entry_key = ""
            if raw_comp:
                entry_key = f"{_canonical_entity_text(raw_comp)}|{_title_lookup_key(start_date)}|{_title_lookup_key(end_date)}"

            resolved_title, quality = _resolve_standardized_title(raw_title, title_lookup) if raw_title else ("", 0)
            slot_details.append((idx, suffix, entry_key, resolved_title, quality))

            if entry_key and resolved_title:
                current = best_title_by_entry.get(entry_key)
                if current is None or quality > current[1]:
                    best_title_by_entry[entry_key] = (resolved_title, quality)

            if raw_comp:
                data[f"normalized_company" if not suffix else f"normalized_exp{idx}_company"] = (
                    normalize_company_deterministic(raw_comp)
                )

        for idx, suffix, entry_key, resolved_title, _quality in slot_details:
            title_value = resolved_title
            if entry_key and entry_key in best_title_by_entry:
                title_value = best_title_by_entry[entry_key][0]
            if title_value:
                data[f"normalized_job_title" if not suffix else f"normalized_exp{idx}_title"] = title_value

    @staticmethod
    def _drop_title_company_collisions(experiences, source="unknown"):
        """Remove experiences where title and company collapse to the same text."""
        sanitized = []
        for exp in experiences or []:
            title = (exp.get("title") or "").strip()
            company = (exp.get("company") or "").strip()
            if _is_company_title_collision(title, company):
                logger.debug(f"Skipping {source} experience with title/company collision: {title} @ {company}")
                continue
            sanitized.append(exp)
        return sanitized

    @staticmethod
    def _looks_like_company_noise(raw_company: str) -> bool:
        """
        Guardrail for experience-company extraction.
        Reject pure location strings and long narrative blobs that are not employers.
        """
        company = " ".join((raw_company or "").split()).strip()
        if not company:
            return True

        if is_location(company):
            return True

        lowered = company.casefold()
        if lowered in {"remote", "hybrid", "on-site", "onsite"}:
            return True

        if len(company) > 140:
            return True

        if re.search(
            r"\b(goal\s+of\s+this\s+project|gained\s+extensive\s+experience|known\s+for|"
            r"hard\s+work|responsible\s+for|worked\s+on|enhancement|carbon\s+footprint)\b",
            lowered,
            re.IGNORECASE,
        ):
            return True

        token_count = len(re.findall(r"[a-z0-9]+", lowered))
        has_company_signal = bool(
            re.search(
                r"\b(inc|llc|ltd|corp|company|co\.?|technologies|technology|systems|solutions|"
                r"group|partners|health|hospital|university|college|school|institute|"
                r"laboratory|lab|bureau|foundation|association)\b",
                lowered,
                re.IGNORECASE,
            )
        )
        if token_count >= 14 and not has_company_signal:
            return True

        return False

    @staticmethod
    def _log_missing_data_warnings(data, all_experiences, edu_entries):
        if not all_experiences:
            logger.warning(f"No experience found for {data.get('name', 'Unknown')}")
        if not edu_entries:
            logger.warning(f"No education found for {data.get('name', 'Unknown')}")

    # ============================================================
    # Missing-date fallback (strict UNT + Graduate Assistant)
    # ============================================================
    _UNT_FULL_NAME_RE = re.compile(r'university\s+of\s+north\s+texas', re.IGNORECASE)
    _UNT_TOKEN_RE = re.compile(r'\bunt\b', re.IGNORECASE)
    _SCHOOL_ID_RE = re.compile(r"/school/(\d+)", re.IGNORECASE)

    def _is_unt_school_name(self, name: str) -> bool:
        if not name:
            return False
        return bool(self._UNT_FULL_NAME_RE.search(name) or self._UNT_TOKEN_RE.search(name))

    def _school_id_from_href(self, href: str) -> str:
        match = self._SCHOOL_ID_RE.search(href or "")
        return match.group(1) if match else ""

    def _is_unt_school_href(self, href: str) -> bool:
        return self._school_id_from_href(href) == _UNT_SCHOOL_ID

    def _is_unt_employer(self, raw_company: str) -> bool:
        if not raw_company or not raw_company.strip():
            return False

        company = " ".join(raw_company.split())
        if self._UNT_FULL_NAME_RE.search(company):
            return True
        if re.search(r'^\s*unt\s+', company, re.IGNORECASE):
            return True
        return bool(self._UNT_TOKEN_RE.search(company))

    def _has_unt_education(self, edu_entries) -> bool:
        if not edu_entries:
            return False
        for entry in edu_entries:
            school_name = (entry or {}).get("school") or (entry or {}).get("education") or ""
            if self._is_unt_school_name(school_name):
                return True
        return False

    def _get_standardized_title(self, exp: dict) -> str:
        standardized_title = (exp.get("standardized_title") or exp.get("normalized_title") or "").strip()
        if standardized_title:
            return standardized_title

        if not _NORM_AVAILABLE:
            return ""

        raw_title = (exp.get("raw_title") or exp.get("title") or "").strip()
        if not raw_title:
            return ""

        try:
            return normalize_title_deterministic(raw_title) or ""
        except Exception:
            return ""

    def _has_unt_graduate_assistant_experience(self, all_experiences) -> bool:
        if not all_experiences:
            return False

        for exp in all_experiences:
            standardized_title = self._get_standardized_title(exp)
            if standardized_title != "Graduate Assistant":
                continue

            raw_company = exp.get("raw_company")
            if raw_company is None:
                raw_company = exp.get("company")
            if not raw_company or not str(raw_company).strip():
                continue

            if self._is_unt_employer(str(raw_company)):
                return True

        return False

    def _apply_missing_dates_unt_ga_fallback(self, best_wws: str, all_experiences, edu_entries) -> str:
        """
        Preserve all computable date-based outcomes.
        Only when status is unknown ("") do we apply a strict fallback:
        UNT education + Graduate Assistant (standardized) + UNT raw employer.
        """
        if best_wws != "":
            return best_wws

        if not self._has_unt_education(edu_entries):
            return "no"

        if self._has_unt_graduate_assistant_experience(all_experiences):
            logger.debug("Missing-date fallback: UNT Graduate Assistant role detected")
            return "yes"

        return "no"

    # ============================================================
    # Parsing Methods
    # ============================================================
    @staticmethod
    def _looks_like_person_name(raw_text):
        text = re.sub(r"\s+", " ", (raw_text or "").strip())
        if len(text) < 2 or len(text) > 80:
            return False
        lowered = text.lower()
        if re.fullmatch(r"\d+\s+notifications?", lowered):
            return False
        banned_tokens = (
            "notification",
            "notifications",
            "messages",
            "my network",
            "linkedin",
            "jobs",
            "feed",
            "search",
        )
        if any(token in lowered for token in banned_tokens):
            return False
        return bool(re.search(r"[a-z]", lowered))

    @staticmethod
    def _name_from_profile_url(profile_url):
        text = (profile_url or "").strip()
        if not text:
            return ""
        match = re.search(r"/in/([^/?#]+)", text, flags=re.IGNORECASE)
        if not match:
            return ""
        slug = urllib.parse.unquote(match.group(1) or "")
        slug = slug.replace("-", " ").replace("_", " ")
        tokens = [token for token in slug.split() if token]
        if not tokens:
            return ""
        cleaned_tokens = [
            token for token in tokens
            if not re.fullmatch(r"[0-9]{3,}[a-z]?", token.lower())
        ]
        if not cleaned_tokens:
            cleaned_tokens = tokens
        candidate = _normalize_person_name(" ".join(cleaned_tokens[:4]))
        if not candidate:
            return ""
        return candidate

    def _fallback_name_from_profile_source(self, soup, profile_url):
        if soup is not None:
            meta = soup.find("meta", attrs={"property": "og:title"})
            content = (meta.get("content") if meta else "") or ""
            if content:
                candidate = content.split("|", 1)[0].strip()
                if " - " in candidate:
                    candidate = candidate.split(" - ", 1)[0].strip()
                if self._looks_like_person_name(candidate):
                    return _normalize_person_name(candidate)

            title_tag = soup.find("title")
            title_text = title_tag.get_text(" ", strip=True) if title_tag else ""
            if title_text:
                candidate = title_text.split("|", 1)[0].strip()
                if " - " in candidate:
                    candidate = candidate.split(" - ", 1)[0].strip()
                if self._looks_like_person_name(candidate):
                    return _normalize_person_name(candidate)

        candidate = self._name_from_profile_url(profile_url)
        if self._looks_like_person_name(candidate):
            return candidate
        return ""

    def _extract_location_via_js(self):
        """Extract location text directly from the browser DOM using JavaScript.

        LinkedIn's profile location sits in the top-card section, typically as
        a ``<span>`` with class ``text-body-small inline t-black--light``.
        This method tries several CSS selector strategies so it survives
        LinkedIn's frequent A/B tests and class renames.
        """
        try:
            result = self.driver.execute_script(r"""
                const main = document.querySelector('main') || document.body;

                function normalizeLocationText(text) {
                    let t = (text || '').replace(/\s+/g, ' ').trim();
                    if (!t) return '';
                    // LinkedIn often renders location and contact on one row.
                    t = t.replace(/\s*[·|]\s*contact info\s*$/i, '').trim();
                    return t;
                }

                function looksLikeLocation(text) {
                    const t = normalizeLocationText(text);
                    if (!t || t.length < 3 || t.length > 120) return false;
                    const tl = t.toLowerCase();
                    if (tl.includes('connection') || tl.includes('follower') || tl.includes('contact info')) return false;
                    if (tl.includes('full-time') || tl.includes('part-time') || tl.includes('contract') || tl.includes('internship')) return false;
                    if (tl.includes('present') || tl.includes('university') || tl.includes('college') || tl.includes('school') || tl.includes('institute')) return false;
                    if (tl.includes('company') || tl.includes('llc') || tl.includes('inc') || tl.includes('corp')) return false;

                    // Common LinkedIn location shape: City, State/Region, Country.
                    if (t.includes(',')) return true;

                    // Handle non-comma locations such as "Remote".
                    if (/^(remote|hybrid|on-site|onsite)$/i.test(t)) return true;

                    // Accept common LinkedIn regional shapes without commas.
                    if (/(metropolitan area|metro area|metroplex|bay area|\bmetro\b|\bregion\b|\bcounty\b)/i.test(t)) return true;
                    if (/^greater\s+[a-z]/i.test(t)) return true;

                    // Accept well-known metro/regional names without commas.
                    if (/\b(silicon valley|tri-state|inland empire|puget sound|research triangle|twin cities|chicagoland|hampton roads|south florida)\b/i.test(t)) return true;
                    // Accept "North Texas", "Central Florida", etc.
                    if (/^(north|south|east|west|central|northern|southern|eastern|western)\s+[a-z]/i.test(t) && t.split(/\s+/).length <= 5) return true;

                    return false;
                }

                // --- Strategy 1: Specific LinkedIn location class combo ---
                const locSpan = main.querySelector(
                    'span.text-body-small.inline.t-black--light.break-words'
                );
                if (locSpan) {
                    const t = normalizeLocationText(locSpan.innerText || '');
                    if (looksLikeLocation(t)) return t;
                }

                const locDiv = main.querySelector(
                    'div.text-body-small.inline.t-black--light.break-words, div.text-body-small.t-black--light.break-words'
                );
                if (locDiv) {
                    const t = normalizeLocationText(locDiv.innerText || '');
                    if (looksLikeLocation(t)) return t;
                }

                // --- Strategy 2: Find the top-card section and look for
                //     the first text-body-small span that isn't a badge ---
                const topCards = main.querySelectorAll(
                    'section.artdeco-card, .pv-top-card, .ph5, .mt2'
                );
                for (const card of topCards) {
                    const nodes = card.querySelectorAll('span.text-body-small, div.text-body-small, span.t-black--light, div.t-black--light');
                    for (const node of nodes) {
                        // Skip if inside an inline-show-more-text (badge)
                        if (node.closest('.inline-show-more-text')) continue;
                        const t = normalizeLocationText(node.innerText || '');
                        if (looksLikeLocation(t)) return t;
                    }
                }

                // --- Strategy 3: Position-based: find span.text-body-small
                //     nearest to / right after the h1 name heading ---
                const h1 = main.querySelector('h1');
                if (h1) {
                    const h1Rect = h1.getBoundingClientRect();
                    let best = null;
                    let bestDist = 9999;
                    const allSmall = main.querySelectorAll('span.text-body-small, div.text-body-small, span.t-black--light, div.t-black--light');
                    for (const node of allSmall) {
                        if (node.closest('.inline-show-more-text')) continue;
                        const t = normalizeLocationText(node.innerText || '');
                        if (!looksLikeLocation(t)) continue;
                        const r = node.getBoundingClientRect();
                        // Must be below the name heading
                        if (r.top < h1Rect.bottom) continue;
                        const dist = r.top - h1Rect.bottom;
                        if (dist < bestDist) {
                            bestDist = dist;
                            best = t;
                        }
                    }
                    if (best && bestDist < 300) return best;
                }

                // --- Strategy 4: Use Contact info row adjacency ---
                // On some layouts, location appears as plain text directly before
                // the Contact info link and lacks the expected text-body-small span.
                const contactAnchors = Array.from(main.querySelectorAll('a, span, div'))
                    .filter(el => /contact info/i.test((el.innerText || '').trim()));
                for (const anchor of contactAnchors) {
                    let current = anchor;
                    for (let i = 0; i < 4 && current; i++) {
                        const prev = current.previousElementSibling;
                        if (prev) {
                            const t = normalizeLocationText(prev.innerText || prev.textContent || '');
                            if (looksLikeLocation(t)) return t;
                        }
                        current = current.parentElement;
                    }
                }

                return '';
            """)
            return (result or "").strip()
        except Exception as e:
            logger.debug("JS location extraction failed: %s", e)
            return ""

    def _extract_top_card(self, soup):
        name, headline, location = "", "", ""
        raw_location = ""
        source_root = soup.find("main") or soup

        def _has_region_location_keywords(value):
            text = (value or "").lower()
            return any(keyword in text for keyword in [
                "metropolitan area",
                "metro area",
                "metroplex",
                "bay area",
                " metro",
                "greater ",
                " region",
                " county",
                "silicon valley",
                "tri-state",
                "inland empire",
                "puget sound",
                "research triangle",
                "twin cities",
                "chicagoland",
                "hampton roads",
                "south florida",
            ])
        
        # Name - prefer H1/H2 inside <main>, ignore global navigation headings.
        for tag_name in ("h1", "h2"):
            if name:
                break
            for tag in source_root.find_all(tag_name):
                candidate = tag.get_text(" ", strip=True)
                candidate = re.sub(r"\s*\(.*?\)\s*$", "", candidate).strip()
                if not self._looks_like_person_name(candidate):
                    continue
                name = candidate
                break

        # Headline - Look for 'text-body-medium' class (LinkedIn's current pattern)
        for div in source_root.find_all("div", class_=lambda x: x and "text-body-medium" in x):
            text = div.get_text(" ", strip=True)
            if text and len(text) > 5 and len(text) < 200:
                # Skip if it looks like a date or connection badge
                if not re.search(r'^\d{4}|^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', text):
                    headline = text
                    break
        
        # Fallback: Look for headline in data-generated-suggestion-target attribute area
        if not headline:
            for div in source_root.find_all("div", {"data-generated-suggestion-target": True}):
                text = div.get_text(" ", strip=True)
                if text and len(text) > 5:
                    headline = text
                    break
        
        # Location - Must have comma (City, State) OR specific location keywords
        # Filter out school/company badges based on class patterns:
        # - Badges have parent div with class "inline-show-more-text"
        # - Real location has class "text-body-small inline t-black--light"
        location_blacklist = ["university", "college", "school", "institute", 
                              "inc", "corp", "llc", "company", "technologies", "solutions",
                              "enterprises", "consulting", "software", "systems", "group"]
        
        # Initialize classifier before the loop so it's available in fallback code below
        classifier = get_classifier()

        for span in source_root.find_all(["span", "div"], class_=lambda x: x and "text-body-small" in x):
            # Check if this is inside a badge container (inline-show-more-text div)
            parent_div = span.find_parent("div")
            if parent_div:
                parent_class = " ".join(parent_div.get("class", []))
                # Skip if it's in a badge container
                if "inline-show-more-text" in parent_class:
                    continue
            
            # Get span's own class to prefer "inline t-black--light" pattern
            span_class = " ".join(span.get("class", []))
            is_location_styled = "inline" in span_class and "t-black--light" in span_class
            
            text = span.get_text(" ", strip=True)
            text = re.sub(r"\s*[·|]\s*Contact info\s*$", "", text, flags=re.IGNORECASE).strip()
            text_lower = text.lower()
            
            # Skip badge-like entries (schools, companies)
            if any(x in text_lower for x in location_blacklist):
                continue
            
            # Skip connection/follower/contact and employment-type text
            if any(x in text_lower for x in ["connection", "follower", "contact info", "full-time", "part-time", "contract", "internship"]):
                continue
            
            # Valid location patterns:
            # 1. Contains comma (City, State or City, Country format)
            # 2. Contains specific location keywords like "metroplex", "area"
            # 3. Contains country names
            has_comma = "," in text
            has_location_keyword = _has_region_location_keywords(text)
            has_country = any(x in text_lower for x in [
                "united states", "india", "canada", "uk", "united kingdom",
                "germany", "australia", "france", "japan", "china", "brazil", "mexico",
                "saudi arabia", "uae", "united arab emirates", "qatar", "kuwait",
                "bahrain", "oman", "jordan", "egypt", "turkey", "pakistan",
                "bangladesh", "sri lanka", "nepal", "malaysia", "singapore",
                "indonesia", "philippines", "vietnam", "thailand", "south korea",
                "nigeria", "kenya", "ghana", "south africa", "ethiopia",
                "italy", "spain", "netherlands", "belgium", "switzerland",
                "sweden", "norway", "denmark", "finland", "poland",
                "ireland", "new zealand", "portugal", "greece",
            ])

            if not raw_location and (is_location_styled or has_comma or has_location_keyword or has_country):
                raw_location = text
            
            if classifier.is_location(text):
                location = text
                break
            
            # If not obvious from heuristics, use Groq if available
            if (is_location_styled or has_comma or has_country) and is_groq_available():
                # Potential location, verify with Groq
                if verify_location(text):
                    location = text
                    break
                if not raw_location:
                    raw_location = text
            elif not raw_location and (has_comma or has_location_keyword or has_country):
                raw_location = text

        # Contact-info adjacency fallback for top-card layouts where location is not
        # rendered inside the expected text-body-small span.
        if not location:
            contact_nodes = source_root.find_all(
                ["a", "span", "div"],
                string=re.compile(r"contact info", re.IGNORECASE),
            )
            for node in contact_nodes:
                for prev in node.find_all_previous(["span", "div"], limit=6):
                    candidate = prev.get_text(" ", strip=True)
                    candidate = re.sub(r"\s*[·|]\s*Contact info\s*$", "", candidate, flags=re.IGNORECASE).strip()
                    if not candidate:
                        continue
                    if any(token in candidate.lower() for token in [
                        "connection", "follower", "company", "full-time", "part-time", "contract", "internship"
                    ]):
                        continue
                    if classifier.is_location(candidate):
                        location = candidate
                        break
                    if (
                        ("," in candidate or _has_region_location_keywords(candidate))
                        and not any(token in candidate.lower() for token in ["university", "college", "school", "institute"])
                    ):
                        location = candidate
                        break
                if location:
                    break

        # Final-pass: accept raw_location if it looks like a location based on
        # geo keywords, even if the classifier rejected it (classifier may be
        # missing a pattern but the raw_location was captured for a reason).
        if not location and raw_location:
            # Accept raw_location if it has commas, region keywords, or recognized country names
            rl_lower = raw_location.lower()
            geo_accept = (
                "," in raw_location
                or _has_region_location_keywords(raw_location)
                or any(c in rl_lower for c in [
                    "united states", "india", "canada", "remote",
                    "united kingdom", "germany", "australia", "france",
                    "saudi arabia", "uae", "japan", "china", "brazil", "mexico",
                ])
            )
            if geo_accept:
                location = raw_location
                logger.debug(f"Accepted raw_location as final fallback: {location}")
            else:
                # Last resort: try Groq verification on the raw_location
                if is_groq_available():
                    if verify_location(raw_location):
                        location = raw_location
                        logger.debug(f"Groq verified raw_location: {location}")
                else:
                    location = raw_location
        name = _normalize_person_name(name)
        if not self._looks_like_person_name(name):
            name = ""
        return name, headline, location

    def _extract_all_experiences(self, soup, max_entries=3, profile_name="unknown"):
        """
        Extract up to max_entries experience entries, sorted by most recent first.
        
        Uses LinkedIn's CSS structure directly:
        - Job title: in span with class containing 't-bold'
        - Company: in span with class 't-14 t-normal' (format: "Company · Part-time")
        - Dates: in span with class 'pvs-entity__caption-wrapper' or 't-black--light'
        """
        exp_root = self._find_section_root(soup, "Experience")
        if not exp_root: 
            logger.debug("No Experience section found")
            self._last_exp_tokens = 0
            return []

        parsed = []
        seen_entries = set()
        
        # ============================================================
        # APPROACH 0: Groq AI extraction (most accurate)
        # ============================================================
        if is_groq_available():
            try:
                # Get the raw HTML of the experience section
                experience_html = str(exp_root)
                
                # Call Groq to extract jobs
                groq_jobs, exp_tokens = extract_experiences_with_groq(experience_html, max_jobs=max_entries, profile_name=profile_name)
                self._last_exp_tokens = exp_tokens
                
                if groq_jobs:
                    for job in groq_jobs:
                        start_d = parse_groq_date(job.get("start_date", ""))
                        end_d = parse_groq_date(job.get("end_date", ""))
                        
                        title = (job.get("job_title") or "").strip()
                        company = (job.get("company") or "").strip()

                        if self._looks_like_company_noise(company):
                            logger.debug(f"Skipping Groq experience with invalid company text: {title} @ {company}")
                            continue

                        if _is_company_title_collision(title, company):
                            logger.debug(f"Skipping Groq experience with title/company collision: {title} @ {company}")
                            continue
                        
                        if (title or company) and start_d and end_d:
                            u_key = f"{title.lower()}|{company.lower()}|{start_d}|{end_d}"
                            if u_key not in seen_entries:
                                parsed.append({
                                    "title": title,
                                    "company": company,
                                    "employment_type": (job.get("employment_type") or "").strip(),
                                    "start": start_d,
                                    "end": end_d,
                                })
                                seen_entries.add(u_key)
                    
                    parsed = self._drop_title_company_collisions(parsed, source="Groq")
                    if parsed:
                        logger.debug(f"Groq extracted {len(parsed)} experience(s)")
                        return parsed[:max_entries]
                        
            except Exception as e:
                logger.warning(f"Groq extraction failed: {e}")
            return parsed[:max_entries]
        
        # ============================================================
        # APPROACH 1: Direct CSS selector extraction (LinkedIn's structure)
        # ============================================================
        # Find experience entry containers - they have data-view-name="profile-component-entity"
        # or are within an anchor that links to experience details
        experience_containers = exp_root.select('div[data-view-name="profile-component-entity"]')
        
        # Also try finding by the link pattern (experience entries usually have links)
        if not experience_containers:
            experience_containers = exp_root.select('a[data-field="experience_company_logo"]')
            # Get parent containers
            experience_containers = [a.find_parent('div') for a in experience_containers if a.find_parent('div')]
        
        for container in experience_containers:
            title = ""
            company = ""
            employment_type_css = ""
            start_d = None
            end_d = None
            
            # Job Title: Look for t-bold class
            # Prefer inner span to avoid doubled text from parent div
            title_elem = container.select_one('.t-bold span[aria-hidden="true"]') or container.select_one('.t-bold')
            if title_elem:
                title = _clean_doubled(title_elem.get_text(strip=True))
            
            # Company + Employment Type: Look for t-14 t-normal (not t-black--light)
            company_spans = container.select('span.t-14.t-normal:not(.t-black--light)')
            for span in company_spans:
                text_elem = span.select_one('span[aria-hidden="true"]')
                if text_elem:
                    text = text_elem.get_text(strip=True)
                else:
                    text = span.get_text(strip=True)
                
                # This could be "Company · Part-time" / "… · Internship" format
                if text and not utils.DATE_RANGE_RE.search(text):
                    emp_css = ""
                    if "·" in text:
                        segs = [s.strip() for s in text.split("·")]
                        if len(segs) >= 2 and _EMP_LINE_TAIL.match(segs[-1]):
                            emp_css = segs[-1]
                    candidate_company = _clean_doubled(text.strip())
                    from entity_classifier import is_location
                    if is_location(candidate_company):
                        parent_ul = container.find_parent('ul')
                        if parent_ul:
                            outer_container = parent_ul.find_parent('div', attrs={'data-view-name': 'profile-component-entity'})
                            if outer_container:
                                outer_title_elem = outer_container.select_one('.t-bold span[aria-hidden="true"]') or outer_container.select_one('.t-bold')
                                if outer_title_elem:
                                    candidate_company = _clean_doubled(outer_title_elem.get_text(strip=True).strip())
                                    emp_css = ""
                    if self._looks_like_company_noise(candidate_company):
                        continue
                    company = candidate_company
                    employment_type_css = emp_css
                    break
            
            # Dates: Look for pvs-entity__caption-wrapper or t-black--light
            date_spans = container.select('span.pvs-entity__caption-wrapper[aria-hidden="true"], span.t-black--light span[aria-hidden="true"]')
            for span in date_spans:
                text = span.get_text(strip=True)
                if utils.DATE_RANGE_RE.search(text):
                    start_d, end_d = utils.parse_date_range_line(text)
                    if start_d and end_d:
                        break
            
            # Entity classification cross-check: detect title/company swaps
            # If the extracted "title" looks like a company and "company" looks
            # like a job title, swap them before saving.
            if title and company:
                from entity_classifier import classify_entity as _classify
                t_type, t_conf = _classify(title)
                c_type, c_conf = _classify(company)
                if t_type == "company" and c_type == "job_title" and t_conf >= 0.5 and c_conf >= 0.5:
                    logger.debug(f"CSS extraction swap detected: '{title}' (classified company) ↔ '{company}' (classified title)")
                    title, company = company, title
                elif t_type == "company" and c_type not in ("job_title",) and t_conf >= 0.8:
                    # Title is strongly classified as company but company isn't classified as title;
                    # Still swap if confidence is high enough
                    logger.debug(f"CSS extraction likely swap: '{title}' is strongly classified as company")
                    title, company = company, title

            # Validate and add
            if (title or company) and start_d and end_d:
                u_key = f"{(title or '').lower()}|{(company or '').lower()}|{start_d}|{end_d}"
                if u_key not in seen_entries:
                    # Log partial extractions for debugging
                    if title and not company:
                        logger.debug(f"Found job title '{title}' but no company detected")
                    elif company and not title:
                        logger.debug(f"Found company '{company}' but no job title detected")
                    
                    parsed.append({
                        "title": title or "",
                        "company": company or "",
                        "employment_type": (employment_type_css or "").strip(),
                        "start": start_d,
                        "end": end_d,
                    })
                    seen_entries.add(u_key)
        
        # ============================================================
        # APPROACH 2: Fallback to text-based extraction if Approach 1 failed
        # ============================================================
        if not parsed:
            logger.debug("Direct CSS extraction found nothing, trying text-based fallback...")
            parsed = self._extract_experiences_text_based(exp_root, max_entries, seen_entries)

        parsed = self._drop_title_company_collisions(parsed, source="Fallback")
        
        # Sort by end date descending (most recent first)
        def experience_sort_key(exp):
            end = exp.get("end", {})
            if end.get("is_present"):
                return (9999, 12)  # Present jobs sort first
            year = end.get("year", 0) or 0
            month = end.get("month", 1) or 1
            return (year, month)
        
        parsed.sort(key=experience_sort_key, reverse=True)
        
        # Log summary
        if parsed:
            logger.debug(f"Extracted {len(parsed)} experience(s) via CSS fallback")
        
        return parsed[:max_entries]
    
    def _extract_experiences_text_based(self, exp_root, max_entries=3, seen_entries=None):
        """
        Fallback text-based experience extraction (original approach).
        Used when CSS selector approach fails.
        """
        if seen_entries is None:
            seen_entries = set()
            
        # Patterns to filter OUT (these are not company/title)
        junk_patterns = re.compile(
            r'^(Full-time|Part-time|Contract|Internship|Freelance|Self-employed|Seasonal|Temporary|Remote|Hybrid|On-site)$|'
            r'^\d+\s*(yr|yrs|year|years|mo|mos|month|months)\b|'
            r'^·\s*\d+\s*(yr|yrs|mo|mos)|'
            r'^\d+\s*(yr|yrs)?\s*\d*\s*(mo|mos)?$',
            re.I
        )
        
        # Company indicator patterns
        company_hints = re.compile(
            r'\b(Inc\.?|Corp\.?|LLC|Ltd\.?|Company|Co\.?|Technologies|Solutions|Enterprises|Group|Partners|Services|Consulting|Software|Systems|S\.?R\.?L\.?)(?=\W|$)',
            re.I
        )
        
        # Job title indicator patterns  
        title_hints = re.compile(
            r'\b(Engineer|Developer|Manager|Director|Analyst|Designer|Consultant|Specialist|Associate|Intern|Lead|Senior|Junior|Sr\.?|Jr\.?|Chief|Head|VP|Vice President|Coordinator|Administrator|Representative|Officer|Architect|Scientist|Drafter|Assistant|Fellow|Co-op|Researcher|Student Researcher|Research Assistant|Teaching Assistant)\\b',
            re.I
        )

        context_company = ""
        parsed = []
        
        divs = exp_root.find_all("div", recursive=True)
        
        for div in divs:
            lines = self._p_texts_clean(div)
            if not lines: continue
            
            has_date_range = any(utils.DATE_RANGE_RE.search(t) for t in lines)
            has_duration = any(re.match(r'^\d+\s*(yr|yrs|mo|mos)', t, re.I) for t in lines)
            
            # Company Header detection
            if has_duration and not has_date_range:
                for t in lines:
                    clean_t = t.strip()
                    if not clean_t or junk_patterns.match(clean_t): continue
                    
                    ent_type, conf = classify_entity(clean_t)
                    if ent_type == "company" and conf >= 0.7:
                        context_company = clean_t
                        break
                    # Also accept universities as employers
                    if is_university(clean_t):
                        context_company = clean_t
                        break
                continue
            
            # Experience Entry detection
            if has_date_range:
                date_idx = next((i for i, t in enumerate(lines) if utils.DATE_RANGE_RE.search(t)), None)
                if date_idx is None: continue
                
                start_d, end_d = utils.parse_date_range_line(lines[date_idx])
                if not (start_d and end_d): continue
                
                text_window = lines[max(0, date_idx - 4):date_idx]
                if not text_window: continue
                
                company = ""
                title = ""
                
                classified_items = []
                for t in text_window:
                    clean_t = self._clean_context_line(t)
                    if not clean_t: continue
                    
                    parts = self._split_context_line(clean_t)
                    
                    for part in parts:
                        if is_location(part): continue
                        if is_university(part):
                            # Universities can be employers!
                            classified_items.append((part, "company", 0.85))
                        else:
                            e_type, conf = classify_entity(part)
                            classified_items.append((part, e_type, conf))

                classified_items.sort(key=lambda x: -x[2])
                
                final_candidates = []
                for text, cat, conf in classified_items:
                    if title_hints.search(text):
                        final_candidates.append((text, "job_title", 1.0))
                    elif company_hints.search(text):
                        final_candidates.append((text, "company", 1.0))
                    else:
                        final_candidates.append((text, cat, conf))
                
                final_candidates.sort(key=lambda x: -x[2])

                for item_text, item_type, conf in final_candidates:
                    if item_type == "company" and not company:
                        company = item_text
                    elif item_type == "job_title" and not title:
                        title = item_text
                    elif item_type == "unknown":
                        if not title and title_hints.search(item_text):
                            title = item_text
                        elif not company and company_hints.search(item_text):
                            company = item_text
                            
                # Context propagation
                if title and not company and context_company:
                    if context_company.lower() != title.lower():
                        company = context_company

                if company:
                    context_company = company

                # Clean doubled text in fallback path
                if title:
                    title = _clean_doubled(title)
                if company:
                    company = _clean_doubled(company)
                    if self._looks_like_company_noise(company):
                        company = ""

                if title or company:
                    u_key = f"{(title or '').lower()}|{(company or '').lower()}|{start_d}|{end_d}"
                    if u_key not in seen_entries:
                        # Log partial extractions
                        if title and not company:
                            logger.info(f"    ⚠️ [Fallback] Found job title '{title}' but no company")
                        elif company and not title:
                            logger.info(f"    ⚠️ [Fallback] Found company '{company}' but no job title")
                        
                        parsed.append({
                            "title": title or "",
                            "company": company or "",
                            "employment_type": "",
                            "start": start_d,
                            "end": end_d,
                        })
                        seen_entries.add(u_key)

        return parsed[:max_entries]

    def _clean_context_line(self, t):
        # Filtering helper
        t = t.strip()
        if not t: return None
        if re.match(r'^(Full-time|Part-time|Contract|Internship)', t, re.I): return None
        if t in ['·', '•', '·', '•']: return None
        # Remove suffix like "· Full-time" (including mojibake bullet variants)
        return re.sub(r'\s*(?:·|•|·|•)\s*(Full-time|Part-time|Contract|Internship|Remote|Hybrid).*$', '', t, flags=re.I).strip()

    def _split_context_line(self, text):
        potential_parts = []
        
        # Split by various delimiters: 
        # - " at " / " @ "
        # - " | " (pipe)
        # - " - " / " – " (dash/en-dash with spaces)
        # - "·" / "•" (dot/bullet)
        
        # Regex for delimiters
        # We require spaces around dashes to avoid splitting "Co-Founder" or "Tier-1"
        # We allow flexible spacing for pipes and dots
        delimiters = r'\s+(?:at|@)\s+|\s*\|\s*|\s*(?:-|\u2013|\u2014|–|—)\s*|\s*(?:·|•|·|•)\s*'
        
        parts = re.split(delimiters, text, flags=re.I)
        potential_parts = [p.strip() for p in parts if len(p.strip()) > 1] # Allowed >1 to catch "QA" or "HR"
        
        if not potential_parts:
            potential_parts.append(text)
        return potential_parts

    def _extract_best_experience(self, soup):
        """Backwards-compatible wrapper - returns just the best experience."""
        experiences = self._extract_all_experiences(soup, max_entries=1)
        if not experiences:
            return "", "", None, None
        best = experiences[0]
        return best["title"], best["company"], best["start"], best["end"]

    @staticmethod
    def _school_match_key(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", (name or "").casefold()).strip()

    def _schools_match(self, left: str, right: str) -> bool:
        lk = self._school_match_key(left)
        rk = self._school_match_key(right)
        if not lk or not rk:
            return False
        if lk == rk:
            return True
        return lk in rk or rk in lk

    def _merge_education_entries(self, primary_entries, fallback_entries):
        """
        Merge fallback education extraction into primary entries by school name.
        Fills missing dates/grad year/degree without overwriting present primary values.
        """
        if not primary_entries:
            return fallback_entries or []
        if not fallback_entries:
            return primary_entries

        merged = [dict(e) for e in primary_entries]
        used_fallback = set()

        for p in merged:
            p_school = p.get("school", "")
            for i, f in enumerate(fallback_entries):
                if i in used_fallback:
                    continue
                fallback_school = f.get("school", "")
                if not (
                    self._schools_match(p_school, fallback_school)
                    or (self._is_unt_school_name(p_school) and self._is_unt_school_name(fallback_school))
                ):
                    continue

                if (not p.get("school")) or (
                    self._is_unt_school_name(fallback_school) and not self._is_unt_school_name(p_school)
                ):
                    p["school"] = fallback_school
                if not p.get("degree") and f.get("degree"):
                    p["degree"] = f.get("degree", "")
                if not p.get("major") and f.get("major"):
                    p["major"] = f.get("major", "")
                if not p.get("raw_degree") and f.get("raw_degree"):
                    p["raw_degree"] = f.get("raw_degree", "")
                if not p.get("school_start") and f.get("school_start"):
                    p["school_start"] = f.get("school_start")
                if not p.get("school_end") and f.get("school_end"):
                    p["school_end"] = f.get("school_end")
                if not p.get("graduation_year") and f.get("graduation_year"):
                    p["graduation_year"] = f.get("graduation_year", "")

                used_fallback.add(i)
                break

        # Add fallback-only schools we did not already match.
        for i, f in enumerate(fallback_entries):
            if i in used_fallback:
                continue
            if any(self._schools_match(m.get("school", ""), f.get("school", "")) for m in merged):
                continue
            merged.append(dict(f))

        return merged

    def _sort_education_entries(self, entries):
        def _entry_sort_key(entry):
            school = entry.get("school", "")
            end_info = entry.get("school_end") or {}
            year = 0
            month = 0
            if isinstance(end_info, dict):
                year = int(end_info.get("year") or 0)
                month = int(end_info.get("month") or 0)
                if end_info.get("is_present"):
                    year, month = 9999, 12
            if not year:
                grad_year = str(entry.get("graduation_year", "") or "").strip()
                if grad_year.isdigit():
                    year = int(grad_year)
            has_degree = 1 if (entry.get("degree") or entry.get("raw_degree")) else 0
            return (
                0 if self._is_unt_school_name(school) else 1,
                -year,
                -month,
                -has_degree,
                self._school_match_key(school),
            )

        return sorted(entries or [], key=_entry_sort_key)

    def _build_education_entries_from_groq(self, groq_results):
        entries = []
        for ge in groq_results or []:
            start_raw = ge.get("start_date", "") or ge.get("start_year", "")
            end_raw = ge.get("end_date", "") or ge.get("end_year", "")
            start_info, end_info = self._parse_education_dates(start_raw, end_raw)
            grad_year = ""
            if end_info and end_info.get("year") and end_info["year"] != 9999:
                grad_year = str(end_info["year"])
            entries.append({
                "school": ge.get("school", ""),
                "degree": ge.get("degree_raw", ge.get("raw_degree", "")),
                "major": ge.get("major_raw", ""),
                "raw_degree": ge.get("degree_raw", ge.get("raw_degree", "")),
                "graduation_year": grad_year,
                "school_start": start_info,
                "school_end": end_info,
            })
        return entries

    @staticmethod
    def _parse_education_dates(start_raw: str, end_raw: str):
        """
        Parse education start/end dates from either split fields or combined ranges.
        Handles cases like:
          - start='2024', end='2028'
          - start='2024 - 2028', end=''
          - start='', end='Aug 2020 - May 2024'
        """
        start_raw = (start_raw or "").strip()
        end_raw = (end_raw or "").strip()

        # Normal path: separate start/end values.
        start_info = parse_groq_date(start_raw) if start_raw else None
        end_info = parse_groq_date(end_raw) if end_raw else None

        # If either side is missing, try parsing date ranges from either field.
        if not (start_info and end_info):
            for candidate in (start_raw, end_raw):
                if not candidate:
                    continue
                s_d, e_d = utils.parse_date_range_line(candidate)
                if s_d and e_d:
                    return s_d, e_d

        # Final attempt: combine split tokens and parse as one range.
        if not (start_info and end_info) and start_raw and end_raw:
            combined = f"{start_raw} - {end_raw}"
            s_d, e_d = utils.parse_date_range_line(combined)
            if s_d and e_d:
                return s_d, e_d

        return start_info, end_info

    def _extract_education_entries(self, soup):
        edu_root = self._find_section_root(soup, "Education")
        if not edu_root:
            return []

        entries = []
        candidate_containers = (
            edu_root.select("li.artdeco-list__item")
            or edu_root.select("li.pvs-list__paged-list-item")
            or edu_root.select("div[data-view-name='profile-component-entity']")
            or edu_root.find_all("div")
        )

        for div in candidate_containers:
            lines = self._p_texts_clean(div)
            if len(lines) < 1:
                continue

            # --- Smart school name detection ---
            # Instead of blindly using lines[0], look for /school/ links first
            # as the most reliable indicator of school name.
            school = ""
            school_links = [
                (a.get("href") or "").strip()
                for a in div.find_all("a", href=True)
                if "/school/" in ((a.get("href") or "").lower())
            ]
            unt_link_present = any(self._is_unt_school_href(href) for href in school_links)
            if unt_link_present:
                school = "University of North Texas"
            elif school_links:
                # Extract school name from the anchor text
                for anchor in div.find_all("a", href=True):
                    href = (anchor.get("href") or "").strip()
                    if "/school/" not in href.lower():
                        continue
                    school_text = anchor.get_text(" ", strip=True)
                    if school_text and len(school_text) > 2:
                        school = school_text
                        break
            
            # Also try t-bold span (LinkedIn's pattern for primary text)
            if not school:
                bold_elem = div.select_one('.t-bold span[aria-hidden="true"]') or div.select_one('.t-bold')
                if bold_elem:
                    bold_text = bold_elem.get_text(strip=True).strip()
                    # Only use if it looks like a school name (not a date, not too short)
                    if bold_text and len(bold_text) > 2 and not utils.DATE_RANGE_RE.search(bold_text):
                        school = bold_text
            
            # Fallback: use first line
            if not school:
                school = lines[0].strip()
            
            # Validate school name
            if not school or len(school) < 3:
                continue
            
            # Initialize
            degree = ""
            school_start, school_end, grad_year = None, None, ""
            
            # Check if line 1 is a date range (LinkedIn sometimes puts dates right after school)
            if len(lines) > 1:
                potential_degree = lines[1].strip()
                
                # Check if it looks like a date range (e.g., "2022 - 2026" or "Jan 2022 - Present")
                if utils.DATE_RANGE_RE.search(potential_degree) or utils.YEAR_RANGE_RE.search(potential_degree):
                    # It's a date, not a degree
                    s_d, e_d = utils.parse_date_range_line(potential_degree)
                    if s_d and e_d:
                        school_start, school_end = s_d, e_d
                        if e_d.get("year"):
                            grad_year = str(e_d.get("year"))
                    else:
                        # Fallback: extract years directly
                        years = re.findall(r"\d{4}", potential_degree)
                        if years:
                            grad_year = years[-1]  # Last year is graduation
                else:
                    # It's a degree
                    degree = potential_degree
            
            # Look for dates in remaining lines
            start_line = 2 if degree else 1
            for t in lines[start_line:]:
                if school_start and school_end:
                    break  # Already found dates
                    
                s_d, e_d = utils.parse_date_range_line(t)
                if s_d and e_d:
                    school_start, school_end = s_d, e_d
                    if e_d.get("year"):
                        grad_year = str(e_d.get("year"))
                    break
                    
                # Fallback year finder
                if not grad_year and utils.YEAR_RANGE_RE.search(t):
                    years = re.findall(r"\d{4}", t)
                    if years:
                        grad_year = years[-1]

            # Heuristic check for validity
            school_hint = unt_link_present or bool(re.search(r"(university|college|institute|school)", school, re.I))
            degree_hint = bool(degree and re.search(r"(degree|bachelor|master|phd|mba|\bbs\b|\bba\b)", degree, re.I))
            
            if not (school_hint or degree_hint):
                continue

            # Filter bad degree text (e.g. date ranges masquerading as degrees)
            if degree and utils.DATE_RANGE_RE.search(degree):
                degree = ""

            entries.append({
                "school": school,
                "degree": degree,
                "major": "",
                "graduation_year": grad_year,
                "school_start": school_start,
                "school_end": school_end
            })

        # De-dupe
        seen = set()
        unique_entries = []
        for e in entries:
            key = (e["school"], e["degree"], e["graduation_year"])
            if key not in seen:
                seen.add(key)
                unique_entries.append(e)
        return self._sort_education_entries(unique_entries)

    def _pick_best_unt_education(self, entries):
        best = None
        best_score = -1

        for e in entries:
            school_name = (e.get("school") or "")
            if not self._is_unt_school_name(school_name):
                continue
            
            score = 0
            deg = (e.get("degree") or "").lower()
            
            # Score based on degree level
            for k, val in utils.DEGREE_LEVELS.items():
                if k in deg:
                    score = val
                    break
            
            # Bonus for Engineering
            if any(k in deg for k in utils.ENGINEERING_KEYWORDS): score += 100
            
            # Bonus for recent year
            yr = e.get("graduation_year")
            if yr and yr.isdigit(): score += int(yr)

            if score > best_score:
                best_score = score
                best = e
        
        return best

    def _extract_education_from_top_card(self, soup):
        """
        Fallback: Extract education from top card shortcuts.
        LinkedIn profiles without a full Education section still show school
        in the top card shortcuts (buttons with aria-label containing 'Education').
        """
        entries = []
        
        # Pattern 1: Look for buttons with aria-label containing 'Education'
        # Example: aria-label="Education: University of North Texas. Click to skip to education card"
        for btn in soup.find_all('button'):
            aria_label = btn.get('aria-label', '')
            if 'education' in aria_label.lower():
                # Extract school name from aria-label
                # Format: "Education: University of North Texas..."
                if ':' in aria_label:
                    parts = aria_label.split(':', 1)
                    if len(parts) > 1:
                        school = parts[1].split('.')[0].strip()
                        if school and len(school) > 2:
                            entries.append({
                                "school": school,
                                "degree": "",
                                "graduation_year": "",
                                "school_start": None,
                                "school_end": None
                            })
                            continue
                
                # Also try to get text from nested span/div
                school_text = btn.get_text(" ", strip=True)
                school_text_lower = school_text.lower() if isinstance(school_text, str) else ""
                if school_text and ('university' in school_text_lower or 'college' in school_text_lower):
                    entries.append({
                        "school": school_text,
                        "degree": "",
                        "graduation_year": "",
                        "school_start": None,
                        "school_end": None
                    })
        
        # Pattern 2: Look for links to school pages
        # These often have href containing "/school/" or company-logo images
        for a in soup.find_all('a'):
            href = a.get('href', '')
            if '/school/' in href:
                school_text = "University of North Texas" if self._is_unt_school_href(href) else a.get_text(" ", strip=True)
                if school_text and len(school_text) > 2:
                    # Clean up the text
                    school_text = school_text.replace('Following', '').strip()
                    if school_text:
                        entries.append({
                            "school": school_text,
                            "degree": "",
                            "graduation_year": "",
                            "school_start": None,
                            "school_end": None
                        })
        
        # De-duplicate by school name
        seen = set()
        unique_entries = []
        for e in entries:
            school_key = e["school"].lower()
            if school_key not in seen:
                seen.add(school_key)
                unique_entries.append(e)
        
        return self._sort_education_entries(unique_entries)

    def scrape_all_education(self, profile_url):
        """
        Open LinkedIn's "Show all education" page and scrape extra education records.
        This is only used when the main profile card cannot confidently identify UNT.
        """
        all_edus = []
        unique_edus = []
        unt_details = None
        
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            link = self._find_show_all_education_link(soup)
            
            if not link:
                logger.debug("No 'Show all education' link found.")
                return [], None

            logger.debug("Found 'Show all education' link. Clicking...")
            if not link.startswith("http"): link = "https://www.linkedin.com" + link
            self.driver.get(link)
            time.sleep(3)
            
            detail_soup = BeautifulSoup(self.driver.page_source, "html.parser")
            entries = []
            if is_groq_available():
                groq_results, _edu_tokens = extract_education_with_groq(
                    str(detail_soup.find("main") or detail_soup),
                    profile_name="unknown",
                )
                if groq_results:
                    entries = self._build_education_entries_from_groq(groq_results)
            else:
                entries = self._extract_education_entries(detail_soup)

            entries = self._sort_education_entries(entries)
            all_edus = list(dict.fromkeys([e.get("school", "") for e in entries if e.get("school")]))

            best_unt = self._pick_best_unt_education(entries)
            if best_unt:
                unt_details = {
                    "education": best_unt.get("school", ""),
                    "degree": best_unt.get("degree", "").strip(),
                    "major": best_unt.get("major", "").strip() or best_unt.get("degree", "").strip(),
                    "graduation_year": best_unt.get("graduation_year", ""),
                    "school_start_date": utils.format_date_for_storage(best_unt.get("school_start")),
                    "school_start": best_unt.get("school_start"),
                    "school_end": best_unt.get("school_end"),
                }
            
            unique_edus = list(dict.fromkeys(all_edus))
            logger.info(f"    ✓ Extracted {len(unique_edus)} education(s) from detailed view.")
            if unt_details:
                logger.info(f"      ✓ Found expanded UNT details: {unt_details.get('major', 'Unknown Major')}")
            else:
                logger.info("      ❌ Still no UNT education found in detailed view.")

            # Go back
            self.driver.get(profile_url)
            time.sleep(2)

        except Exception as e:
            logger.error(f"Error expanding education: {e}")
        
        return unique_edus, unt_details

    def _find_show_all_education_link(self, soup):
        if not soup:
            return None
        for a in soup.find_all("a"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            text = (a.get_text(" ", strip=True) or "").lower()
            href_l = href.lower()
            if "/details/education" in href_l:
                return href
            if "show all" in text and "education" in text:
                return href
        return None

    def _extract_education_entries_from_detailed_view(self, profile_url, soup=None):
        """Open detailed education page (if available) and return up to latest 3 education entries."""
        token_count = 0
        link = self._find_show_all_education_link(soup)
        if not link:
            return [], token_count

        if not link.startswith("http"):
            link = "https://www.linkedin.com" + link

        try:
            logger.debug("Found detailed education link; extracting from expanded page.")
            self.driver.get(link)
            time.sleep(2)

            detail_soup = BeautifulSoup(self.driver.page_source, "html.parser")
            main = detail_soup.find("main") or detail_soup

            entries = []
            if is_groq_available():
                groq_results, edu_tokens = extract_education_with_groq(
                    str(main),
                    profile_name="unknown",
                )
                token_count += edu_tokens

                if groq_results and self._education_entries_exceed_cloud_limits(groq_results):
                    strict_results, strict_tokens = extract_education_with_groq(
                        str(main),
                        profile_name="unknown",
                        strict_mode=True,
                    )
                    token_count += strict_tokens
                    if strict_results:
                        groq_results = strict_results

                entries = self._build_education_entries_from_groq(groq_results)

            else:
                css_entries = self._extract_education_entries(detail_soup)
                entries = self._merge_education_entries(entries, css_entries)
            entries = self._sort_education_entries(entries)

            return entries, token_count
        except Exception as e:
            logger.debug(f"Detailed education extraction failed: {e}")
            return [], token_count
        finally:
            try:
                self.driver.get(profile_url)
                time.sleep(1)
            except Exception:
                pass

    # ============================================================
    # Parsing Helpers
    # ============================================================
    def _find_section_root(self, soup, heading_text):
        """
        Resolve a section container by heading text.
        LinkedIn markup changes often, so we probe multiple heading patterns.
        """
        norm = heading_text.lower()
        
        # Try h2, h3 tags
        for tag in ("h2", "h3"):
            for h in soup.find_all(tag):
                text = h.get_text(" ", strip=True).lower()
                # Changed to partial match to handle LinkedIn's <!---->Education<!----> structure
                if norm in text:
                    return h.find_parent("section") or h.find_parent("div")
        
        # Also try span with aria-hidden (LinkedIn's current pattern)
        for span in soup.find_all("span", {"aria-hidden": "true"}):
            text = span.get_text(" ", strip=True).lower()
            if text == norm:
                # Walk up to find section or card
                parent = span.find_parent("section")
                if parent:
                    return parent

        component_markers = (
            f"{norm}toplevelsection",
            f"profile_{norm}_top_anchor",
            f"/details/{norm}",
        )
        for tag in soup.find_all(["section", "div"]):
            haystack = " ".join(
                str(tag.get(attr, "") or "")
                for attr in ("componentkey", "id", "data-view-name", "aria-label")
            ).lower()
            if any(marker in haystack for marker in component_markers):
                return tag

        if norm == "education":
            school_link = soup.find("a", href=lambda href: href and "/school/" in href.lower())
            if school_link:
                return school_link.find_parent("section") or school_link.find_parent("div")
        
        return None

    def _p_texts_clean(self, container):
        """Extract clean text lines from a container element.
        
        Works on a COPY of the container to avoid destructively modifying
        the soup, which could corrupt subsequent parsing if the same soup
        object is reused (e.g., for education + experience sections).
        """
        if not container: return []
        import copy
        work = copy.copy(container)
        # Remove "visually hidden" or skill descriptions
        for bad in work.select("[data-testid='expandable-text-box'], .visually-hidden"):
            bad.decompose()
        
        lines = []
        for p in work.find_all(["p", "span"]):
            # Specific exclusion for skill badges
            if p.select_one("svg"): continue
            t = p.get_text(" ", strip=True)
            if t and t not in lines:
                lines.append(t)
        return lines

    def _clean_company(self, text):
        """Remove employment type and location suffixes from company text.
        
        Handles multi-segment lines like:
          "Company · Location · Full-time"  → "Company"
          "Company · Part-time"             → "Company"
          "Company · Remote"                → "Company"
        """
        if not text: return ""
        # First strip employment type and everything after it
        cleaned = re.sub(r"\s*·\s*(Full-time|Part-time|Contract|Internship|Remote|Hybrid|On-site|Self-employed|Freelance|Seasonal|Temporary|Apprenticeship).*$", "", text, flags=re.I).strip()
        # Then check if remaining text still has a · separator with a location fragment
        # e.g., "Company · Dallas" → strip the location part
        if "·" in cleaned:
            parts = [p.strip() for p in cleaned.split("·")]
            if len(parts) == 2:
                from entity_classifier import is_location
                # If the second segment looks like a location, keep only the first
                if is_location(parts[1]):
                    cleaned = parts[0]
        return cleaned
