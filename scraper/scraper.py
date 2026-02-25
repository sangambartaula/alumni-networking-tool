import time
import json
import random
import re
from datetime import datetime
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
import utils
import config
from config import logger, print_profile_summary
from entity_classifier import classify_entity, is_location, is_university, get_classifier
from groq_client import is_groq_available, verify_location, parse_groq_date, _clean_doubled
from groq_extractor_experience import extract_experiences_with_groq
from groq_extractor_education import extract_education_with_groq
from utils import determine_work_study_status

try:
    from job_title_normalization import normalize_title_deterministic, normalize_title_with_groq
    from company_normalization import normalize_company_deterministic, normalize_company_with_groq
    from degree_normalization import standardize_degree
    from major_normalization import standardize_major
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
    - Unescape HTML entities (e.g. &amp; -> &)
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

    def quit(self):
        if self.driver:
            self.driver.quit()
            logger.info("✓ WebDriver closed")

    # ============================================================
    # Navigation & Waits
    # ============================================================
    def scroll_full_page(self):
        """
        Faster and more reliable scroll using window.scrollBy.
        This is critical for LinkedIn because many profile sections (like Education
        and Experience) are lazy-loaded and only appear in the DOM when scrolled into view.
        """
        logger.debug("Scrolling page...")
        try:
            # Scroll down in chunks to trigger lazy loading
            # Reduced steps from 12 to 5 for speed
            for _ in range(5):
                self.driver.execute_script("window.scrollBy(0, 800);")
                time.sleep(random.uniform(0.8, 1.2))

            # One final scroll to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)

            # Scroll back up to top to ensure elements are interactable
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)
        except Exception:
            pass

    def extract_profile_urls_from_page(self):
        logger.debug("Extracting profile URLs...")
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
                    url = url.rstrip('/')
                    profile_urls.add(url)
        
        return list(profile_urls)

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
                    const h = document.querySelector('h1, h2');
                    return (h ? (h.innerText || '').trim().length : 0) >= 2;
                """)
                if ok: return True
            except Exception: pass
            time.sleep(0.5)
        return False

    def _wait_for_education_ready(self, timeout=15):
        """Wait for the Education section to become available."""
        end = time.time() + timeout
        while time.time() < end:
            try:
                # Scroll a bit if not found yet to trigger render
                self.driver.execute_script("window.scrollBy(0, 300);")
                
                ok = self.driver.execute_script("""
                    const m = document.querySelector('main') || document.body;
                    const headings = Array.from(m.querySelectorAll('h2,h3,span')); 
                    // Added 'span' because sometimes headers are inside spans now
                    
                    const h = headings.find(x => (x.innerText || '').trim().toLowerCase().includes('education'));
                    if (!h) return false;
                    
                    return true;
                """)
                if ok: return True
            except Exception: pass
            time.sleep(0.5)
        return False

    def _page_looks_blocked(self):
        try:
            url = (self.driver.current_url or "").lower()
            title = (self.driver.title or "").lower()
            html = (self.driver.page_source or "").lower()
            
            if any(x in url for x in ("checkpoint", "authwall", "challenge")): return True
            if any(x in title for x in ("sign in", "security verification")): return True
            if len(html.strip()) < 1000: return True
            return False
        except Exception:
            return False

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
    def _initialize_profile_data(self, profile_url):
        """Return the normalized data envelope used for every profile scrape."""
        return {
            "name": "", "headline": "", "location": "",
            "job_title": "", "company": "", "job_start_date": "", "job_end_date": "",
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

            # Check if blocked
            if self._page_looks_blocked():
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

            # 1. Trigger Full Page Load (Aggressive Scroll)
            # This is critical for the Education section to appear in DOM
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, 0);") # Go back up to parse
            time.sleep(1)

            # 2. Wait for Education specifically
            found_edu = self._wait_for_education_ready(timeout=10)
            if not found_edu:
                logger.debug("Education section not detected quickly (might be missing or different layout).")

            soup = BeautifulSoup(self.driver.execute_script("return document.body.innerHTML;"), "html.parser")

            # 3. Top Card
            name, headline, location = self._extract_top_card(soup)
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

            if is_groq_available():
                edu_root = self._find_section_root(soup, "Education")
                if edu_root:
                    edu_html = str(edu_root)
                    groq_results, edu_tokens = extract_education_with_groq(edu_html, profile_name=data.get("name", "unknown"))
                    _total_tokens += edu_tokens
                    if groq_results:
                        logger.debug(f"Using Groq education results ({len(groq_results)} entries)")
                        for ge in groq_results:
                            start_info = parse_groq_date(ge.get("start_date", ""))
                            end_info = parse_groq_date(ge.get("end_date", ""))
                            grad_year = ""
                            if end_info and end_info.get("year") and end_info["year"] != 9999:
                                grad_year = str(end_info["year"])
                            edu_entries.append({
                                "school": ge.get("school", ""),
                                "degree": ge.get("degree_raw", ge.get("raw_degree", "")),
                                "major": ge.get("major_raw", ""),
                                "raw_degree": ge.get("degree_raw", ge.get("raw_degree", "")),
                                "graduation_year": grad_year,
                                "school_start": start_info,
                                "school_end": end_info,
                            })

            # CSS fallback if Groq didn't produce results
            if not edu_entries:
                edu_entries = self._extract_education_entries(soup)
            
            # Fallback: Check top card shortcuts for education if no Education section found
            if not edu_entries:
                edu_entries = self._extract_education_from_top_card(soup)
            
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
                if not edu_entries or "unt" not in str(edu_entries).lower():
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
                        return None

            # --- Store up to 3 education entries (school2/degree2/major2, etc.) ---
            # Trust the extraction layer (Groq/CSS) to return clean entries.
            # Only exclude the primary entry itself.
            primary_entry = best_unt if best_unt else None
            other_entries = [e for e in edu_entries if e is not primary_entry]
            for i, entry in enumerate(other_entries[:2], start=2):
                data[f"school{i}"] = entry.get("school", "")
                data[f"degree{i}"] = entry.get("degree", "").strip()
                data[f"major{i}"] = entry.get("major", "").strip()

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
            data["job_title"] = best_exp["title"]
            data["company"] = best_exp["company"]
            data["job_start_date"] = utils.format_date_for_storage(best_exp["start"])
            data["job_end_date"] = utils.format_date_for_storage(best_exp["end"])
        else:
            data["job_title"] = ""
            data["company"] = ""
            data["job_start_date"] = ""
            data["job_end_date"] = ""

        for i, exp in enumerate(all_experiences[1:3], start=2):
            data[f"exp{i}_title"] = exp["title"]
            data[f"exp{i}_company"] = exp["company"]
            data[f"exp{i}_dates"] = (
                f"{utils.format_date_for_storage(exp['start'])} - "
                f"{utils.format_date_for_storage(exp['end'])}"
            )

        for i in range(2, 4):
            if f"exp{i}_title" not in data:
                data[f"exp{i}_title"] = ""
                data[f"exp{i}_company"] = ""
                data[f"exp{i}_dates"] = ""

    @staticmethod
    def _append_standardization_log(filename, raw, standardized):
        if raw and standardized and raw != standardized:
            try:
                with open(f"scraper/output/{filename}", "a") as f:
                    f.write(f"{raw} -> {standardized}\n")
            except Exception:
                pass

    def _apply_education_and_discipline_normalization(self, data):
        """
        Apply degree/major normalization and infer discipline once education is set.
        """
        try:
            data["discipline"] = infer_discipline(
                f"{data.get('degree', '')} {data.get('major', '')}",
                data.get("job_title", ""),
                data.get("headline", ""),
                use_llm=True
            )

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
                    std_maj = standardize_major(raw_maj, data.get("job_title", ""))
                    data[std_maj_key] = std_maj
                    self._append_standardization_log("standardized_major.txt", raw_maj, std_maj)

            if data.get("discipline") and data.get("discipline") != "Unknown":
                with open("scraper/output/inferred_disciplines.txt", "a") as f:
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
        for idx, suffix in enumerate(["", "2", "3"], start=1):
            title_key = "job_title" if not suffix else f"exp{idx}_title"
            comp_key = "company" if not suffix else f"exp{idx}_company"
            raw_title = data.get(title_key, "")
            raw_comp = data.get(comp_key, "")
            if raw_title:
                data[f"normalized_job_title" if not suffix else f"normalized_exp{idx}_title"] = (
                    normalize_title_deterministic(raw_title)
                )
            if raw_comp:
                data[f"normalized_company" if not suffix else f"normalized_exp{idx}_company"] = (
                    normalize_company_deterministic(raw_comp)
                )

    @staticmethod
    def _log_missing_data_warnings(data, all_experiences, edu_entries):
        if not all_experiences:
            logger.warning(f"No experience found for {data.get('name', 'Unknown')}")
        if not edu_entries:
            logger.warning(f"No education found for {data.get('name', 'Unknown')}")
        if not data.get("graduation_year"):
            logger.warning(f"No graduation year for {data.get('name', 'Unknown')}")

    # ============================================================
    # Missing-date fallback (strict UNT + Graduate Assistant)
    # ============================================================
    _UNT_FULL_NAME_RE = re.compile(r'university\s+of\s+north\s+texas', re.IGNORECASE)
    _UNT_TOKEN_RE = re.compile(r'\bunt\b', re.IGNORECASE)

    def _is_unt_school_name(self, name: str) -> bool:
        if not name:
            return False
        return bool(self._UNT_FULL_NAME_RE.search(name) or self._UNT_TOKEN_RE.search(name))

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
    def _extract_top_card(self, soup):
        name, headline, location = "", "", ""
        
        # Name - Try h1 first (main profile name)
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(" ", strip=True)
            # Clean pronouns if present
            name = re.sub(r'\s*\(.*?\)\s*$', '', name).strip()
        
        # Fallback to h2 if no h1
        if not name:
            for h in soup.find_all("h2"):
                t = h.get_text(" ", strip=True)
                if len(t) >= 2 and len(t) < 60 and not any(x in t.lower() for x in ["linkedin", "contact info", "experience", "education"]):
                    name = t
                    break

        # Headline - Look for 'text-body-medium' class (LinkedIn's current pattern)
        for div in soup.find_all("div", class_=lambda x: x and "text-body-medium" in x):
            text = div.get_text(" ", strip=True)
            if text and len(text) > 5 and len(text) < 200:
                # Skip if it looks like a date or connection badge
                if not re.search(r'^\d{4}|^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', text):
                    headline = text
                    break
        
        # Fallback: Look for headline in data-generated-suggestion-target attribute area
        if not headline:
            for div in soup.find_all("div", {"data-generated-suggestion-target": True}):
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
        
        for span in soup.find_all("span", class_=lambda x: x and "text-body-small" in x):
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
            text_lower = text.lower()
            
            # Skip badge-like entries (schools, companies)
            if any(x in text_lower for x in location_blacklist):
                continue
            
            # Skip connection/follower/contact text
            if any(x in text_lower for x in ["connection", "follower", "contact info"]):
                continue
            
            # Valid location patterns:
            # 1. Contains comma (City, State or City, Country format)
            # 2. Contains specific location keywords like "metroplex", "area"
            # 3. Contains country names
            has_comma = "," in text
            has_location_keyword = any(x in text_lower for x in ["metroplex", "area", "metro"])
            has_country = any(x in text_lower for x in ["united states", "india", "canada", "uk", "germany", "australia"])
            
            # Use EntityClassifier for more robust check
            classifier = get_classifier()
            
            if classifier.is_location(text):
                location = text
                break
            
            # If not obvious from heuristics, use Groq if available
            if (is_location_styled or has_comma or has_country) and is_groq_available():
                # Potential location, verify with Groq
                if verify_location(text):
                    location = text
                    break
        
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
                        
                        title = job.get("job_title", "")
                        company = job.get("company", "")
                        
                        if (title or company) and start_d and end_d:
                            u_key = f"{title.lower()}|{company.lower()}|{start_d}|{end_d}"
                            if u_key not in seen_entries:
                                parsed.append({
                                    "title": title,
                                    "company": company,
                                    "start": start_d,
                                    "end": end_d
                                })
                                seen_entries.add(u_key)
                    
                    if parsed:
                        logger.debug(f"Groq extracted {len(parsed)} experience(s)")
                        return parsed[:max_entries]
                        
            except Exception as e:
                logger.warning(f"Groq extraction failed: {e}")
        
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
                
                # This could be "Company · Part-time" format
                if text and not utils.DATE_RANGE_RE.search(text):
                    candidate_company = _clean_doubled(self._clean_company(text))
                    from entity_classifier import is_location
                    if is_location(candidate_company):
                        parent_ul = container.find_parent('ul')
                        if parent_ul:
                            outer_container = parent_ul.find_parent('div', attrs={'data-view-name': 'profile-component-entity'})
                            if outer_container:
                                outer_title_elem = outer_container.select_one('.t-bold span[aria-hidden="true"]') or outer_container.select_one('.t-bold')
                                if outer_title_elem:
                                    candidate_company = _clean_doubled(self._clean_company(outer_title_elem.get_text(strip=True)))
                    company = candidate_company
                    break
            
            # Dates: Look for pvs-entity__caption-wrapper or t-black--light
            date_spans = container.select('span.pvs-entity__caption-wrapper[aria-hidden="true"], span.t-black--light span[aria-hidden="true"]')
            for span in date_spans:
                text = span.get_text(strip=True)
                if utils.DATE_RANGE_RE.search(text):
                    start_d, end_d = utils.parse_date_range_line(text)
                    if start_d and end_d:
                        break
            
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
                        "start": start_d,
                        "end": end_d
                    })
                    seen_entries.add(u_key)
        
        # ============================================================
        # APPROACH 2: Fallback to text-based extraction if Approach 1 failed
        # ============================================================
        if not parsed:
            logger.debug("Direct CSS extraction found nothing, trying text-based fallback...")
            parsed = self._extract_experiences_text_based(exp_root, max_entries, seen_entries)
        
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
                            "start": start_d,
                            "end": end_d
                        })
                        seen_entries.add(u_key)

        return parsed[:max_entries]

    def _clean_context_line(self, t):
        # Filtering helper
        t = t.strip()
        if not t: return None
        if re.match(r'^(Full-time|Part-time|Contract|Internship)', t, re.I): return None
        if t in ['·', '•']: return None
        # Remove suffix like " · Full-time"
        return re.sub(r'\s*·\s*(Full-time|Part-time|Contract|Internship|Remote|Hybrid).*$', '', t, flags=re.I).strip()

    def _split_context_line(self, text):
        potential_parts = []
        
        # Split by various delimiters: 
        # - " at " / " @ "
        # - " | " (pipe)
        # - " - " / " – " (dash/en-dash with spaces)
        # - " · " (dot)
        
        # Regex for delimiters
        # We require spaces around dashes to avoid splitting "Co-Founder" or "Tier-1"
        # We allow flexible spacing for pipes and dots
        delimiters = r'\s+(?:at|@)\s+|\s*\|\s*|\s+(?:-|–)\s+|\s*·\s*'
        
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

    def _extract_education_entries(self, soup):
        edu_root = self._find_section_root(soup, "Education")
        if not edu_root: return []

        entries = []
        for div in edu_root.find_all("div"):
            lines = self._p_texts_clean(div)
            if len(lines) < 1: continue

            school = lines[0].strip()
            
            # Validate school name
            if not school or len(school) < 3: continue
            
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
            school_hint = bool(re.search(r"(university|college|institute|school)", school, re.I))
            degree_hint = bool(degree and re.search(r"(degree|bachelor|master|phd|mba|\bbs\b|\bba\b)", degree, re.I))
            
            if not (school_hint or degree_hint): continue

            # Filter bad degree text (e.g. date ranges masquerading as degrees)
            if degree and utils.DATE_RANGE_RE.search(degree):
                degree = ""

            entries.append({
                "school": school,
                "degree": degree,
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
        return unique_entries

    def _pick_best_unt_education(self, entries):
        best = None
        best_score = -1

        for e in entries:
            school_lower = (e.get("school") or "").lower()
            if not any(k in school_lower for k in utils.UNT_KEYWORDS):
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
                # Get the school name from the link text
                school_text = a.get_text(" ", strip=True)
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
        
        return unique_entries

    def scrape_all_education(self, profile_url):
        """
        Open LinkedIn's "Show all education" page and scrape extra education records.
        This is only used when the main profile card cannot confidently identify UNT.
        """
        all_edus = []
        unt_details = None
        
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            link = None
            for a in soup.find_all('a'):
                if 'show all' in a.get_text(strip=True).lower() and 'education' in a.get('href', '').lower():
                    link = a.get('href')
                    break
            
            if not link:
                logger.debug("No 'Show all education' link found.")
                return [], None

            logger.debug("Found 'Show all education' link. Clicking...")
            if not link.startswith("http"): link = "https://www.linkedin.com" + link
            self.driver.get(link)
            time.sleep(3)
            
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            main = soup.find('main') or soup
            
            for div in main.find_all("div"):
                lines = self._p_texts_clean(div)
                if len(lines) < 2: continue
                
                school = lines[0].strip()
                degree = lines[1].strip()
                
                # Check keywords
                if not any(k in school.lower() for k in ["university", "college", "school", "institute"]):
                    if not any(k in degree.lower() for k in ["degree", "bachelor", "master"]):
                        continue

                all_edus.append(school)
                logger.debug(f"Found: {school} | {degree}")
                
                # Capture UNT details if found
                if unt_details is None and any(k in school.lower() for k in utils.UNT_KEYWORDS):
                    unt_details = {
                        "education": school,
                        "major": degree,
                        "graduation_year": "",
                        "school_start_date": "",
                        "school_start": None,
                        "school_end": None
                    }
                    # Find dates
                    for t in lines[2:]:
                        s_d, e_d = utils.parse_date_range_line(t)
                        if s_d and e_d:
                            unt_details["school_start"] = s_d
                            unt_details["school_end"] = e_d
                            unt_details["school_start_date"] = utils.format_date_for_storage(s_d)
                            if not e_d.get("is_present") and e_d.get("year"):
                                unt_details["graduation_year"] = str(e_d.get("year"))
                            break
                        if re.findall(r"\d{4}", t) and not unt_details["graduation_year"]:
                            unt_details["graduation_year"] = re.findall(r"\d{4}", t)[-1]
            
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
        
        return None

    def _p_texts_clean(self, container):
        if not container: return []
        # Remove "visually hidden" or skill descriptions
        for bad in container.select("[data-testid='expandable-text-box'], .visually-hidden"):
            bad.decompose()
        
        lines = []
        for p in container.find_all(["p", "span"]):
            # Specific exclusion for skill badges
            if p.select_one("svg"): continue
            t = p.get_text(" ", strip=True)
            if t and t not in lines:
                lines.append(t)
        return lines

    def _clean_company(self, text):
        if not text: return ""
        return re.sub(r"\s*·\s*(Full-time|Part-time|Contract|Internship|Remote|Hybrid).*$", "", text, flags=re.I).strip()
