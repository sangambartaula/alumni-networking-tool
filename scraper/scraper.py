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
import utils
import config
from config import logger
from entity_classifier import classify_entity, is_location, is_university


def normalize_scraped_data(data):
    """
    Normalize all scraped data fields:
    - Strip leading/trailing whitespace from all string fields
    - Remove trailing slashes from URLs
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
            data[key] = value
        elif isinstance(value, list):
            # Handle list fields like all_education
            data[key] = [v.strip() if isinstance(v, str) else v for v in value]
    
    return data

class LinkedInScraper:
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
        # Blocks some automated detection
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10) # Reduced global wait
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
        Updated to be faster and more reliable using window.scrollBy
        """
        logger.info("Scrolling page...")
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
        logger.info("Extracting profile URLs...")
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
        """
        Wait for Education section. 
        Reduced timeout to 15s to fail faster if missing.
        """
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

    # ============================================================
    # Core Scraping Logic
    # ============================================================
    def scrape_profile_page(self, profile_url):
        data = {
            "name": "", "headline": "", "location": "",
            "job_title": "", "company": "", "job_start_date": "", "job_end_date": "",
            "education": "", "major": "", "school_start_date": "", "graduation_year": "",
            "working_while_studying": "",
            "profile_url": profile_url,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "all_education": []
        }

        try:
            logger.info(f"  Opening profile: {profile_url}")
            self.driver.get(profile_url)
            self._force_focus()
            
            # Initial settle
            time.sleep(2)

            # Check if blocked
            if self._page_looks_blocked():
                logger.warning("⚠️ Page looks blocked or empty.")
                return None
            
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
                logger.info("    ℹ️ Education section not detected quickly (might be missing or different layout).")

            soup = BeautifulSoup(self.driver.execute_script("return document.body.innerHTML;"), "html.parser")

            # 3. Top Card
            name, headline, location = self._extract_top_card(soup)
            data["name"] = name
            data["headline"] = headline
            data["location"] = location or "Not Found"

            # 4. Experience - Get up to 3 entries
            all_experiences = self._extract_all_experiences(soup, max_entries=3)
            
            # Primary experience (most recent)
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
            
            # Additional experiences (exp2, exp3)
            for i, exp in enumerate(all_experiences[1:3], start=2):
                data[f"exp{i}_title"] = exp["title"]
                data[f"exp{i}_company"] = exp["company"]
                data[f"exp{i}_dates"] = f"{utils.format_date_for_storage(exp['start'])} - {utils.format_date_for_storage(exp['end'])}"
            
            # Fill empty slots
            for i in range(2, 4):
                if f"exp{i}_title" not in data:
                    data[f"exp{i}_title"] = ""
                    data[f"exp{i}_company"] = ""
                    data[f"exp{i}_dates"] = ""

            # 5. Education
            edu_entries = self._extract_education_entries(soup)
            
            # Fallback: Check top card shortcuts for education if no Education section found
            if not edu_entries:
                edu_entries = self._extract_education_from_top_card(soup)
            
            data["all_education"] = list(dict.fromkeys([e["school"] for e in edu_entries if e.get("school")]))

            best_unt = self._pick_best_unt_education(edu_entries)
            
            if best_unt:
                data["education"] = best_unt.get("school", "")
                data["degree"] = best_unt.get("degree", "")
                data["major"] = ""   # leave empty so backend infers discipline
                data["graduation_year"] = best_unt.get("graduation_year", "")

                
                school_start_d = best_unt.get("school_start")
                school_end_d = best_unt.get("school_end")
                data["school_start_date"] = utils.format_date_for_storage(school_start_d)

                # Check ALL experiences for overlap with school (not just the latest)
                is_overlap = False
                for exp in all_experiences:
                    job_start_d = exp.get("start")
                    job_end_d = exp.get("end")
                    if utils.check_working_while_studying(school_start_d, school_end_d, job_start_d, job_end_d):
                        is_overlap = True
                        break
                data["working_while_studying"] = "yes" if is_overlap else "no"
            else:
                # If we have NO education entries, or just no UNT, try expanding
                if not edu_entries or "unt" not in str(edu_entries).lower():
                    logger.info("    ❌ No UNT education found in main profile. Expanding...")
                    expanded_edus, unt_details = self.scrape_all_education(profile_url)
                    
                    if expanded_edus:
                        data["all_education"] = list(dict.fromkeys(expanded_edus))
                    
                    if unt_details:
                        data["education"] = unt_details.get("education", "")
                        data["major"] = unt_details.get("major", "")
                        data["graduation_year"] = unt_details.get("graduation_year", "")
                        data["school_start_date"] = unt_details.get("school_start_date", "")
                        
                        if unt_details.get("school_start") and unt_details.get("school_end"):
                            # Check ALL experiences for overlap
                            is_overlap = False
                            for exp in all_experiences:
                                if utils.check_working_while_studying(
                                    unt_details["school_start"], unt_details["school_end"], 
                                    exp.get("start"), exp.get("end")
                                ):
                                    is_overlap = True
                                    break
                            data["working_while_studying"] = "yes" if is_overlap else "no"
                    else:
                        return None # No UNT found at all

            # Log scrape summary
            logger.info(f"    ✓ Scraped: {data.get('name', 'N/A')}")
            logger.info(f"      Headline: {data.get('headline', 'N/A')[:60]}{'...' if len(data.get('headline', '')) > 60 else ''}")
            logger.info(f"      Location: {data.get('location', 'N/A')}")
            logger.info(f"      Company: {data.get('company', 'N/A')} | Job: {data.get('job_title', 'N/A')}")
            if len(all_experiences) > 1:
                logger.info(f"      ({len(all_experiences)} experiences captured)")
            logger.info(f"      School: {data.get('education', 'N/A')} | Grad: {data.get('graduation_year', 'N/A')}")
            logger.info(f"      Major: {data.get('major', 'N/A')}")
            logger.info(f"      Working While Studying: {data.get('working_while_studying', 'N/A')}")

            # Normalize all fields before returning
            return normalize_scraped_data(data)

        except Exception as e:
            logger.error(f"Error scraping profile {profile_url}: {e}")
            return None

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
            
            if is_location_styled or has_comma or has_location_keyword or has_country:
                if has_comma or has_location_keyword or has_country:  # Must still match a location pattern
                    location = text
                    break
        
        return name, headline, location

    def _extract_all_experiences(self, soup, max_entries=3):
        """Extract up to max_entries experience entries, sorted by most recent first."""
        exp_root = self._find_section_root(soup, "Experience")
        if not exp_root: return []

        # Patterns to filter OUT (these are not company/title)
        junk_patterns = re.compile(
            r'^(Full-time|Part-time|Contract|Internship|Freelance|Self-employed|Seasonal|Temporary|Remote|Hybrid|On-site)$|'
            r'^\d+\s*(yr|yrs|year|years|mo|mos|month|months)\b|'  # "3 yrs 1 mo"
            r'^·\s*\d+\s*(yr|yrs|mo|mos)|'  # "· 3 yrs"
            r'^\d+\s*(yr|yrs)?\s*\d*\s*(mo|mos)?$',  # Just duration
            re.I
        )
        
        # Company indicator patterns
        company_hints = re.compile(
            r'\b(Inc\.?|Corp\.?|LLC|Ltd\.?|Company|Co\.?|Technologies|Solutions|Enterprises|Group|Partners|Services|Consulting|Software|Systems|S\.?R\.?L\.?)(?=\W|$)',
            re.I
        )
        
        # Job title indicator patterns
        title_hints = re.compile(
            r'\b(Engineer|Developer|Manager|Director|Analyst|Designer|Consultant|Specialist|Associate|Intern|Lead|Senior|Junior|Sr\.?|Jr\.?|Chief|Head|VP|Vice President|Coordinator|Administrator|Representative|Officer|Architect|Scientist|Drafter|Assistant|Fellow|Co-op)\b',
            re.I
        )

        # Track context for grouped experiences (Company Name)
        context_company = ""
        
        candidates = []
        # Iterate specific containers if possible, but find_all("div") is broad.
        # We rely on specific content patterns to distinguish Headers vs Rows.
        
        # We process in document order.
        divs = exp_root.find_all("div", recursive=True) # Recursive to find nested items
        
        parsed = []
        seen_entries = set() # Avoid duplicates based on unique content signature
        
        for div in divs:
            lines = self._p_texts_clean(div)
            if not lines: continue
            
            # Check signatures
            has_date_range = any(utils.DATE_RANGE_RE.search(t) for t in lines)
            has_duration = any(re.match(r'^\d+\s*(yr|yrs|mo|mos)', t, re.I) for t in lines)
            
            # 1. Potential Company Header (No specific date range like "Jan 2020 - Present", but has "2 yrs 5 mos")
            if has_duration and not has_date_range:
                # Attempt to extract company name from this header to utilize as context
                # Usually the company name is the first non-junk line
                for t in lines:
                    clean_t = t.strip()
                    if not clean_t: continue
                    if junk_patterns.match(clean_t): continue
                    
                    # Classify
                    ent_type, conf = classify_entity(clean_t)
                    if ent_type == "company" and conf >= 0.7:
                        context_company = clean_t
                        # print(f"DEBUG: Set context company to {context_company}")
                        break
                continue # Don't process this div as an experience entry itself
            
            # 2. Potential Experience Entry (Has Date Range)
            if has_date_range:
                # Identify the date line
                date_idx = next((i for i, t in enumerate(lines) if utils.DATE_RANGE_RE.search(t)), None)
                if date_idx is None: continue
                
                start_d, end_d = utils.parse_date_range_line(lines[date_idx])
                if not (start_d and end_d): continue
                
                # Context is lines mostly BEFORE the date
                # Grouped experiences often have Title BEFORE date.
                # Sometimes Company is also there.
                
                # Use a window of text
                text_window = lines[max(0, date_idx - 4):date_idx]
                if not text_window: continue
                
                # Analyze content
                company = ""
                title = ""
                
                classified_items = []
                for t in text_window:
                    clean_t = self._clean_context_line(t)
                    if not clean_t: continue
                    
                    # Split logic (at/dots)
                    parts = self._split_context_line(clean_t)
                    
                    for part in parts:
                        if is_location(part): continue
                        if is_university(part):
                            classified_items.append((part, "university", 0.6))
                        else:
                            e_type, conf = classify_entity(part)
                            classified_items.append((part, e_type, conf))

                # Sort by confidence primarily, but we might override based on hints
                classified_items.sort(key=lambda x: -x[2])
                
                # Re-eval candidates with hints to correct misclassifications
                final_candidates = []
                for text, cat, conf in classified_items:
                    # Override: If it matches a specific title hint, force it to job_title
                    if title_hints.search(text):
                        final_candidates.append((text, "job_title", 1.0))
                    # Override: If it matches a specific company hint, force it to company
                    elif company_hints.search(text):
                        final_candidates.append((text, "company", 1.0))
                    else:
                        final_candidates.append((text, cat, conf))
                
                # Re-sort after overrides (hints get 1.0 confidence)
                final_candidates.sort(key=lambda x: -x[2])

                for item_text, item_type, conf in final_candidates:
                    if item_type == "company" and not company:
                        company = item_text
                    elif item_type == "university" and not company:
                        company = item_text
                    elif item_type == "job_title" and not title:
                        title = item_text
                    elif item_type == "unknown":
                        # Tiebreakers (Redundant if we did overrides, but safe to keep)
                        if not title and title_hints.search(item_text):
                            title = item_text
                        elif not company and company_hints.search(item_text):
                            company = item_text
                            
                # Context propagation logic...
                if title and not company and context_company:
                     # Check if context_company is actually just the title again (safety)
                     if context_company.lower() != title.lower():
                         company = context_company

                if company:
                    context_company = company

                if title and company:
                    # Construct unique key to avoid sub-div duplicates
                    # Often find_all("div") returns parent AND child. 
                    # Child processing is better (more specific).
                    # We accept the first valid extraction of a specific (Title/Company/Date) tuple.
                    
                    # Normalize simple
                    u_key = f"{title.lower()}|{company.lower()}|{start_d}|{end_d}"
                    if u_key not in seen_entries:
                        parsed.append({
                            "title": title,
                            "company": company,
                            "start": start_d,
                            "end": end_d
                        })
                        seen_entries.add(u_key)

        # Sort by end date descending (most recent first)
        # "Present" jobs should come first, then by year/month
        def experience_sort_key(exp):
            end = exp.get("end", {})
            if end.get("is_present"):
                return (9999, 12)  # Present jobs sort first
            year = end.get("year", 0) or 0
            month = end.get("month", 1) or 1
            return (year, month)
        
        parsed.sort(key=experience_sort_key, reverse=True)
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
                if school_text and 'university' in school_text.lower() or 'college' in school_text.lower():
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
        all_edus = []
        unt_details = None
        
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            link = None
            for a in soup.find_all('a'):
                if 'show all' in a.get_text(strip=True).lower() and 'education' in a.get('href', '').lower():
                    link = a.get('href')
                    break
            
            if not link: return [], None

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

            # Go back
            self.driver.get(profile_url)
            time.sleep(2)

        except Exception as e:
            logger.error(f"Error expanding education: {e}")
        
        return list(dict.fromkeys(all_edus)), unt_details

    # ============================================================
    # Parsing Helpers
    # ============================================================
    def _find_section_root(self, soup, heading_text):
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