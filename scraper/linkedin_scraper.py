import os
import sys
import time
import json
import csv
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
from selenium.common.exceptions import NoSuchElementException, TimeoutException, NoSuchWindowException
from bs4 import BeautifulSoup
import pandas as pd
import re
import urllib

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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

# Set delay based on TESTING mode
if TESTING:
    MIN_DELAY = 15  # 15 seconds for testing
    MAX_DELAY = 60  # 60 seconds for testing
else:
    MIN_DELAY = 120  # 2 minutes for production
    MAX_DELAY = 120  # 10 minutes for production

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
OUTPUT_CSV = OUTPUT_DIR / OUTPUT_CSV_ENV
COOKIES_FILE = OUTPUT_DIR / LINKEDIN_COOKIES_PATH

# Column names - same for both modes now
CSV_COLUMNS = ['name', 'headline', 'location', 'job_title', 'company', 'education', 'major', 'graduation_year', 'profile_url', 'scraped_at']

logger.info(f"SCRAPER_MODE: {SCRAPER_MODE}")
logger.info(f"TESTING MODE: {TESTING}")
logger.info(f"DELAY RANGE: {MIN_DELAY}s - {MAX_DELAY}s")
logger.info(f"OUTPUT_CSV from .env: {OUTPUT_CSV_ENV}")
logger.info(f"OUTPUT_CSV full path: {OUTPUT_CSV.absolute()}")
logger.info(f"OUTPUT_DIR: {OUTPUT_DIR.absolute()}")
logger.info(f"OUTPUT_DIR exists: {OUTPUT_DIR.exists()}")


def parse_frequency(frequency_str):
    """Parse frequency string like '6 months', '1 year', '2 years' into a timedelta"""
    try:
        parts = frequency_str.strip().lower().split()
        if len(parts) != 2:
            logger.warning(f"Invalid frequency format: {frequency_str}. Using default 6 months.")
            return timedelta(days=180)
        
        amount = int(parts[0])
        unit = parts[1].rstrip('s')  # Remove trailing 's' for consistency
        
        if unit == "day":
            return timedelta(days=amount)
        elif unit == "month":
            return timedelta(days=amount * 30)  # Approximate
        elif unit == "year":
            return timedelta(days=amount * 365)
        else:
            logger.warning(f"Unknown time unit: {unit}. Using default 6 months.")
            return timedelta(days=180)
    except Exception as e:
        logger.warning(f"Error parsing frequency: {e}. Using default 6 months.")
        return timedelta(days=180)


def get_outdated_profiles():
    """Get alumni profiles that need updating based on UPDATE_FREQUENCY"""
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


def load_names_from_csv(csv_path):
    """Read a list of names (column 'name') from a CSV file."""
    try:
        df = pd.read_csv(csv_path)

        # Support multiple input formats:
        # - 'name' column containing full name
        # - 'first_name' + 'last_name' columns produced by the PDF reader
        if 'name' in df.columns:
            names = [str(n).strip() for n in df['name'].dropna().unique() if str(n).strip()]
        elif 'first_name' in df.columns and 'last_name' in df.columns:
            names = [f"{str(r).strip()} {str(l).strip()}".strip() for r, l in zip(df['first_name'].fillna(''), df['last_name'].fillna('')) if (str(r).strip() or str(l).strip())]
            # dedupe while preserving order
            seen = set()
            uniq = []
            for n in names:
                if n not in seen:
                    seen.add(n)
                    uniq.append(n)
            names = uniq
        else:
            raise ValueError("Input CSV must contain either a 'name' column or 'first_name' and 'last_name' columns")

        logger.info(f"Loaded {len(names)} names from {csv_path}")
        return names
    except Exception as e:
        logger.error(f"Failed to read names from {csv_path}: {e}")
        return []


class LinkedInSearchScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.existing_profiles = set()
        self.scraper_mode = SCRAPER_MODE
        self.ensure_csv_headers()

    def safe_get_text(self, selector, parent=None):
        try:
            context = parent if parent else self.driver
            element = context.find_element(By.CSS_SELECTOR, selector)
            return element.text.strip()
        except Exception:
            return ""

    def ensure_csv_headers(self):
        """Ensure CSV has correct headers"""
        try:
            logger.info(f"🔍 Checking CSV headers at: {OUTPUT_CSV.absolute()}")
            
            if OUTPUT_CSV.exists():
                logger.info(f"📄 CSV exists, checking if it's valid...")
                try:
                    df = pd.read_csv(OUTPUT_CSV)
                    logger.info(f"   Current columns: {list(df.columns)}")
                    logger.info(f"   Expected columns: {CSV_COLUMNS}")
                    
                    if list(df.columns) != CSV_COLUMNS:
                        logger.warning("CSV columns don't match, resetting...")
                        df_new = pd.DataFrame(columns=CSV_COLUMNS)
                        df_new.to_csv(OUTPUT_CSV, index=False)
                        logger.info("✓ CSV reset with correct columns")
                    else:
                        logger.info("✓ CSV columns match!")
                except Exception as csv_error:
                    logger.warning(f"CSV is corrupted/empty, rebuilding: {csv_error}")
                    df_new = pd.DataFrame(columns=CSV_COLUMNS)
                    df_new.to_csv(OUTPUT_CSV, index=False)
                    logger.info("✓ CSV rebuilt with correct columns")
            else:
                logger.info(f"📝 CSV doesn't exist, creating new one at: {OUTPUT_CSV.absolute()}")
                df = pd.DataFrame(columns=CSV_COLUMNS)
                df.to_csv(OUTPUT_CSV, index=False)
                logger.info("✓ CSV created with correct columns")
                logger.info(f"   File now exists: {OUTPUT_CSV.exists()}")
        except Exception as e:
            logger.error(f"Error ensuring CSV headers: {e}")
            import traceback
            traceback.print_exc()
        
    def setup_driver(self):
        """Initialize Selenium WebDriver"""
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
        """Load saved cookies if they exist"""
        try:
            if COOKIES_FILE.exists():
                logger.info(f"Loading saved cookies...")
                self.driver.get("https://www.linkedin.com")
                time.sleep(2)
                
                with open(COOKIES_FILE, 'r') as f:
                    cookies = json.load(f)
                
                for cookie in cookies:
                    try:
                        if 'expiry' in cookie:
                            cookie['expiry'] = int(cookie['expiry'])
                        cookie.pop('sameSite', None)
                        self.driver.add_cookie(cookie)
                    except Exception as e:
                        logger.debug(f"Skipped cookie: {e}")
                
                logger.info(f"✓ Loaded {len(cookies)} cookies")
                self.driver.get("https://www.linkedin.com/feed")
                time.sleep(3)
                
                if "feed" in self.driver.current_url:
                    logger.info("✓ Cookie login successful!")
                    return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            logger.warning(f"Error loading cookies: {e}")
            return False
    
    def save_cookies(self):
        """Save cookies for future use"""
        try:
            cookies = self.driver.get_cookies()
            with open(COOKIES_FILE, 'w') as f:
                json.dump(cookies, f, indent=4)
            logger.info(f"✓ Saved {len(cookies)} cookies")
        except Exception as e:
            logger.error(f"Error saving cookies: {e}")
    
    def load_existing_profiles(self):
        """Load existing profiles from CSV"""
        try:
            logger.info(f"📂 Loading existing profiles from: {OUTPUT_CSV.absolute()}")
            if OUTPUT_CSV.exists():
                try:
                    df = pd.read_csv(OUTPUT_CSV)
                    self.existing_profiles = set(df['profile_url'].dropna())
                    logger.info(f"✓ Loaded {len(self.existing_profiles)} existing profiles")
                except Exception as csv_error:
                    logger.warning(f"CSV is corrupted/empty, starting fresh: {csv_error}")
                    self.existing_profiles = set()
            else:
                logger.warning(f"⚠️  CSV file doesn't exist yet: {OUTPUT_CSV.absolute()}")
                self.existing_profiles = set()
        except Exception as e:
            logger.error(f"Error loading existing profiles: {e}")
            import traceback
            traceback.print_exc()
            self.existing_profiles = set()
    
    def login(self):
        """Login to LinkedIn"""
        logger.info("Logging in to LinkedIn...")
        
        if USE_COOKIES and self.load_cookies():
            return True
        
        try:
            self.driver.get("https://www.linkedin.com/login")
            time.sleep(2)
            
            logger.info("Entering email...")
            email_field = self.wait.until(EC.presence_of_element_located((By.ID, "username")))
            email_field.send_keys(LINKEDIN_EMAIL)
            time.sleep(random.uniform(0.5, 1))
            
            logger.info("Entering password...")
            password_field = self.driver.find_element(By.ID, "password")
            password_field.send_keys(LINKEDIN_PASSWORD)
            time.sleep(random.uniform(0.5, 1))
            
            logger.info("Submitting login...")
            password_field.send_keys(Keys.RETURN)
            
            self.wait.until(EC.url_contains("feed"))
            time.sleep(3)
            
            logger.info("✓ Logged in successfully")
            self.save_cookies()
            return True
        
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False
    
    def scroll_full_page(self):
        """Scroll down to bottom, then back up to load all content"""
        logger.info("Scrolling down to load all profiles...")
        
        # Scroll down
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        for _ in range(10):
            self.driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(random.uniform(1, 3))
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        
        # Scroll back up
        logger.info("Scrolling back up...")
        self.driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
    
    def extract_profile_urls_from_page(self):
        """Extract all profile URLs from current page"""
        logger.info("Extracting profile URLs...")
        
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        
        if self.scraper_mode == "names":
            # Names mode: use simple selectors
            selectors = [
                "a.app-aware-link[href*='/in/']",
                "a[href*='/in/']:not([tabindex='-1'])",
                "div.entity-result__content a.app-aware-link[href*='/in/']"
            ]
            profile_urls = set()
            for selector in selectors:
                for a in soup.select(selector):
                    href = a.get("href", "").split("?")[0]
                    if href and "/in/" in href:
                        if not href.startswith("http"):
                            href = f"https://www.linkedin.com{href}"
                        profile_urls.add(href)
            logger.info(f"Extracted {len(profile_urls)} unique profile URLs")
            return list(profile_urls)
        else:
            # Search mode: Extract all profile URLs from page
            profile_urls = []
            
            # Find all profile links with miniProfileUrn (these are the main profile links)
            all_profile_links = soup.find_all('a', href=re.compile(r'/in/[a-z0-9-]+.*miniProfileUrn'))
            logger.info(f"Found {len(all_profile_links)} profile links with miniProfileUrn")
            
            for link in all_profile_links:
                href = link.get('href', '').split('?')[0]
                
                if href and '/in/' in href:
                    if not href.startswith('http'):
                        href = f"https://www.linkedin.com{href}"
                    
                    if href not in profile_urls:
                        profile_urls.append(href)
            
            logger.info(f"Extracted {len(profile_urls)} unique profile URLs from page")
            return profile_urls
    
    def scrape_profile_page(self, profile_url):
        """Open the full profile page and extract ALL data from there"""
        profile_data = {
            "name": "",
            "headline": "",
            "location": "",
            "job_title": "",
            "company": "",
            "education": "",
            "major": "",
            "graduation_year": "",
            "profile_url": profile_url,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "all_education": []  # Store all education entries for UNT check
        }

        try:
            logger.info(f"  Opening profile: {profile_url}")
            self.driver.get(profile_url)
            time.sleep(4)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # ===== EXTRACT NAME =====
            try:
                name_elem = soup.find('h1', {'class': lambda x: x and 'text-heading-xlarge' in (x or '')})
                if not name_elem:
                    name_elem = soup.find('h1')
                if name_elem:
                    profile_data["name"] = name_elem.get_text(strip=True)
                else:
                    logger.debug(f"  ⚠️  Failed to get name")
            except Exception as e:
                logger.debug(f"  ⚠️  Error extracting name: {e}")

            # ===== EXTRACT HEADLINE =====
            try:
                headline_elem = soup.find('div', {'class': lambda x: x and 'text-body-medium' in (x or '')})
                if headline_elem:
                    profile_data["headline"] = headline_elem.get_text(strip=True)
                else:
                    logger.debug(f"  ⚠️  Missing headline")
            except Exception as e:
                logger.debug(f"  ⚠️  Error extracting headline: {e}")

            # ===== EXTRACT LOCATION =====
            try:
                location = ""
                
                # Method 1: Look for location in profile header/intro section
                intro_section = soup.find('div', {'class': lambda x: x and 'top-card-layout' in (x or '')})
                if intro_section:
                    small_texts = intro_section.find_all('span', {'class': lambda x: x and 'text-body-small' in (x or '')})
                    for elem in small_texts:
                        text = elem.get_text(strip=True)
                        if text and len(text) > 3 and len(text) < 100:
                            if (',' in text and 
                                not any(conn in text.lower() for conn in ['connection', 'follower', '2nd', 'degree'])):
                                location = text
                                break
                
                # Method 2: Look for location using h2/h3 headers in the profile
                if not location:
                    # Find "Contact info" section which typically contains location
                    all_elements = soup.find_all(['h2', 'h3', 'div'])
                    for i, elem in enumerate(all_elements):
                        text = elem.get_text(strip=True)
                        if 'Contact info' in text or 'contact information' in text.lower():
                            # Location usually follows Contact info
                            # Look in nearby elements
                            parent = elem.find_parent('section') or elem.find_parent('div', {'class': lambda x: x and 'pvs-list' in (x or '')})
                            if parent:
                                location_candidates = parent.find_all('span', string=lambda s: s and ',' in s and len(s) < 100)
                                if location_candidates:
                                    for candidate in location_candidates:
                                        loc_text = candidate.get_text(strip=True)
                                        if not any(skip in loc_text.lower() for skip in ['connection', 'follower', '2nd', 'degree']):
                                            location = loc_text
                                            break
                            if location:
                                break
                
                # Method 3: Search for location pattern across entire profile (fallback)
                if not location:
                    # Look for text with comma that looks like a location
                    all_spans = soup.find_all('span')
                    for span in all_spans:
                        text = span.get_text(strip=True)
                        # Location format: "City, State, Country" or "City, State"
                        if (text and ',' in text and len(text) < 100 and len(text) > 5 and
                            not any(skip in text.lower() for skip in ['connection', 'follower', '2nd', 'degree', 'contact', 'message'])):
                            # Check if it looks like a location (has common state/country words)
                            if any(state in text for state in ['Texas', 'California', 'New York', 'Florida', 'United States', 
                                                               'India', 'Canada', 'UK', 'Illinois', 'Virginia', 'Washington', 
                                                               'Massachusetts', 'Pennsylvania', 'Georgia']):
                                location = text
                                break
                
                profile_data["location"] = location if location else "Not Found"
                if not location:
                    logger.debug(f"  ⚠️  Location not found in profile")
            except Exception as e:
                logger.debug(f"  ⚠️  Error extracting location: {e}")
                profile_data["location"] = "Not Found"

            # ===== EXTRACT JOB TITLE AND COMPANY =====
            try:
                # Find Experience section by looking for h2 with "Experience" text
                h2_tags = soup.find_all('h2', {'class': lambda x: x and 'pvs-header__title' in (x or '') and 'text-heading-large' in (x or '')})
                
                found_experience = False
                for h2 in h2_tags:
                    h2_text = h2.get_text(strip=True)
                    if 'Experience' in h2_text:
                        logger.debug("Found Experience section")
                        # Get the parent section
                        section = h2.find_parent('section')
                        if section:
                            # Find first profile-component-entity
                            first_job = section.find('div', {'data-view-name': 'profile-component-entity'})
                            if first_job:
                                # Get all aria-hidden spans
                                spans = first_job.find_all('span', {'aria-hidden': 'true'})
                                if len(spans) > 0:
                                    job_title = spans[0].get_text(strip=True).replace('<!---->', '').strip()
                                    if job_title:
                                        profile_data["job_title"] = job_title
                                        logger.debug(f"  ✓ Found job title: {job_title}")
                                        found_experience = True
                                
                                if len(spans) > 1:
                                    company = spans[1].get_text(strip=True).replace('<!---->', '').strip()
                                    if company:
                                        profile_data["company"] = company
                                        logger.debug(f"  ✓ Found company: {company}")
                        break
                
                if not found_experience:
                    logger.debug(f"  ⚠️  Missing job_title/company")
            except Exception as e:
                logger.debug(f"  ⚠️  Error extracting job: {e}")
                import traceback
                traceback.print_exc()

            # ===== EXTRACT EDUCATION (ALL VISIBLE ENTRIES) =====
            try:
                h2_tags = soup.find_all('h2', {'class': lambda x: x and 'pvs-header__title' in (x or '') and 'text-heading-large' in (x or '')})
                
                found_education = False
                all_education = []
                
                for h2 in h2_tags:
                    h2_text = h2.get_text(strip=True)
                    if 'Education' in h2_text:
                        logger.debug("Found Education section")
                        section = h2.find_parent('section')
                        if section:
                            # Find ALL education entries, not just the first
                            all_edu_entries = section.find_all('div', {'data-view-name': 'profile-component-entity'})
                            
                            for edu_idx, edu_entry in enumerate(all_edu_entries):
                                spans = edu_entry.find_all('span', {'aria-hidden': 'true'})
                                
                                if len(spans) > 0:
                                    school = spans[0].get_text(strip=True).replace('<!---->', '').strip()
                                    if school:
                                        all_education.append(school)
                                        logger.debug(f"  ✓ Found school [{edu_idx + 1}]: {school}")
                                        
                                        # Use first education as the "primary" one for display
                                        if not found_education:
                                            profile_data["education"] = school
                                            found_education = True
                                            
                                            if len(spans) > 1:
                                                major = spans[1].get_text(strip=True).replace('<!---->', '').strip()
                                                if major:
                                                    profile_data["major"] = major
                                                    logger.debug(f"  ✓ Found major: {major}")
                                            
                                            if len(spans) > 2:
                                                dates_text = spans[2].get_text(strip=True).replace('<!---->', '').strip()
                                                logger.debug(f"  Found dates: {dates_text}")
                                                year_match = re.search(r'(\d{4})\s*$', dates_text)
                                                if year_match:
                                                    profile_data["graduation_year"] = year_match.group(1)
                                                    logger.debug(f"  ✓ Found graduation year: {year_match.group(1)}")
                            
                            # Store all education for UNT checking
                            profile_data["all_education"] = all_education
                            logger.debug(f"  ✓ All education entries: {all_education}")
                        break
                
                if not found_education:
                    logger.debug(f"  ⚠️  Missing education/major/graduation_year")
            except Exception as e:
                logger.debug(f"  ⚠️  Error extracting education: {e}")
                import traceback
                traceback.print_exc()

            logger.info(f"    ✓ Name: {profile_data['name']}")
            logger.info(f"    ✓ Headline: {profile_data['headline']}")
            logger.info(f"    ✓ Location: {profile_data['location']}")
            logger.info(f"    ✓ Job: {profile_data['job_title']} @ {profile_data['company']}")
            logger.info(f"    ✓ Education: {profile_data['education']} | Major: {profile_data['major']} | Year: {profile_data['graduation_year']}")
            if len(profile_data['all_education']) > 1:
                logger.info(f"    ✓ All Education: {profile_data['all_education']}")

        except Exception as e:
            logger.error(f"Error scraping profile {profile_url}: {e}")
            import traceback
            traceback.print_exc()

        return profile_data

    def scrape_all_education(self, profile_url):
        """
        For connections mode only: Click 'Show all X educations' link and scrape ALL education entries. 
        Returns list of all school names.
        """
        all_education = []
        
        try:
            # Look for "Show all X educations" link
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            # Find the "Show all" link for education
            show_all_link = None
            for a in soup.find_all('a'):
                text = a.get_text(strip=True).lower()
                if 'show all' in text and 'education' in text:
                    show_all_link = a.get('href')
                    logger.info(f"    📚 Found 'Show all educations' link")
                    break
            
            if show_all_link:
                # Navigate to the full education page
                if not show_all_link.startswith('http'):
                    show_all_link = f"https://www.linkedin.com{show_all_link}"
                
                logger.info(f"    📚 Opening full education page...")
                self.driver.get(show_all_link)
                time.sleep(3)
                
                # Now scrape all education from this page
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                
                # Find all education entries on this page
                edu_entries = soup.find_all('div', {'data-view-name': 'profile-component-entity'})
                
                for edu_entry in edu_entries:
                    spans = edu_entry.find_all('span', {'aria-hidden': 'true'})
                    if len(spans) > 0:
                        school = spans[0].get_text(strip=True).replace('<!---->', '').strip()
                        if school and school not in all_education:
                            all_education.append(school)
                            logger.debug(f"    ✓ Found school: {school}")
                
                logger.info(f"    📚 Scraped {len(all_education)} education entries from full page: {all_education}")
                
                # Go back to main profile
                self.driver.get(profile_url)
                time.sleep(2)
            else:
                logger.debug("    No 'Show all educations' link found")
                
        except Exception as e:
            logger.error(f"    Error scraping all education: {e}")
        
        return all_education
    
    def save_profile(self, profile_data):
        """Save a single profile to CSV"""
        try:
            logger.debug(f"💾 save_profile() called with data: {profile_data}")
            
            # Check if we have at least a profile_url (required field)
            if not profile_data.get('profile_url'):
                logger.warning("⚠️  No profile_url, skipping save")
                return False
            
            # CRITICAL: Check if we have a name - MUST HAVE NAME to save
            if not profile_data.get('name'):
                logger.debug(f"  ❌ SKIP: No name found for {profile_data.get('profile_url')}")
                return False
            
            # Check if we extracted at least SOME meaningful data
            # (headline OR location OR job_title OR education)
            has_meaningful_data = any([
                profile_data.get('headline'),
                profile_data.get('location'),
                profile_data.get('job_title'),
                profile_data.get('education')
            ])
            
            if not has_meaningful_data:
                logger.debug(f"  ❌ SKIP: No meaningful data extracted for {profile_data.get('name')} ({profile_data.get('profile_url')})")
                return False
            
            logger.debug(f"📁 Checking if CSV exists at: {OUTPUT_CSV.absolute()}")
            if OUTPUT_CSV.exists():
                logger.debug(f"   CSV exists, reading...")
                try:
                    existing_df = pd.read_csv(OUTPUT_CSV)
                    logger.debug(f"   Read {len(existing_df)} existing rows")
                except Exception as csv_error:
                    logger.warning(f"CSV is corrupted/empty, starting fresh: {csv_error}")
                    existing_df = pd.DataFrame(columns=CSV_COLUMNS)
            else:
                logger.debug(f"   CSV doesn't exist, creating new DataFrame")
                existing_df = pd.DataFrame(columns=CSV_COLUMNS)
            
            # Remove all_education from profile_data before saving (it's not in CSV_COLUMNS)
            save_data = {k: v for k, v in profile_data.items() if k in CSV_COLUMNS}
            
            # Fill missing keys
            for col in CSV_COLUMNS:
                if col not in save_data:
                    save_data[col] = ""
            
            logger.debug(f"📝 Creating new row with profile data...")
            new_row = pd.DataFrame([save_data])[CSV_COLUMNS]
            logger.debug(f"   New row: {new_row.to_dict('records')}")
            
            combined_df = pd.concat([existing_df, new_row], ignore_index=True)
            logger.debug(f"   Combined DF has {len(combined_df)} rows")
            
            combined_df = combined_df.drop_duplicates(subset=['profile_url'], keep='first')
            logger.debug(f"   After dedup: {len(combined_df)} rows")
            
            logger.info(f"📤 Writing {len(combined_df)} rows to: {OUTPUT_CSV.absolute()}")
            combined_df.to_csv(OUTPUT_CSV, index=False)
            
            # Verify the file was written
            if OUTPUT_CSV.exists():
                file_size = OUTPUT_CSV.stat().st_size
                logger.info(f"✅ SUCCESS!  Saved to {OUTPUT_CSV.absolute()} (size: {file_size} bytes, rows: {len(combined_df)})")
                return True
            else:
                logger.error(f"❌ FAILED! File was not created at {OUTPUT_CSV.absolute()}")
                return False
        
        except Exception as e:
            logger.error(f"❌ Error saving profile: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def wait_between_profiles(self):
        """Wait random time between scraping profiles to avoid detection"""
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        logger.info(f"\n⏳ Waiting {delay:.1f}s before next profile (avoiding detection)...\n")
        
        increment = delay / 10
        for i in range(10):
            time.sleep(increment)
            remaining = delay - (increment * (i + 1))
            if remaining > 0:
                logger.info(f"   ... {remaining:.0f}s remaining")

    def run_update_mode(self, outdated_profiles):
        """Update mode: Re-scrape outdated alumni profiles"""
        profiles_updated = 0
        
        logger.info(f"Processing {len(outdated_profiles)} outdated profiles...\n")
        
        for idx, profile_info in enumerate(outdated_profiles, start=1):
            profile_url, first_name, last_name, last_updated = profile_info
            full_name = f"{first_name} {last_name}".strip()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"PROFILE {idx}/{len(outdated_profiles)}: {full_name}")
            logger.info(f"Last updated: {last_updated}")
            logger.info(f"{'='*60}")
            
            try:
                # Scrape the profile
                profile_data = self.scrape_profile_page(profile_url)
                profile_data['name'] = full_name
                
                # Save (this will update the existing record)
                if self.save_profile(profile_data):
                    profiles_updated += 1
                    logger.info(f"✅ Updated profile for {full_name}")
                
                # Wait before next profile
                if idx < len(outdated_profiles):
                    self.wait_between_profiles()
            
            except NoSuchWindowException:
                logger.error("  Browser window closed, restarting driver...")
                self.setup_driver()
                if not self.login():
                    logger.error("Failed to login again after browser restart")
                    return
            except Exception as e:
                logger.error(f"  Error updating profile: {e}")
                if idx < len(outdated_profiles):
                    self.wait_between_profiles()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Update Complete!")
        logger.info(f"Total profiles updated: {profiles_updated}/{len(outdated_profiles)}")
        logger.info(f"{'='*60}\n")

    def run(self):
        """Main scraping loop - routes to names, search, or connections mode"""
        try:
            self.setup_driver()
            self.load_existing_profiles()

            if not self.login():
                logger.error("Failed to login")
                return

            # Check for outdated profiles that need updating
            outdated_profiles, cutoff_date = get_outdated_profiles()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 UPDATE CHECK")
            logger.info(f"{'='*60}")
            
            if outdated_profiles:
                logger.info(f"You have {len(outdated_profiles)} alumni records that were last updated")
                logger.info(f"over {UPDATE_FREQUENCY} ago (before {cutoff_date.strftime('%Y-%m-%d')})")
                logger.info(f"{'='*60}\n")
                
                response = input("Would you like to run the scraper to update their info now? (y/n): ").strip().lower()
                
                if response == 'y' or response == 'yes':
                    logger.info(f"\n🔄 Starting update of {len(outdated_profiles)} profiles...\n")
                    self.run_update_mode(outdated_profiles)
                    return
                else:
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
            except:
                pass

    def run_names_mode(self):
        """Names mode: read names from CSV and search for each one"""
        # Load names from input CSV
        input_csv_path = os.getenv("INPUT_CSV", "backend/engineering_graduate.csv")
        full_input_path = Path(__file__).resolve().parent.parent / input_csv_path
        
        names = load_names_from_csv(full_input_path)
        if not names:
            logger.warning(f"\n⚠️  No names found in {input_csv_path}")
            logger.warning("Defaulting to general UNT alumni search mode...\n")
            self.run_search_mode()
            return

        profiles_scraped = 0

        for name_idx, name in enumerate(names, start=1):
            logger.info(f"\n{'='*60}\nNAME {name_idx}/{len(names)}: {name}\n{'='*60}")

            # Build LinkedIn people search URL
            q = urllib.parse.quote_plus(f'"{name}"')
            school_id = "6464"  # University of North Texas
            search_url = (
                f"https://www.linkedin.com/search/results/people/?"
                f"keywords={q}"
                f"&schoolFilter=%5B%22{school_id}%22%5D"
                f"&origin=FACETED_SEARCH"
            )

            logger.info("Loading search results page...")
            self.driver.get(search_url)
            time.sleep(5)

            self.scroll_full_page()
            profile_urls = self.extract_profile_urls_from_page()
            if not profile_urls:
                logger.info(f"No profiles found for '{name}'.")
                continue

            # Limit results per name
            try:
                limit = int(os.getenv("RESULTS_PER_SEARCH", "5"))
            except:
                limit = 5
            profile_urls = profile_urls[:limit]

            logger.info(f"\nProcessing {len(profile_urls)} profiles for '{name}'...\n")

            # Process each profile
            for idx, profile_url in enumerate(profile_urls, start=1):
                if profile_url in self.existing_profiles:
                    logger.info(f"[{idx}/{len(profile_urls)}] ⊘ Already scraped: {profile_url}")
                    continue

                logger.info(f"[{idx}/{len(profile_urls)}] Extracting full profile: {profile_url}")

                try:
                    # Scrape from the full profile page (not search page)
                    profile_data = self.scrape_profile_page(profile_url)
                    profile_data['name'] = name  # Set the name from our search query

                    # Save
                    if self.save_profile(profile_data):
                        self.existing_profiles.add(profile_url)
                        profiles_scraped += 1

                    if idx < len(profile_urls):
                        self.wait_between_profiles()

                except NoSuchWindowException:
                    logger.error("  Browser window closed, restarting driver...")
                    self.setup_driver()
                    if not self.login():
                        logger.error("Failed to login again after browser restart")
                        return
                    self.driver.get(search_url)
                    time.sleep(5)
                except Exception as e:
                    logger.error(f"  Error while processing profile: {e}")
                    if idx < len(profile_urls):
                        self.wait_between_profiles()

        logger.info(f"\n{'='*60}\nDone! Total profiles scraped: {profiles_scraped}\nSaved to: {OUTPUT_CSV}\n{'='*60}\n")

    def run_search_mode(self):
        """Search mode: use paginated LinkedIn search with filters"""
        base_search_url = "https://www.linkedin.com/search/results/people/?industry=%5B%22109%22%2C%22118%22%2C%223%22%2C%223248%22%2C%2251%22%2C%223242%22%2C%223107%22%2C%221594%22%2C%226%22%2C%2296%22%2C%224%22%5D&origin=FACETED_SEARCH&schoolFilter=%5B%226464%22%5D&sid=X)p"
        
        page = 1
        profiles_scraped = 0
        
        while True:
            logger.info(f"\n{'='*60}")
            logger.info(f"PAGE {page}")
            logger.info(f"{'='*60}\n")
            
            if page == 1:
                search_url = base_search_url
            else:
                search_url = f"{base_search_url}&page={page}"
            
            logger.info(f"Loading page...")
            logger.info(f"🔗 SEARCH URL: {search_url}")
            
            self.driver.get(search_url)
            time.sleep(5)
            
            # Scroll full page
            self.scroll_full_page()
            
            # Extract URLs
            profile_urls = self.extract_profile_urls_from_page()
            
            if not profile_urls:
                logger.info("No more profiles.  Done!")
                break
            
            logger.info(f"\nProcessing {len(profile_urls)} profiles...\n")
            
            # Process each profile
            for idx, profile_url in enumerate(profile_urls):
                if profile_url in self.existing_profiles:
                    logger.info(f"[{idx + 1}/{len(profile_urls)}] ⊘ Already scraped")
                    continue
                
                logger.info(f"[{idx + 1}/{len(profile_urls)}] Scraping profile page...")
                
                try:
                    # Scrape from the full profile page ONLY
                    profile_data = self.scrape_profile_page(profile_url)
                    
                    # Save
                    if self.save_profile(profile_data):
                        self.existing_profiles.add(profile_url)
                        profiles_scraped += 1
                    
                    # WAIT AFTER SCRAPING
                    if idx < len(profile_urls) - 1:
                        self.wait_between_profiles()
                
                except NoSuchWindowException:
                    logger.error("  Browser window closed, restarting...")
                    self.setup_driver()
                    if not self.login():
                        logger.error("Failed to login again")
                        return
                    self.driver.get(search_url)
                    time.sleep(5)
                except Exception as e:
                    logger.error(f"  Error: {e}")
                    if idx < len(profile_urls) - 1:
                        self.wait_between_profiles()
            
            page += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Scraping Complete!")
        logger.info(f"Total profiles scraped: {profiles_scraped}")
        logger.info(f"Results saved to: {OUTPUT_CSV}")
        logger.info(f"{'='*60}\n")

    def run_connections_mode(self):
        """Connections mode: read LinkedIn URLs from Connections.csv and scrape each profile. 
        Only saves profiles where the person attended University of North Texas. 
        """
        # Load connections CSV
        connections_csv_path = Path(__file__).resolve().parent.parent / CONNECTIONS_CSV_PATH
        
        logger.info(f"\n{'='*60}")
        logger.info(f"CONNECTIONS MODE")
        logger.info(f"Reading from: {connections_csv_path}")
        logger.info(f"{'='*60}\n")
        
        try:
            # Skip the first 3 rows (notes/headers from LinkedIn export)
            df = pd.read_csv(connections_csv_path, skiprows=3)
        except Exception as e:
            logger.error(f"Failed to read connections CSV: {e}")
            return
        
        # Filter rows that have a valid URL
        df = df.dropna(subset=['URL'])
        df = df[df['URL'].str.contains('linkedin.com/in/', na=False)]
        
        total_connections = len(df)
        logger.info(f"Found {total_connections} connections with valid LinkedIn URLs")
        
        profiles_scraped = 0
        profiles_skipped_not_unt = 0
        profiles_skipped_already_scraped = 0
        
        for idx, row in enumerate(df.iterrows(), start=1):
            row_data = row[1]  # row is (index, Series)
            profile_url = str(row_data.get('URL', '')).strip()
            first_name = str(row_data.get('First Name', '') or '').strip()
            last_name = str(row_data.get('Last Name', '') or '').strip()
            full_name = f"{first_name} {last_name}".strip()
            csv_company = str(row_data.get('Company', '') or '').strip()
            csv_position = str(row_data.get('Position', '') or '').strip()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"CONNECTION {idx}/{total_connections}: {full_name}")
            logger.info(f"URL: {profile_url}")
            logger.info(f"{'='*60}")
            
            # Skip if already scraped
            if profile_url in self.existing_profiles:
                logger.info(f"⊘ Already scraped, skipping...")
                profiles_skipped_already_scraped += 1
                continue
            
            try:
                # Scrape the profile
                profile_data = self.scrape_profile_page(profile_url)
                
                # Check if UNT is already in the visible education entries
                all_education = profile_data.get('all_education', [])
                education_text = ' '.join(all_education).lower()
                primary_education = profile_data.get('education', '').lower()
                combined_education = f"{education_text} {primary_education}"
                
                is_unt_alum = (
                    'university of north texas' in combined_education or
                    ' unt ' in f" {combined_education} " or
                    combined_education.startswith('unt ') or
                    combined_education.endswith(' unt')
                )
                
                # Only click "Show all educations" if UNT is NOT already found
                if not is_unt_alum:
                    additional_education = self.scrape_all_education(profile_url)
                    if additional_education:
                        # Merge with existing education list
                        existing_edu = profile_data.get('all_education', [])
                        for school in additional_education:
                            if school not in existing_edu:
                                existing_edu.append(school)
                        profile_data['all_education'] = existing_edu
                        logger.info(f"    ✓ Total education entries after expansion: {profile_data['all_education']}")
                        
                        # Re-check for UNT with expanded education list
                        all_education = profile_data.get('all_education', [])
                        education_text = ' '.join(all_education).lower()
                        combined_education = f"{education_text} {primary_education}"
                        
                        is_unt_alum = (
                            'university of north texas' in combined_education or
                            ' unt ' in f" {combined_education} " or
                            combined_education.startswith('unt ') or
                            combined_education.endswith(' unt')
                        )
                
                # Use name from CSV if scraping didn't get it
                if not profile_data.get('name'):
                    profile_data['name'] = full_name
                
                # Use CSV data as fallback if scraping didn't get it
                if not profile_data.get('company') and csv_company:
                    profile_data['company'] = csv_company
                if not profile_data.get('job_title') and csv_position:
                    profile_data['job_title'] = csv_position
                
                if is_unt_alum:
                    # Save the profile
                    if self.save_profile(profile_data):
                        self.existing_profiles.add(profile_url)
                        profiles_scraped += 1
                        logger.info(f"✅ UNT Alum!  Saved: {full_name}")
                else:
                    profiles_skipped_not_unt += 1
                    logger.info(f"❌ Not a UNT alum (Education: {profile_data.get('all_education', []) or profile_data.get('education', 'N/A')}), skipping...")
                
                # Wait before next profile
                if idx < total_connections:
                    self.wait_between_profiles()
                
            except NoSuchWindowException:
                logger.error("Browser window closed, restarting driver...")
                self.setup_driver()
                if not self.login():
                    logger.error("Failed to login again after browser restart")
                    return
            except Exception as e:
                logger.error(f"Error processing profile: {e}")
                if idx < total_connections:
                    self.wait_between_profiles()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Connections Mode Complete!")
        logger.info(f"{'='*60}")
        logger.info(f"Total connections processed: {total_connections}")
        logger.info(f"UNT alumni saved: {profiles_scraped}")
        logger.info(f"Skipped (not UNT alumni): {profiles_skipped_not_unt}")
        logger.info(f"Skipped (already scraped): {profiles_skipped_already_scraped}")
        logger.info(f"Results saved to: {OUTPUT_CSV}")
        logger.info(f"{'='*60}\n")


if __name__ == "__main__":
    scraper = LinkedInSearchScraper()
    scraper.run()