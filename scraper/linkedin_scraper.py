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

    raw = raw_title.strip()

    # These should never be treated as job titles
    banned_exact = {
        "Full-time", "Part-time", "Internship", "Contract", "Temporary",
        "Volunteer", "Apprenticeship", "Self-employed", "Freelance"
    }

    # If the job title is ONLY one of these — wipe it
    if raw in banned_exact:
        return ""

    # Remove suffixes like: "Software Engineer · Full-time"
    for bad in banned_exact:
        raw = raw.replace(f"· {bad}", "")
        raw = raw.replace(bad, "")

    # Extra cleanup — remove double spaces
    raw = " ".join(raw.split())

    return raw.strip()


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

# --- VISITED HISTORY CSV FILE ---
# This file will store URLs of everyone visited, even if they were discarded.
# Columns: profile_url, saved (yes/no), visited_at, update_needed, last_db_update
VISITED_HISTORY_FILE = OUTPUT_DIR / "visited_history.csv"
VISITED_HISTORY_COLUMNS = ['profile_url', 'saved', 'visited_at', 'update_needed', 'last_db_update']

# Column names - same for both modes now
CSV_COLUMNS = ['name', 'headline', 'location', 'job_title', 'company', 'education', 'major', 'graduation_year', 'profile_url', 'scraped_at']

logger.info(f"SCRAPER_MODE: {SCRAPER_MODE}")
logger.info(f"TESTING MODE: {TESTING}")
logger.info(f"DELAY RANGE: {MIN_DELAY}s - {MAX_DELAY}s")
logger.info(f"OUTPUT_CSV from .env: {OUTPUT_CSV_ENV}")
logger.info(f"OUTPUT_CSV full path: {OUTPUT_CSV.absolute()}")
logger.info(f"VISITED_HISTORY_FILE: {VISITED_HISTORY_FILE.absolute()}")
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


def get_all_profile_urls_from_db():
    """Get all profile URLs from the database to initialize visited history"""
    try:
        import mysql.connector
        
        conn = mysql.connector.connect(
            host=os.getenv('MYSQLHOST'),
            user=os.getenv('MYSQLUSER'),
            password=os.getenv('MYSQLPASSWORD'),
            database=os.getenv('MYSQL_DATABASE'),
            port=int(os.getenv('MYSQLPORT', 3306))
        )
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT linkedin_url, last_updated
                FROM alumni
                WHERE linkedin_url IS NOT NULL AND linkedin_url != ''
            """)
            
            profiles = cur.fetchall()
        
        conn.close()
        logger.info(f"✓ Retrieved {len(profiles)} profile URLs from database")
        return profiles
    
    except Exception as e:
        logger.error(f"Error fetching profile URLs from database: {e}")
        return []


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
        self.existing_profiles = set()  # From CSV (Successful UNT matches)
        self.visited_history = {}       # From CSV (Everyone we ever visited: {url: {saved, visited_at, update_needed, last_db_update}})
        self.scraper_mode = SCRAPER_MODE
        self.ensure_csv_headers()
        self.db_profile_urls = set()    # All URLs from database
        
    def initialize_visited_history_from_db(self):
        """Load all visited profiles from database and initialize visited history"""
        logger.info("\n📊 Initializing visited history from database...")
        
        # Get ALL visited profiles (UNT and non-UNT) from the new table
        visited_profiles = get_all_visited_profiles()
        
        if not visited_profiles:
            logger. warning("⚠️ No visited profiles found in database")
            # Fall back to loading from CSV if it exists
            self.load_visited_history()
            return
        
        # Parse UPDATE_FREQUENCY to timedelta
        frequency_delta = parse_frequency(UPDATE_FREQUENCY)
        now = datetime.now()
        
        # Build visited history from database
        self.visited_history = {}
        
        for profile in visited_profiles:
            url = profile['linkedin_url']. strip()
            is_unt = profile['is_unt_alum']
            last_checked = profile['last_checked']
            needs_update_db = profile['needs_update']
            
            # Determine if update is needed
            update_needed = 'no'
            if needs_update_db:
                update_needed = 'yes'
            elif is_unt and last_checked:
                # For UNT alumni, check if it's been longer than UPDATE_FREQUENCY
                if isinstance(last_checked, str):
                    try:
                        last_checked_dt = datetime.fromisoformat(last_checked. replace('Z', '+00:00'))
                    except:
                        last_checked_dt = now
                else:
                    last_checked_dt = last_checked
                
                time_since_check = now - last_checked_dt
                if time_since_check > frequency_delta:
                    update_needed = 'yes'
            
            self.visited_history[url] = {
                'saved': 'yes' if is_unt else 'no',
                'visited_at': str(profile['visited_at']) if profile['visited_at'] else '',
                'update_needed': update_needed,
                'last_db_update': str(last_checked) if last_checked else ''
            }
        
        logger.info(f"✓ Loaded {len(self.visited_history)} visited profiles from database")
        
        # Count stats
        unt_count = sum(1 for v in self.visited_history.values() if v['saved'] == 'yes')
        non_unt_count = len(self.visited_history) - unt_count
        logger.info(f"   - UNT Alumni: {unt_count}")
        logger.info(f"   - Non-UNT (skipped): {non_unt_count}")
        
        # Save to CSV as backup
        self._save_visited_history()
    
    def _ensure_visited_history_headers(self):
        """Ensure visited history CSV has correct headers"""
        try:
            if VISITED_HISTORY_FILE.exists():
                try:
                    df = pd.read_csv(VISITED_HISTORY_FILE)
                    if list(df.columns) != VISITED_HISTORY_COLUMNS:
                        logger.warning("Visited history CSV columns don't match, rebuilding...")
                        df_new = pd.DataFrame(columns=VISITED_HISTORY_COLUMNS)
                        df_new.to_csv(VISITED_HISTORY_FILE, index=False)
                        logger.info("✓ Visited history CSV reset with correct columns")
                except Exception as e:
                    logger.warning(f"Visited history CSV is corrupted/empty, rebuilding: {e}")
                    df_new = pd.DataFrame(columns=VISITED_HISTORY_COLUMNS)
                    df_new.to_csv(VISITED_HISTORY_FILE, index=False)
                    logger.info("✓ Visited history CSV rebuilt with correct columns")
            else:
                logger.info(f"📝 Creating new visited history CSV...")
                df = pd.DataFrame(columns=VISITED_HISTORY_COLUMNS)
                df.to_csv(VISITED_HISTORY_FILE, index=False)
                logger.info("✓ Visited history CSV created with correct columns")
        except Exception as e:
            logger.error(f"Error ensuring visited history CSV headers: {e}")

    def load_visited_history(self):
        """Loads the list of URLs we have previously visited from CSV"""
        if VISITED_HISTORY_FILE.exists():
            try:
                df = pd.read_csv(VISITED_HISTORY_FILE)
                self.visited_history = {}
                for _, row in df.iterrows():
                    url = row.get('profile_url', '').strip()
                    if url:
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
            logger.info("📜 No visited history file found.  Will create on first run.")
            self.visited_history = {}

    def _save_visited_history(self):
        """Save visited history to CSV"""
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
            
            df = pd.DataFrame(rows)
            df.to_csv(VISITED_HISTORY_FILE, index=False)
            logger.debug(f"💾 Saved {len(rows)} entries to visited history")
        except Exception as e:
            logger.error(f"Error saving visited history: {e}")

    def mark_as_visited(self, url, saved=False, update_needed=False):
        """Mark a URL as visited with save status - saves to both CSV and DB"""
        if url:
            url = url.strip()
            is_unt_alum = bool(saved)
            
            # Save to database FIRST (source of truth)
            save_visited_profile(url, is_unt_alum=is_unt_alum)
            
            # Then update local cache
            self.visited_history[url] = {
                'saved': 'yes' if saved else 'no',
                'visited_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'update_needed': 'yes' if update_needed else 'no',
                'last_db_update': self.visited_history. get(url, {}).get('last_db_update', '')
            }
            
            # Save to CSV as backup
            self._save_visited_history()
            
            logger.debug(f"📝 Marked as visited: {url} (saved: {saved}, UNT: {is_unt_alum})")
    
    def should_skip_profile(self, url):
        """Check if profile should be skipped based on visited history"""
        if url not in self.visited_history:
            return False
        
        entry = self.visited_history[url]
        saved = entry.get('saved', 'no'). lower()
        update_needed = entry. get('update_needed', 'no').lower()
        
        # If it's a UNT alum AND marked for update, don't skip
        if saved == 'yes' and update_needed == 'yes':
            logger.info(f"    🔄 Re-visiting UNT alum (update needed)")
            return False
        
        # If already saved (UNT alum) and up to date, skip
        if saved == 'yes':
            return True
        
        # If NOT a UNT alum (saved=no), always skip - they weren't UNT before
        logger.debug(f"    ⊘ Skipping non-UNT profile (previously visited)")
        return True

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
        """Extract all profile URLs from the LinkedIn search results page."""
        logger.info("Extracting profile URLs…")

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        profile_urls = set()

        # New 2025 LinkedIn selectors
        selectors = [
            "a.app-aware-link[href*='/in/']",                       # Primary card links
            "a[href*='/in/'][data-view-name='entity_result']",     # New card wrapper
            "a[href*='/in/'][aria-label]",                         # Accessibility labeled profile links
            "a[href*='/in/']:not([tabindex='-1'])"                 # Visible profile links
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

    
    def scrape_profile_page(self, profile_url):
        """Open the full profile page and extract ALL data from there, but only if UNT is in education."""
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
            "all_education": []
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
                
                # Patterns that indicate this is NOT a location
                not_location_patterns = [
                    'university', 'college', 'institute', 'school', 'academy',
                    'inc', 'llc', 'ltd', 'corp', 'corporation', 'company',
                    'technologies', 'solutions', 'services', 'consulting',
                    'group', 'labs', 'laboratory', 'shade', 'dental', 'engineer',
                    'software', 'developer', 'manager', 'director', 'specialist',
                ]
                
                # Metro area indicators
                metro_indicators = ['metroplex', 'metropolitan', 'greater', 'bay area', 'metro']
                
                # Common countries (for international)
                countries = [
                    'India', 'Canada', 'United Kingdom', 'Germany', 'France',
                    'Australia', 'Singapore', 'Japan', 'China', 'Brazil', 'Mexico',
                    'Netherlands', 'Ireland', 'Spain', 'Italy', 'Sweden', 'Switzerland',
                    'Israel', 'United Arab Emirates', 'Saudi Arabia', 'South Korea',
                    'Philippines', 'Indonesia', 'Malaysia', 'Thailand', 'Vietnam',
                    'Pakistan', 'Bangladesh', 'Nigeria', 'South Africa', 'Egypt',
                    'Poland', 'Belgium', 'Austria', 'Denmark', 'Norway', 'Finland',
                    'New Zealand', 'Portugal', 'Greece', 'Czech Republic', 'Romania',
                    'Turkey', 'Argentina', 'Colombia', 'Chile', 'Peru', 'Russia',
                    'Ukraine', 'Kenya', 'Morocco', 'Taiwan', 'Hong Kong',
                ]
                
                def is_valid_location(text):
                    """Check if text is a real location"""
                    if not text:
                        return False
                    text_lower = text.lower()
                    
                    for pattern in not_location_patterns:
                        if pattern in text_lower:
                            return False
                    
                    if any(x in text_lower for x in ['on-site', 'remote', 'hybrid', 'full-time', 'part-time']):
                        return False
                    
                    return True
                
                def is_social_stats(text):
                    """Check if text is social stats"""
                    text_lower = text. lower()
                    return any(x in text_lower for x in ['follower', 'connection', 'mutual', '2nd', '3rd'])
                
                def is_number_like(text):
                    """Check if text looks like a number"""
                    cleaned = text.replace(',', '').replace(' ', '').replace('+', '')
                    return cleaned.isdigit()
                
                all_spans = soup.find_all('span')
                
                # =====================================================
                # METHOD 1: Look for "City, State, United States" pattern
                # (Two commas = most specific US location)
                # =====================================================
                logger.debug("  🔍 Location Method 1: Looking for City, State, Country pattern...")
                
                for span in all_spans:
                    text = span. get_text(strip=True)
                    if not text or len(text) < 10 or len(text) > 100:
                        continue
                    
                    if is_social_stats(text) or is_number_like(text):
                        continue
                    
                    # Must have exactly 2 commas and end with "United States"
                    if text.count(',') == 2 and text.strip().endswith('United States'):
                        if is_valid_location(text):
                            location = text. strip()
                            logger.info(f"  ✓ Location found (Method 1 - City, State, Country): {location}")
                            break
                
                # =====================================================
                # METHOD 2: Look for Metro Area pattern
                # (Contains "Metroplex", "Metropolitan", "Greater", etc.)
                # =====================================================
                if not location:
                    logger.debug("  🔍 Location Method 2: Looking for Metro Area pattern...")
                    
                    for span in all_spans:
                        text = span.get_text(strip=True)
                        if not text or len(text) < 5 or len(text) > 80:
                            continue
                        
                        if is_social_stats(text) or is_number_like(text):
                            continue
                        
                        text_lower = text. lower()
                        
                        # Check if it contains a metro indicator
                        if any(metro in text_lower for metro in metro_indicators):
                            if is_valid_location(text):
                                location = text.strip()
                                logger.info(f"  ✓ Location found (Method 2 - Metro Area): {location}")
                                break
                
                # =====================================================
                # METHOD 3: Look for exact "United States" (country only)
                # =====================================================
                if not location:
                    logger.debug("  🔍 Location Method 3: Looking for exact 'United States'...")
                    
                    for span in all_spans:
                        text = span.get_text(strip=True)
                        
                        if text == 'United States':
                            location = 'United States'
                            logger.info(f"  ✓ Location found (Method 3 - Country only): {location}")
                            break
                
                # =====================================================
                # METHOD 4: Fallback - "State, United States" (1 comma)
                # =====================================================
                if not location:
                    logger.debug("  🔍 Location Method 4: Looking for State, Country pattern...")
                    
                    for span in all_spans:
                        text = span.get_text(strip=True)
                        if not text or len(text) < 5 or len(text) > 80:
                            continue
                        
                        if is_social_stats(text) or is_number_like(text):
                            continue
                        
                        # Must have exactly 1 comma and end with "United States"
                        if text. count(',') == 1 and text. strip().endswith('United States'):
                            if is_valid_location(text):
                                location = text.strip()
                                logger.info(f"  ✓ Location found (Method 4 - State, Country): {location}")
                                break
                
                # =====================================================
                # METHOD 5: International - ends with a country name
                # (e.g., "Karnataka, India" or "London, United Kingdom")
                # =====================================================
                if not location:
                    logger.debug("  🔍 Location Method 5: Looking for international pattern...")
                    
                    for span in all_spans:
                        text = span. get_text(strip=True)
                        if not text or len(text) < 3 or len(text) > 100:
                            continue
                        
                        if is_social_stats(text) or is_number_like(text):
                            continue
                        
                        # Check if text ends with a known country
                        for country in countries:
                            if text.strip().endswith(country):
                                if is_valid_location(text):
                                    location = text.strip()
                                    logger.info(f"  ✓ Location found (Method 5 - International): {location}")
                                    break
                        
                        if location:
                            break
                
                profile_data["location"] = location if location else "Not Found"
                
                if not location:
                    logger.warning(f"  ⚠️ LOCATION NOT FOUND")
                    
            except Exception as e:
                logger. error(f"  ❌ Error extracting location: {e}")
                profile_data["location"] = "Not Found"

            # ===== EXTRACT JOB TITLE AND COMPANY =====
            try:
                job_title = ""
                company = ""
                
                # ============================================
                # COMPANY KEYWORDS - indicates this is a company name
                # ============================================
                company_keywords = [
                    # Legal suffixes
                    'inc', 'inc.', 'incorporated', 'llc', 'l.l.c. ', 'ltd', 'ltd.', 'limited',
                    'corp', 'corp.', 'corporation', 'co. ', 'company', 'companies',
                    'lp', 'l.p.', 'llp', 'l.l.p. ', 'pllc', 'p.l.l.c.', 'pc', 'p. c.',
                    'plc', 'gmbh', 'ag', 's.a.', 'b.v.', 'n.v.', 'pty',
                    # Company type words
                    'group', 'holdings', 'enterprises', 'partners', 'partnership',
                    'associates', 'association', 'firm', 'agency', 'agencies',
                    # Industry indicators
                    'solutions', 'services', 'systems', 'technologies', 'technology',
                    'tech', 'software', 'consulting', 'consultants', 'consultancy',
                    'digital', 'media', 'studios', 'studio', 'labs', 'lab', 'laboratory',
                    'industries', 'industrial', 'manufacturing', 'productions',
                    'healthcare', 'health', 'medical', 'pharma', 'pharmaceutical',
                    'financial', 'finance', 'bank', 'banking', 'insurance', 'capital',
                    'ventures', 'venture', 'investment', 'investments', 'advisors',
                    'retail', 'store', 'stores', 'shop', 'mart', 'market',
                    'logistics', 'transport', 'transportation', 'shipping', 'freight',
                    'construction', 'builders', 'building', 'development', 'developments',
                    'real estate', 'realty', 'properties', 'property',
                    'energy', 'power', 'electric', 'utilities', 'oil', 'gas', 'petroleum',
                    'aerospace', 'aviation', 'airlines', 'airline', 'airways',
                    'automotive', 'auto', 'motors', 'motor', 'cars',
                    'telecommunications', 'telecom', 'communications', 'wireless', 'network', 'networks',
                    'entertainment', 'gaming', 'games', 'sports',
                    'foods', 'food', 'beverage', 'beverages', 'restaurant', 'restaurants', 'dining',
                    'hospitality', 'hotel', 'hotels', 'resort', 'resorts', 'travel',
                    # Education
                    'university', 'college', 'school', 'institute', 'institution',
                    'academy', 'education', 'educational', 'learning',
                    # Government/Non-profit
                    'government', 'federal', 'state', 'city', 'county', 'municipal',
                    'foundation', 'nonprofit', 'non-profit', 'ngo', 'charity',
                    # Other common company words
                    'global', 'international', 'worldwide', 'national', 'american', 'usa',
                    'dental', 'clinic', 'hospital', 'center', 'centre',
                ]
                
                # ============================================
                # JOB TITLE KEYWORDS - indicates this is a job title
                # ============================================
                job_title_keywords = [
                    # Seniority prefixes
                    'senior', 'sr. ', 'sr', 'junior', 'jr. ', 'jr', 'lead', 'principal',
                    'staff', 'chief', 'head', 'associate', 'assistant', 'entry',
                    'executive', 'vp', 'v.p.', 'svp', 'evp', 'avp', 'ceo', 'cto', 'cfo', 'coo', 'cio', 'cmo',
                    
                    # Management titles
                    'manager', 'management', 'director', 'supervisor', 'coordinator',
                    'administrator', 'president', 'vice president', 'officer',
                    'team lead', 'team leader', 'group lead', 'group leader',
                    
                    # Engineering titles
                    'engineer', 'engineering', 'developer', 'programmer', 'coder',
                    'architect', 'devops', 'sre', 'qa', 'qe', 'sdet', 'tester', 'testing',
                    'frontend', 'front-end', 'front end', 'backend', 'back-end', 'back end',
                    'fullstack', 'full-stack', 'full stack', 'software', 'hardware',
                    'embedded', 'firmware', 'mobile', 'web', 'cloud', 'data', 'ml', 'ai',
                    'machine learning', 'artificial intelligence', 'deep learning',
                    'ios', 'android', 'react', 'angular', 'node', 'python', 'java',
                    'database', 'dba', 'sql', 'etl', 'bi', 'business intelligence',
                    'security', 'cybersecurity', 'infosec', 'information security',
                    'network', 'systems', 'system', 'infrastructure', 'platform',
                    'reliability', 'performance', 'automation', 'integration',
                    
                    # Data/Analytics titles
                    'analyst', 'analysis', 'analytics', 'scientist', 'science',
                    'researcher', 'research', 'statistician', 'quantitative',
                    
                    # Design titles
                    'designer', 'design', 'ux', 'ui', 'user experience', 'user interface',
                    'graphic', 'visual', 'creative', 'product',
                    
                    # IT titles
                    'technician', 'specialist', 'support', 'helpdesk', 'help desk',
                    'administrator', 'admin', 'it ', 'information technology',
                    
                    # Project/Product titles
                    'project', 'program', 'product', 'scrum', 'agile', 'owner',
                    'delivery', 'release', 'launch',
                    
                    # Business titles
                    'consultant', 'advisor', 'strategist', 'planner',
                    'accountant', 'accounting', 'auditor', 'finance', 'financial',
                    'marketing', 'sales', 'business', 'operations', 'ops',
                    'recruiter', 'recruiting', 'hr', 'human resources', 'talent',
                    'legal', 'counsel', 'attorney', 'lawyer', 'paralegal',
                    'writer', 'editor', 'content', 'copywriter', 'journalist',
                    'trainer', 'training', 'instructor', 'teacher', 'professor',
                    'nurse', 'doctor', 'physician', 'therapist', 'pharmacist',
                    
                    # Other common job words
                    'intern', 'internship', 'apprentice', 'trainee', 'fellow',
                    'contractor', 'freelance', 'freelancer', 'consultant',
                    'representative', 'rep', 'agent', 'broker',
                    'technologist', 'professional', 'expert', 'guru',
                    'student', 'graduate', 'worker', 'assistant', 'aide',
                ]
                
                # ============================================
                # SKIP PATTERNS - these are NOT job titles or companies
                # ============================================
                skip_patterns = [
                    # Employment type
                    'full-time', 'full time', 'fulltime', 'part-time', 'part time', 'parttime',
                    'contract', 'contractor', 'temporary', 'temp', 'seasonal',
                    'internship', 'apprenticeship', 'freelance', 'self-employed',
                    'volunteer', 'volunteering',
                    
                    # Duration patterns (will also check with regex)
                    'yrs', 'yr', 'years', 'year', 'mos', 'mo', 'months', 'month',
                    'present', 'current',
                    
                    # Work arrangement
                    'remote', 'on-site', 'onsite', 'on site', 'hybrid', 'work from home', 'wfh',
                    
                    # Location indicators (these should be in location field)
                    'united states', 'usa', 'area', 'metropolitan', 'greater',
                ]
                
                def is_skip_text(text):
                    """Check if this text should be skipped (duration, status, description, etc.)"""
                    if not text:
                        return True
                    
                    text_lower = text. lower(). strip()
                    
                    # Skip if too long (likely a job description)
                    if len(text) > 80:
                        return True
                    
                    # Skip if it looks like a sentence/description (has ".  " pattern indicating multiple sentences)
                    if '.  ' in text and len(text) > 50:
                        return True
                    
                    # Skip if it starts with a verb (common in descriptions)
                    description_starters = ['focus on', 'responsible for', 'worked on', 'working on', 
                                            'manage', 'managing', 'led', 'leading', 'develop', 'developing',
                                            'created', 'creating', 'built', 'building', 'designed', 'designing',
                                            'implemented', 'implementing', 'supported', 'supporting']
                    for starter in description_starters:
                        if text_lower.startswith(starter):
                            return True
                    
                    # Skip if matches skip patterns
                    for pattern in skip_patterns:
                        if pattern in text_lower:
                            return True
                    
                    # Skip if it's a date pattern (Jan 2024, 2020 - 2024, etc.)
                    if re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*\d{4}', text_lower):
                        return True
                    if re.search(r'\d{4}\s*[-–—]\s*(present|\d{4})', text_lower):
                        return True
                    
                    # Skip if it's primarily a duration (e.g., "3 yrs 9 mos", "2 years")
                    if re.search(r'^\d+\s*(yrs?|mos?|years? |months?)', text_lower):
                        return True
                    
                    # Skip if it starts with a bullet or special char
                    if text. startswith('·') or text.startswith('•'):
                        return True
                    
                    # Skip if it's just a number or very short
                    if len(text) < 3:
                        return True
                    
                    return False
                
                def is_company(text):
                    """Check if text looks like a company name"""
                    if not text:
                        return False
                    text_lower = text. lower()
                    
                    for keyword in company_keywords:
                        # Check for whole word match or at end of string
                        if keyword in text_lower:
                            return True
                    return False
                
                def is_job_title(text):
                    """Check if text looks like a job title"""
                    if not text:
                        return False
                    text_lower = text.lower()
                    
                    for keyword in job_title_keywords:
                        if keyword in text_lower:
                            return True
                    return False
                
                def clean_company_name(text):
                    """Clean company name - remove duration, status suffixes"""
                    if not text:
                        return ""
                    # Remove " · Full-time", " · 3 yrs 9 mos", etc.
                    cleaned = re.sub(r'\s*·\s*(Full-time|Part-time|Contract|Internship|Temporary|Remote|Hybrid|On-site).*$', '', text, flags=re. IGNORECASE)
                    cleaned = re.sub(r'\s*·\s*\d+\s*(yrs? |mos?|years?|months?).*$', '', cleaned, flags=re. IGNORECASE)
                    return cleaned.strip()
                
                # Find Experience section
                h2_tags = soup.find_all('h2', {'class': lambda x: x and 'pvs-header__title' in (x or '') and 'text-heading-large' in (x or '')})
                
                for h2 in h2_tags:
                    h2_text = h2.get_text(strip=True)
                    if 'Experience' in h2_text:
                        logger.debug("Found Experience section")
                        section = h2. find_parent('section')
                        if section:
                            all_jobs = section.find_all('div', {'data-view-name': 'profile-component-entity'})
                            
                            if all_jobs:
                                first_job = all_jobs[0]
                                spans = first_job. find_all('span', {'aria-hidden': 'true'})
                                
                                # Extract and filter span texts
                                raw_texts = []
                                span_texts = []
                                for s in spans:
                                    text = s. get_text(strip=True). replace('', '').strip()
                                    if text:
                                        raw_texts.append(text)
                                        
                                        # Clean the text first - remove "· Full-time", "· Part-time", etc. 
                                        cleaned = re.sub(r'\s*·\s*(Full-time|Part-time|Contract|Internship|Temporary|Remote|Hybrid|On-site).*$', '', text, flags=re.  IGNORECASE)
                                        cleaned = cleaned.strip()
                                        
                                        if cleaned and not is_skip_text(cleaned):
                                            span_texts.append(cleaned)
                                
                                logger.info(f"  Raw job spans: {raw_texts[:10]}")
                                logger.info(f"  Filtered job spans: {span_texts[:6]}")
                                
                                # Determine job title and company
                                # IMPORTANT: Check job title FIRST - it's more specific
                                found_job_title = None
                                found_company = None
                                
                                for text in span_texts:  # <-- FIXED: was valid_spans
                                    text_is_job = is_job_title(text)
                                    text_is_company = is_company(text)
                                    
                                    logger. debug(f"    '{text[:40]}' → job: {text_is_job}, company: {text_is_company}")
                                    
                                    # If it matches BOTH keywords, prefer job title
                                    # (e.g., "Manufacturing Engineer" matches both but is a job title)
                                    if text_is_job and text_is_company:
                                        if not found_job_title:
                                            found_job_title = clean_job_title(text)
                                            logger.debug(f"    → Matched both, using as job title")
                                        continue
                                    
                                    # Job title only
                                    if text_is_job and not found_job_title:
                                        found_job_title = clean_job_title(text)
                                    
                                    # Company only
                                    elif text_is_company and not found_company:
                                        found_company = clean_company_name(text)
                                    
                                    if found_job_title and found_company:
                                        break
                                
                                # Fallback: if keywords didn't match, use position
                                if not found_job_title and not found_company and len(span_texts) >= 2:  # <-- FIXED
                                    logger. warning(f"  ⚠️ No keyword matches.  Using position fallback.")
                                    found_job_title = clean_job_title(span_texts[0])  # <-- FIXED
                                    found_company = clean_company_name(span_texts[1])  # <-- FIXED
                                elif not found_company and found_job_title and len(span_texts) >= 2:  # <-- FIXED
                                    # Have job but no company - use next span
                                    for text in span_texts:  # <-- FIXED
                                        if text != found_job_title:
                                            found_company = clean_company_name(text)
                                            break
                                elif not found_job_title and found_company and len(span_texts) >= 2:  # <-- FIXED
                                    # Have company but no job - use next span
                                    for text in span_texts:  # <-- FIXED
                                        if text != found_company:
                                            found_job_title = clean_job_title(text)
                                            break
                                
                                # Prevent duplicates
                                if found_job_title and found_company:
                                    if found_job_title.lower().strip() == found_company.lower().strip():
                                        logger.warning(f"  ⚠️ Job equals company '{found_job_title}', clearing company")
                                        found_company = ""
                                
                                job_title = found_job_title or ""
                                company = found_company or ""
                        
                        break
                
                profile_data["job_title"] = job_title
                profile_data["company"] = company
                
                if job_title:
                    logger.debug(f"  ✓ Found job title: {job_title}")
                if company:
                    logger.debug(f"  ✓ Found company: {company}")
                if not job_title and not company:
                    logger.debug(f"  ⚠️  Missing job_title/company")
                    
            except Exception as e:
                logger. debug(f"  ⚠️  Error extracting job: {e}")
                import traceback
                traceback.print_exc()

            # ===== EXTRACT EDUCATION (ALL VISIBLE ENTRIES) =====
            try:
                h2_tags = soup.find_all('h2', {'class': lambda x: x and 'pvs-header__title' in (x or '') and 'text-heading-large' in (x or '')})
                all_education = []
                unt_education_entries = []  # Store all UNT entries with details
                unt_keywords = ["unt", "university of north texas", "north texas"]
                
                # Degree level scoring (higher = better)
                degree_levels = {
                    'ph.d': 100, 'phd': 100, 'doctor': 100, 'doctorate': 100, 'd.phil': 100,
                    'master': 80, 'ms': 80, 'm.s': 80, 'mba': 80, 'm.b.a': 80, 'ma': 80, 'm.a': 80,
                    'bachelor': 60, 'bs': 60, 'b. s': 60, 'ba': 60, 'b.a': 60, 'bba': 60,
                    'associate': 40,
                }
                
                # Engineering-related keywords (gets bonus points)
                engineering_keywords = [
                    'engineering', 'engineer', 'computer science', 'computer engineering',
                    'mechanical', 'electrical', 'civil', 'chemical', 'aerospace',
                    'software', 'hardware', 'materials', 'industrial', 'manufacturing',
                    'biomedical', 'petroleum', 'environmental', 'systems',
                    'technology', 'physics', 'mathematics', 'math',
                    'data science', 'cybersecurity', 'information technology',
                    'electronics', 'robotics', 'mechatronics', 'energy',
                ]
                
                def get_degree_score(text):
                    """Score a degree by level (PhD=100, Masters=80, Bachelors=60)"""
                    if not text:
                        return 0
                    text_lower = text.lower()
                    for keyword, score in degree_levels. items():
                        if keyword in text_lower:
                            return score
                    return 30  # Unknown degree type
                
                def is_engineering_related(text):
                    """Check if degree is engineering-related"""
                    if not text:
                        return False
                    text_lower = text.lower()
                    return any(kw in text_lower for kw in engineering_keywords)
                
                def calculate_education_score(edu_entry):
                    """
                    Calculate score for an education entry. 
                    Higher score = better match.
                    
                    Scoring:
                    - Degree level: +100 (PhD), +80 (Masters), +60 (Bachelors)
                    - Engineering-related: +100 (high priority for College of Engineering)
                    - Has graduation year: +25
                    """
                    score = 0
                    major_text = edu_entry.get('major', '').lower()
                    year = edu_entry.get('graduation_year')
                    
                    # Degree level score
                    score += get_degree_score(major_text)
                    
                    # Engineering bonus (high priority for College of Engineering)
                    if is_engineering_related(major_text):
                        score += 100
                    
                    # Has graduation year bonus
                    if year:
                        score += 25
                    
                    return score
                
                for h2 in h2_tags:
                    h2_text = h2. get_text(strip=True)
                    if 'Education' in h2_text:
                        logger.debug("Found Education section")
                        section = h2. find_parent('section')
                        if section:
                            all_edu_entries = section.find_all('div', {'data-view-name': 'profile-component-entity'})
                            for edu_idx, edu_entry in enumerate(all_edu_entries):
                                spans = edu_entry. find_all('span', {'aria-hidden': 'true'})
                                if len(spans) > 0:
                                    school = spans[0].get_text(strip=True). replace('', '').strip()
                                    if school:
                                        all_education.append(school)
                                        logger.debug(f"  ✓ Found school [{edu_idx + 1}]: {school}")
                                        
                                        # Check if this is UNT
                                        is_unt = any(k in school.lower() for k in unt_keywords)
                                        
                                        # Collect ALL UNT education entries
                                        if is_unt:
                                            unt_entry = {
                                                'school': school,
                                                'major': '',
                                                'graduation_year': '',
                                            }
                                            
                                            # Extract major
                                            if len(spans) > 1:
                                                major = spans[1].get_text(strip=True).replace('', '').strip()
                                                # Skip if it's skills
                                                if major and 'skills' not in major.lower():
                                                    unt_entry['major'] = major
                                            
                                            # Look for dates in spans[2] or beyond
                                            for span_idx in range(2, len(spans)):
                                                span_text = spans[span_idx].get_text(strip=True). replace('', ''). strip()
                                                if re.search(r'\d{4}', span_text):
                                                    year_matches = re.findall(r'\d{4}', span_text)
                                                    if year_matches:
                                                        unt_entry['graduation_year'] = year_matches[-1]
                                                    break
                                            
                                            # Calculate score
                                            unt_entry['score'] = calculate_education_score(unt_entry)
                                            unt_education_entries.append(unt_entry)
                                            logger.debug(f"    UNT Entry: {unt_entry['major']} | Year: {unt_entry['graduation_year']} | Score: {unt_entry['score']}")
                            
                            profile_data["all_education"] = all_education
                            logger.debug(f"  ✓ All education entries: {all_education}")
                        break
                
                # Pick the BEST UNT education entry
                if unt_education_entries:
                    # Sort by score (highest first)
                    unt_education_entries.sort(key=lambda x: x['score'], reverse=True)
                    best = unt_education_entries[0]
                    
                    profile_data["education"] = best['school']
                    profile_data["major"] = best['major']
                    profile_data["graduation_year"] = best['graduation_year']
                    
                    logger.info(f"  ✓ Best UNT education (score={best['score']}): {best['major']} | Year: {best['graduation_year']}")
                    
                    if len(unt_education_entries) > 1:
                        logger.debug(f"  All UNT entries: {[(e['major'], e['score']) for e in unt_education_entries]}")
                
                # --- UNT CHECK (initial education) ---
                found_unt = len(unt_education_entries) > 0
                
                if not found_unt:
                    # Try to expand education section if possible
                    logger. info("    No UNT found in initial education.  Checking for 'View More'...")
                    all_education_expanded, unt_details = self.scrape_all_education(profile_url)
                    if all_education_expanded:
                        all_education = all_education_expanded
                        profile_data["all_education"] = all_education
                        found_unt = any(any(k in (school or '').lower() for k in unt_keywords) for school in all_education)
                        
                        # If we found UNT details from the expanded page, use them! 
                        if unt_details:
                            logger.info(f"    🎓 Applying UNT details from expanded education page")
                            if unt_details. get('education'):
                                profile_data["education"] = unt_details['education']
                            if unt_details.get('major'):
                                profile_data["major"] = unt_details['major']
                            if unt_details.get('graduation_year'):
                                profile_data["graduation_year"] = unt_details['graduation_year']
                            found_unt = True
                            
                if not found_unt:
                    logger.info("    ❌ No UNT education found after expanding.  Skipping profile.")
                    return None
                    
            except Exception as e:
                logger.debug(f"  ⚠️ Error extracting education: {e}")
                import traceback
                traceback.print_exc()

            logger.info(f"    ✓ Name: {profile_data['name']}")
            logger.info(f"    ✓ Headline: {profile_data['headline']}")
            logger.info(f"    ✓ Location: {profile_data['location']}")
            logger.info(f"    ✓ Job: {profile_data['job_title']} @ {profile_data['company']}")
            logger.info(f"    ✓ Education: {profile_data['education']} | Major: {profile_data['major']} | Year: {profile_data['graduation_year']}")
            if len(profile_data['all_education']) > 1:
                logger. info(f"    ✓ All Education: {profile_data['all_education']}")

        except Exception as e:
            logger.error(f"Error scraping profile {profile_url}: {e}")
            import traceback
            traceback.print_exc()

        return profile_data

    def scrape_all_education(self, profile_url):
        """
        For connections mode only: Click 'Show all X educations' link and scrape ALL education entries. 
        Returns tuple: (list of school names, dict of UNT details if found)
        
        UNT details dict: {'education': school_name, 'major': major, 'graduation_year': year}
        """
        all_education = []
        unt_details = None
        unt_keywords = ["unt", "university of north texas", "north texas"]
        
        # Keywords that indicate this is a real educational institution
        education_keywords = [
            'university', 'college', 'institute', 'school', 'academy',
            'polytechnic', 'conservatory', 'seminary', 'of technology',
            'of science', 'of arts', 'of engineering', 'of business',
            'of medicine', 'of law', 'community college', 'state university',
            'technical college', 'vocational'
        ]
        
        try:
            # Look for "Show all X educations" link
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            # Find the "Show all" link for education
            show_all_link = None
            for a in soup.find_all('a'):
                text = a.get_text(strip=True). lower()
                if 'show all' in text and 'education' in text:
                    show_all_link = a. get('href')
                    logger.info(f"    📚 Found 'Show all educations' link")
                    break
            
            if show_all_link:
                # Navigate to the full education page
                if not show_all_link. startswith('http'):
                    show_all_link = f"https://www.linkedin.com{show_all_link}"
                
                logger.info(f"    📚 Opening full education page...")
                self.driver.get(show_all_link)
                time.sleep(3)
                
                # Now scrape all education from this page
                soup = BeautifulSoup(self.driver. page_source, "html.parser")
                
                # IMPORTANT: Only look within the MAIN content area, not the sidebar
                # The main education list is typically in a <main> tag or specific section
                main_content = soup.find('main') or soup.find('section', {'class': lambda x: x and 'pvs-list' in (x or '')})
                
                if not main_content:
                    # Fallback: try to find the education list container
                    main_content = soup. find('div', {'class': lambda x: x and 'scaffold-layout__main' in (x or '')})
                
                if not main_content:
                    logger.warning("    ⚠️ Could not find main content area, using full page (may include sidebar)")
                    main_content = soup
                
                # Find all education entries in the MAIN content only
                edu_entries = main_content. find_all('div', {'data-view-name': 'profile-component-entity'})
                
                logger.debug(f"    Found {len(edu_entries)} potential education entries")
                
                for edu_entry in edu_entries:
                    spans = edu_entry. find_all('span', {'aria-hidden': 'true'})
                    if len(spans) > 0:
                        school = spans[0].get_text(strip=True). replace('', '').strip()
                        
                        if not school:
                            continue
                        
                        # FILTER: Check if this looks like an educational institution
                        school_lower = school. lower()
                        is_education = any(keyword in school_lower for keyword in education_keywords)
                        
                        # Also check if it has degree-related text nearby (Master's, Bachelor's, PhD, etc.)
                        has_degree_info = False
                        if len(spans) > 1:
                            degree_text = spans[1]. get_text(strip=True).lower()
                            degree_keywords = ['degree', 'bachelor', 'master', 'phd', 'doctor', 'associate', 'diploma', 'certificate', 'bs', 'ba', 'ms', 'ma', 'mba']
                            has_degree_info = any(dk in degree_text for dk in degree_keywords)
                        
                        if not is_education and not has_degree_info:
                            logger.debug(f"    ⚠️ Skipping non-education entry: '{school}' (no education keywords or degree info)")
                            continue
                        
                        if school not in all_education:
                            all_education.append(school)
                            logger.debug(f"    ✓ Found school: {school}")
                            
                            # Check if this is UNT and extract details
                            is_unt = any(k in school_lower for k in unt_keywords)
                            if is_unt and unt_details is None:
                                logger.info(f"    🎓 Found UNT in expanded education: {school}")
                                unt_details = {
                                    'education': school,
                                    'major': '',
                                    'graduation_year': ''
                                }
                                
                                # Extract major (usually in spans[1])
                                if len(spans) > 1:
                                    major = spans[1].get_text(strip=True).replace('', '').strip()
                                    if major:
                                        unt_details['major'] = major
                                        logger.info(f"    🎓 UNT Major: {major}")
                                
                                # Extract graduation year from remaining spans
                                for span_idx in range(2, len(spans)):
                                    span_text = spans[span_idx].get_text(strip=True).replace('', '').strip()
                                    # Check if this span contains year information
                                    year_matches = re.findall(r'\d{4}', span_text)
                                    if year_matches:
                                        # Use the LAST year found (end year for date ranges)
                                        final_year = year_matches[-1]
                                        unt_details['graduation_year'] = final_year
                                        logger.info(f"    🎓 UNT Graduation Year: {final_year}")
                                        break
                
                logger.info(f"    📚 Scraped {len(all_education)} valid education entries: {all_education}")
                
                # Go back to main profile
                self.driver. get(profile_url)
                time.sleep(2)
            else:
                logger.debug("    No 'Show all educations' link found")
                
        except Exception as e:
            logger.error(f"    Error scraping all education: {e}")
            import traceback
            traceback.print_exc()
        
        return all_education, unt_details
    
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

            # FINAL CLEANING BEFORE SAVE
            if 'job_title' in save_data:
                 save_data['job_title'] = clean_job_title(save_data['job_title'])

            
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
                logger.error(f"❌ FAILED!  File was not created at {OUTPUT_CSV.absolute()}")
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
            
            # Check if should skip based on update frequency
            if self.should_skip_profile(profile_url):
                logger.info(f"⊘ Profile not marked for update, skipping...")
                continue
            
            try:
                # Scrape the profile
                profile_data = self.scrape_profile_page(profile_url)
                if not profile_data:
                    logger.info("❌ No profile data returned, marking as visited")
                    self.mark_as_visited(profile_url, saved=False, update_needed=False)
                    continue
                    
                profile_data['name'] = full_name
                
                # Save (this will update the existing record)
                if self.save_profile(profile_data):
                    profiles_updated += 1
                    logger.info(f"✅ Updated profile for {full_name}")
                    self.mark_as_visited(profile_url, saved=True, update_needed=False)
                else:
                    self.mark_as_visited(profile_url, saved=False, update_needed=False)
                
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
            self._ensure_visited_history_headers()
            self.load_visited_history()
            self.initialize_visited_history_from_db()

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
                    logger.info("Skipping update.  Running normal scraping mode...\n")
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
                # Check visited history first
                if self.should_skip_profile(profile_url):
                    logger.info(f"[{idx}/{len(profile_urls)}] ⊘ Already processed: {profile_url}")
                    continue

                logger.info(f"[{idx}/{len(profile_urls)}] Extracting full profile: {profile_url}")

                try:
                    # Scrape from the full profile page (not search page)
                    profile_data = self.scrape_profile_page(profile_url)
                    if not profile_data:
                        logger.info("❌ No profile data returned (likely no UNT), marking as visited")
                        self.mark_as_visited(profile_url, saved=False, update_needed=False)
                        continue
                        
                    profile_data['name'] = name  # Set the name from our search query

                    # Save
                    if self.save_profile(profile_data):
                        self.existing_profiles.add(profile_url)
                        profiles_scraped += 1
                        self.mark_as_visited(profile_url, saved=True, update_needed=False)
                    else:
                        self.mark_as_visited(profile_url, saved=False, update_needed=False)

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
        """Search mode: use paginated LinkedIn search with filters (Industries + School + Network)"""
        # URL with filters: Industries, School (UNT), and 3rd+ connections
        base_search_url = "https://www.linkedin.com/search/results/people/?origin=FACETED_SEARCH&network=%5B%22O%22%5D&industry=%5B%221594%22%2C%226%22%2C%2296%22%2C%224%22%2C%22109%22%2C%22118%22%5D&schoolFilter=%5B%226464%22%5D"
        
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
                logger.info("No more profiles. Done!")
                break
            
            logger.info(f"\nProcessing {len(profile_urls)} profiles...\n")
            
            # Process each profile
            for idx, profile_url in enumerate(profile_urls):
                # Check visited history
                if self.should_skip_profile(profile_url):
                    logger.info(f"[{idx + 1}/{len(profile_urls)}] ⊘ Already processed")
                    continue
                
                logger.info(f"[{idx + 1}/{len(profile_urls)}] Scraping profile page...")
                
                try:
                    # Scrape from the full profile page ONLY
                    profile_data = self.scrape_profile_page(profile_url)
                    
                    if not profile_data:
                        logger.info("❌ No profile data returned (likely no UNT), marking as visited")
                        self.mark_as_visited(profile_url, saved=False, update_needed=False)
                        if idx < len(profile_urls) - 1:
                            self.wait_between_profiles()
                        continue
                    
                    # Save
                    if self.save_profile(profile_data):
                        self.existing_profiles.add(profile_url)
                        profiles_scraped += 1
                        self.mark_as_visited(profile_url, saved=True, update_needed=False)
                    else:
                        self.mark_as_visited(profile_url, saved=False, update_needed=False)

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
        Tracks all visited profiles with save status (yes/no).
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
        profiles_skipped_already_processed = 0
        
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
            
            # --- CHECK VISITED HISTORY ---
            if self.should_skip_profile(profile_url):
                logger.info(f"⊘ Already processed, skipping...")
                profiles_skipped_already_processed += 1
                continue
            
            try:
                # Scrape the profile
                profile_data = self.scrape_profile_page(profile_url)
                
                # If scrape_profile_page returned None (no UNT found initially), we still want to log it!
                if not profile_data:
                    logger.info("❌ Profile returned no valid data (likely no UNT). Marking as visited.")
                    self.mark_as_visited(profile_url, saved=False, update_needed=False)
                    profiles_skipped_not_unt += 1
                    continue

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
                    additional_education, unt_details = self.scrape_all_education(profile_url)
                    if additional_education:
                        # Merge with existing education list
                        existing_edu = profile_data.get('all_education', [])
                        for school in additional_education:
                            if school not in existing_edu:
                                existing_edu.append(school)
                        profile_data['all_education'] = existing_edu
                        logger.info(f"    ✓ Total education entries after expansion: {profile_data['all_education']}")
                        
                        # Apply UNT details if found
                        if unt_details:
                            logger.info(f"    🎓 Applying UNT details from expanded education page")
                            if unt_details. get('education'):
                                profile_data["education"] = unt_details['education']
                            if unt_details. get('major'):
                                profile_data["major"] = unt_details['major']
                            if unt_details. get('graduation_year'):
                                profile_data["graduation_year"] = unt_details['graduation_year']
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
                    profile_data['job_title'] = clean_job_title(csv_position)
                
                if is_unt_alum:
                    # Save the profile
                    if self.save_profile(profile_data):
                        self.existing_profiles.add(profile_url)
                        profiles_scraped += 1
                        logger.info(f"✅ UNT Alum!  Saved: {full_name}")
                else:
                    profiles_skipped_not_unt += 1
                    logger.info(f"❌ Not a UNT alum (Education: {profile_data.get('all_education', []) or profile_data.get('education', 'N/A')}), skipping...")
                    self.mark_as_visited(profile_url, saved=False, update_needed=False)
                
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
        logger.info(f"Skipped (already scraped): {profiles_skipped_already_processed}")
        logger.info(f"Results saved to: {OUTPUT_CSV}")
        logger.info(f"{'='*60}\n")


if __name__ == "__main__":
    scraper = LinkedInSearchScraper()
    scraper.run()