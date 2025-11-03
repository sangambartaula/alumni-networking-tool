import os
import sys
import time
import json
import csv
import logging
import random
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, NoSuchWindowException
from bs4 import BeautifulSoup, Tag
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
#print("DEBUG EMAIL:", LINKEDIN_EMAIL)
#print("DEBUG PASSWORD:", LINKEDIN_PASSWORD)
# Set delay based on TESTING mode
if TESTING:
    MIN_DELAY = 15  # 15 seconds for testing
    MAX_DELAY = 60  # 60 seconds for testing
else:
    MIN_DELAY = 120  # 2 minutes for production
    MAX_DELAY = 600  # 10 minutes for production

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
OUTPUT_CSV = OUTPUT_DIR / "UNT_Alumni_Data.csv"
COOKIES_FILE = OUTPUT_DIR / LINKEDIN_COOKIES_PATH

# Correct column names
CSV_COLUMNS = ['name', 'headline', 'location', 'job_title', 'company', 'education', 'major', 'graduation_year', 'profile_url']

logger.info(f"TESTING MODE: {TESTING}")
logger.info(f"DELAY RANGE: {MIN_DELAY}s - {MAX_DELAY}s")

def load_names_from_csv(csv_path):
    """Read people from a CSV file and return a list of dicts with keys:
    { 'name': str, 'degree': str|None, 'graduation_year': str|None }

    The loader supports these input variants (case-insensitive):
    - 'name' column containing full name (e.g., engineering_graduate.csv has 'Name')
    - 'first_name' + 'last_name' columns
    - optional 'degree' or 'Degree' column
    - optional 'semyr' or 'SemYr' column from which a 4-digit year is extracted
    """
    try:
        df = pd.read_csv(csv_path)

        cols_lower = {c.lower(): c for c in df.columns}

        # Build list of raw name strings preserving CSV order
        raw_names = []
        if 'name' in cols_lower:
            col = cols_lower['name']
            raw_names = [str(n).strip() for n in df[col].fillna('').tolist()]
        elif 'first_name' in cols_lower and 'last_name' in cols_lower:
            col_first = cols_lower['first_name']
            col_last = cols_lower['last_name']
            raw_names = [f"{str(r).strip()} {str(l).strip()}".strip() for r, l in zip(df[col_first].fillna(''), df[col_last].fillna(''))]
        else:
            raise ValueError("Input CSV must contain either a 'name' column (case-insensitive) or 'first_name' and 'last_name' columns")

        # Optional degree column
        degree_col = cols_lower.get('degree')
        # Optional sem/term column (SemYr / semyr) which often contains 'Fall 2024' — extract year
        semyr_col = cols_lower.get('semyr') or cols_lower.get('semyr')

        people = []

        def is_likely_person(fullname: str) -> bool:
            if not fullname or len(fullname) < 3:
                return False
            parts = [p for p in fullname.split() if p.strip()]
            if len(parts) < 2:
                return False
            blacklist = {"development","public","administration","applied","artificial","intelligence","finance","student","analysis","behavior","studies","program","major","department","learning","analytics"}
            low = fullname.lower()
            if any(tok in low for tok in blacklist):
                return False
            for part in parts:
                if not any(ch.isalpha() for ch in part):
                    return False
            return True

        for i, raw in enumerate(raw_names):
            name = raw.strip()
            if not is_likely_person(name):
                continue

            degree = None
            if degree_col:
                try:
                    degree = str(df.iloc[i][degree_col]).strip()
                    if degree == 'nan':
                        degree = None
                except Exception:
                    degree = None

            grad_year = None
            if semyr_col:
                try:
                    sem = str(df.iloc[i][semyr_col])
                    m = re.search(r"(\d{4})", sem)
                    if m:
                        grad_year = m.group(1)
                except Exception:
                    grad_year = None

            people.append({
                'name': name,
                'degree': degree,
                'graduation_year': grad_year,
            })

        logger.info(f"Loaded {len(people)} people from {csv_path} (after filtering)")
        return people
    except Exception as e:
        logger.error(f"Failed to read names from {csv_path}: {e}")
        return []


class LinkedInSearchScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.existing_profiles = set()
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
            if OUTPUT_CSV.exists():
                df = pd.read_csv(OUTPUT_CSV)
                if list(df.columns) != CSV_COLUMNS:
                    logger.warning("CSV columns don't match, resetting...")
                    df_new = pd.DataFrame(columns=CSV_COLUMNS)
                    df_new.to_csv(OUTPUT_CSV, index=False)
                    logger.info("✓ CSV reset with correct columns")
            else:
                df = pd.DataFrame(columns=CSV_COLUMNS)
                df.to_csv(OUTPUT_CSV, index=False)
                logger.info("✓ CSV created with correct columns")
        except Exception as e:
            logger.error(f"Error ensuring CSV headers: {e}")
        
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
            if OUTPUT_CSV.exists():
                df = pd.read_csv(OUTPUT_CSV)
                self.existing_profiles = set(df['profile_url'].dropna())
                logger.info(f"Loaded {len(self.existing_profiles)} existing profiles")
            else:
                self.existing_profiles = set()
        except Exception as e:
            logger.error(f"Error loading existing profiles: {e}")
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
            time.sleep(random.uniform(1, 3))  # Random 1-3s between scrolls
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        
        # Scroll back up
        logger.info("Scrolling back up...")
        self.driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
    
    def extract_profile_urls_from_page(self):
        """Extract all profile URLs from current LinkedIn search page (2025 layout)."""
        logger.info("Extracting profile URLs...")

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        # Try multiple patterns, since LinkedIn randomizes container classes
        selectors = [
            "a.app-aware-link[href*='/in/']",                      # modern layout
            "a[href*='/in/']:not([tabindex='-1'])",                # fallback
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

    
    def extract_profile_from_search_result(self, profile_url):
        """
        Extract profile information from a LinkedIn profile (2025 layout).
        Handles name, headline, location, job, company, etc.
        """
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            # Initialize profile data with the URL
            profile_data = {
                "name": "Not Found",
                "headline": "Not Found",
                "location": "Not Found",
                "job_title": "Not Found",
                "company": "Not Found",
                "education": "Not Found",
                "major": "Not Found",
                "graduation_year": "Not Found",
                "profile_url": profile_url
            }

            # Find main profile sections using multiple selectors for 2025 layout
            top_card = soup.find('div', {'class': ['pv-top-card', 'top-card-layout', 'profile-top-card']})
            about_section = soup.find('div', {'id': 'about'}) or soup.find('section', {'id': 'about'}) or \
                          soup.find('div', {'class': 'summary'})
            experience_section = soup.find('div', {'id': 'experience'}) or soup.find('section', {'id': 'experience'}) or \
                               soup.find('section', {'class': 'experience'})
            education_section = soup.find('div', {'id': 'education'}) or soup.find('section', {'id': 'education'})

            # Extract basic info from top card
            if top_card:
                # Name from h1
                name_heading = top_card.find('h1') or top_card.find('div', {'class': ['text-heading-xlarge', 'name']})
                if name_heading:
                    profile_data["name"] = name_heading.get_text(strip=True)

                # Headline - specifically look for text right under the name
                if name_heading:
                    # Method 1: Look at next sibling of name heading
                    next_elem = name_heading.find_next_sibling()
                    if next_elem:
                        headline_text = next_elem.get_text(strip=True)
                        if headline_text and not any(x in headline_text.lower() for x in ['degree connection', 'you both']):
                            profile_data["headline"] = headline_text
                    
                    # Method 2: If not found, try specific headline classes
                    if profile_data["headline"] == "Not Found":
                        headline_selectors = [
                            'div[class*="headline"]',  # Any div with headline in class
                            'div[data-field="headline"]',  # Data field attribute
                            '.pv-text-details__left-panel div.text-body-medium',  # Common headline location
                            '.ph5 div.text-body-medium',  # Another common location
                            '.top-card-layout__headline',  # Direct headline class
                            '.profile-info div.text-body-medium'  # Profile info section
                        ]
                        
                        for selector in headline_selectors:
                            headline_candidates = top_card.select(selector)
                            for elem in headline_candidates:
                                # Make sure it's close to the name (within reasonable DOM distance)
                                if elem.get_text(strip=True) and not any(x in elem.get_text().lower() for x in ['degree connection', 'you both', 'contact info']):
                                    headline_text = elem.get_text(strip=True)
                                    if len(headline_text) > 3:  # Avoid very short texts
                                        profile_data["headline"] = headline_text
                                        break
                            if profile_data["headline"] != "Not Found":
                                break

                # Location - multiple attempts
                location_selectors = [
                    'span.text-body-small.inline.t-black--light.break-words',  # New LinkedIn layout
                    '.profile-info-subheader__secondary-text',  # Student location
                    'span.profile-info-item__secondary-text',  # Another location format
                    'span[class*="location"]',
                    'div[class*="location"]',
                    'span.org-top-card-summary-info-list__info-item',
                    'span.top-card__subline-item',
                    'div.pb2.pv2',  # Common location container
                    'div[data-section="location"]',  # Data attribute selector
                ]
                
                for selector in location_selectors:
                    loc_elems = top_card.select(selector) if top_card else soup.select(selector)
                    for loc_elem in loc_elems:
                        loc_text = loc_elem.get_text(strip=True)
                        # Location validation - look for typical location patterns
                        if ((',' in loc_text or ' Area' in loc_text) and 
                            not any(x in loc_text.lower() for x in ['degree connection', 'you both', 'followers', 'connections'])):
                            profile_data["location"] = loc_text
                            break
                    if profile_data["location"] != "Not Found":
                        break

            # Extract name - try multiple strategies
            try:
                # Try main heading first
                h1 = soup.find('h1')
                if h1:
                    name = h1.get_text(strip=True)
                    if name and len(name.split()) >= 2:  # Ensure it looks like a full name
                        profile_data["name"] = name
                
                # Fallback to meta title if needed
                if profile_data["name"] == "Not Found":
                    meta_title = soup.find('meta', property='og:title')
                    if meta_title and meta_title.get('content'):
                        name = meta_title.get('content').split('|')[0].strip()
                        if name and len(name.split()) >= 2:
                            profile_data["name"] = name
            except Exception as e:
                logger.error(f"Error extracting name: {e}")

            # Extract headline from multiple possible locations
            try:
                # Try headline specific selectors first - focus on text directly under name
                headline_selectors = [
                    '.pv-text-details__left-panel div.text-body-medium',  # Text right under name
                    '.top-card-layout__headline',  # Main profile headline
                    '.text-body-medium.break-words',  # Common headline format
                    '.mt1.t-18.t-black.t-normal.break-words',  # Another headline format
                    'div.pv-text-details__left-panel div.inline-show-more-text',  # Expandable headline
                    '.pv-top-card-section__headline',  # Classic headline location
                    '.ph5.pb5 h2',  # Another headline location
                    'div[data-field="headline"]'  # Data attribute headline
                ]
                
                for selector in headline_selectors:
                    headline_elem = soup.select_one(selector)
                    if headline_elem:
                        headline_text = headline_elem.get_text(strip=True)
                        if headline_text and not any(x in headline_text.lower() for x in ['degree connection', 'you both']):
                            profile_data["headline"] = headline_text
                            break
                
                # Try about section if no headline found
                if profile_data["headline"] == "Not Found" and about_section:
                    about_text = about_section.get_text(strip=True)
                    if about_text:
                        # Take first paragraph or reasonable chunk
                        profile_data["headline"] = ' '.join(about_text.split('\n')[0].split()[:30])

                # Fallback to meta description
                if profile_data["headline"] == "Not Found":
                    meta_desc = soup.find('meta', property='og:description') or soup.find('meta', attrs={'name': 'description'})
                    if meta_desc and meta_desc.get('content'):
                        desc = meta_desc.get('content').strip()
                        if desc:
                            if profile_data["name"] != "Not Found":
                                desc = desc.replace(profile_data["name"], '').strip()
                            profile_data["headline"] = desc.split('\n')[0].strip()

                # Additional cleanup
                if profile_data["headline"] != "Not Found":
                    # Remove connection info if present
                    if "degree connection" in profile_data["headline"].lower():
                        profile_data["headline"] = "Not Found"
                    else:
                        # Clean up and limit length
                        profile_data["headline"] = re.sub(r'^(About|Summary|Profile)\s*:?\s*', '', profile_data["headline"])
                        if len(profile_data["headline"]) > 200:
                            profile_data["headline"] = ' '.join(profile_data["headline"][:200].split()[:-1]) + '...'
            
            except Exception as e:
                logger.error(f"Error extracting headline: {e}")
                profile_data["headline"] = "Not Found"

            # Extract experience info (job title and company)
            try:
                if experience_section:
                    # Multiple selectors for experience items
                    exp_selectors = [
                        'div.experience-item',
                        'li.artdeco-list__item',
                        'div.pv-entity__position-group',
                        'section.experience-section li',
                        'div[class*="experience-item"]',
                        'div[class*="profile-section-card"]',  # New LinkedIn layout
                        'ul.pvs-list > li.artdeco-list__item',  # New experience list
                        'div.pvs-entity'  # Generic entity container
                    ]
                    
                    for selector in exp_selectors:
                        exp_items = experience_section.select(selector) if experience_section else soup.select(selector)
                        if exp_items:
                            for item in exp_items:
                                # Job title - multiple possible locations
                                title_selectors = [
                                    'span[class*="primary-title"]',  # New LinkedIn layout
                                    '.experience-item__title',  # Student/basic profile job title
                                    '.profile-position__title',  # Alternative title format
                                    'span.mr1.t-bold',  # Another common format
                                    'h3.t-bold',
                                    'h3.profile-section-card__title',
                                    'h3[class*="title"]',
                                    'span.title',
                                    'div[class*="title"]',
                                    'div[data-section="position"]',  # Data attribute selector
                                    '.pv-entity__secondary-title',  # Company position/role
                                    '.pv-entity__company-summary-info span:first-child'  # Company title
                                ]
                                
                                for title_sel in title_selectors:
                                    title_elem = item.select_one(title_sel)
                                    if title_elem:
                                        job_text = title_elem.get_text(strip=True)
                                        if job_text and not any(x in job_text.lower() for x in ['degree connection', 'you both']):
                                            profile_data["job_title"] = job_text
                                            break
                                
                                # Company name - multiple possible locations
                                company_selectors = [
                                    'p.company-name',
                                    'span[class*="company"]',
                                    'p.org-name',
                                    'span.pv-entity__secondary-title'
                                ]
                                
                                for company_sel in company_selectors:
                                    company_elem = item.select_one(company_sel)
                                    if company_elem:
                                        company_text = company_elem.get_text(strip=True)
                                        if company_text and not any(x in company_text.lower() for x in ['degree connection', 'you both', 'university', 'college']):
                                            profile_data["company"] = company_text
                                            break
                                
                                if profile_data["job_title"] != "Not Found" and profile_data["company"] != "Not Found":
                                    break  # Found both job title and company
                            
                            if profile_data["job_title"] != "Not Found" and profile_data["company"] != "Not Found":
                                break  # No need to try other selectors
            except Exception as e:
                logger.error(f"Error extracting experience: {e}")

            # Extract education info
            try:
                if education_section:
                    edu_items = education_section.find_all(['li', 'div'], {'class': ['education-item', 'pv-profile-section__list-item']})
                    for item in edu_items:
                        # Look for UNT specifically
                        school_elem = item.find(['h3', 'span'], string=lambda x: x and 'north texas' in x.lower())
                        if school_elem:
                            # Try to find degree type first
                            degree_elem = item.find(['p', 'span'], {'class': ['degree-name', 'field-of-study']})
                            if degree_elem:
                                degree_text = degree_elem.get_text(strip=True)
                                if degree_text and not "degree connection" in degree_text.lower():
                                    # Parse degree text to extract degree type and major
                                    degree_match = re.search(r"(?i)(Bachelor[''']s|Master[''']s|Bachelor|Master|PhD|Doctor|Doctorate)?\s*(of|in|'s degree in)?\s*(Science|Engineering|Arts|Business Administration|Technology|Applied Science|Computer Science|Fine Arts|Education)?\s*(in|,|\s+)?(?:\s+(.+))?", degree_text)
                                    
                                    if degree_match:
                                        # Construct degree (e.g., "Bachelor of Science")
                                        degree_level = degree_match.group(1)
                                        if degree_level:
                                            degree_level = re.sub(r"[''']s", "", degree_level)  # Remove 's from Bachelor's/Master's
                                            if degree_match.group(3):  # Has "of Science/Arts/etc"
                                                profile_data["education"] = f"{degree_level} of {degree_match.group(3)}"
                                            else:
                                                profile_data["education"] = degree_level
                                        
                                        # Get major (after "in" or last part)
                                        major = degree_match.group(5)
                                        if major:
                                            profile_data["major"] = major.strip(', ')
                                        elif degree_match.group(3) and not degree_match.group(5):
                                            # If no separate major but has field (e.g., "Bachelor of Computer Science")
                                            profile_data["major"] = degree_match.group(3)
                                
                                # Try to find graduation year
                                date_elem = item.find(['p', 'span', 'time'], string=lambda x: x and re.search(r'20\d{2}', str(x)))
                                if date_elem:
                                    year_match = re.search(r'20\d{2}', date_elem.get_text())
                                    if year_match:
                                        profile_data["graduation_year"] = year_match.group(0)
                                break  # Stop after finding UNT education
            except Exception as e:
                logger.error(f"Error extracting education: {e}")

            # Clean up headline length if needed
            try:
                if len(profile_data["headline"]) > 200:
                    profile_data["headline"] = ' '.join(profile_data["headline"][:200].split()[:-1]) + '...'
            except Exception as e:
                logger.error(f"Error cleaning up headline: {e}")

            # Extract location
            try:
                location_selectors = [
                    "span.top-card__subline-item",  # 2025 layout
                    "span[class*='location']",
                    ".pb2.pv2",  # Common location container
                    "[data-field='location']"
                ]
                
                for selector in location_selectors:
                    elements = soup.select(selector)
                    for element in elements:
                        text = element.get_text(strip=True)
                        if text and ',' in text and len(text.split()) <= 6:
                            if not any(x in text.lower() for x in ["university", "college", "school"]):
                                profile_data["location"] = text
                                break
                    if profile_data["location"] != "Not Found":
                        break

                # Fallback: look for location in meta tags
                if profile_data["location"] == "Not Found":
                    meta_loc = soup.find('meta', {'name': 'profile:location'})
                    if meta_loc and meta_loc.get('content'):
                        profile_data["location"] = meta_loc.get('content').strip()
            except Exception as e:
                logger.error(f"Error extracting location: {e}")

            # Location: improved pattern matching for 2025 LinkedIn layout
            location = None
            try:
                # Try multiple strategies to find location
                location_patterns = [
                    # Common location containers
                    "div[class*='location']",
                    "span[class*='location']", 
                    "span[class*='geo']",
                    "span[class*='region']",
                    ".top-card__subline-item",  # 2025 layout specific
                    "[data-field='location']",   # Sometimes used for location
                    ".pv-text-details__location",
                    ".pb2.pv2"  # Common container class that often has location
                ]
                
                # Try each pattern
                for pattern in location_patterns:
                    elements = soup.select(pattern)
                    for el in elements:
                        txt = el.get_text(strip=True)
                        if txt and len(txt) < 80:  # Reasonable location length
                            # Check for location patterns (City, State/Province/Country format)
                            if (',' in txt and len(txt.split()) <= 6 and 
                                not any(x in txt.lower() for x in ["university", "college", "school"])):
                                location = txt
                                break
                    if location:
                        break
                        
                # Backup: look for location meta tags
                if not location:
                    meta_loc = soup.find("meta", {"name": "profile:location"})
                    if meta_loc:
                        location = meta_loc.get("content", "").strip()
                        
                # Final backup: scan spans near the top for location-like text
                if not location:
                    top_spans = soup.find_all("span")[:50]  # Check first 50 spans
                    for span in top_spans:
                        txt = span.get_text(strip=True)
                        if (txt and ',' in txt and 
                            len(txt.split()) <= 4 and  # City, State format
                            not any(x in txt.lower() for x in ["university", "college", "school"])):
                            location = txt
                            break
                            
            except Exception as e:
                logger.error(f"Error extracting location: {e}")
                location = None

            # keep name/headline/location in local variables for later return

            # Extract work experience
            try:
                experience_section = soup.find('section', {'id': lambda x: x and 'experience' in x.lower()})
                if experience_section:
                    # Look for current position
                    position_elements = experience_section.find_all(['h3', 'h4', 'div', 'span'])
                    for element in position_elements:
                        text = element.get_text(strip=True)
                        if text:
                            # Check for job title patterns
                            job_keywords = ["engineer", "developer", "analyst", "manager", "researcher", 
                                         "scientist", "specialist", "consultant", "intern", "director"]
                            if any(keyword in text.lower() for keyword in job_keywords):
                                profile_data["job_title"] = text
                                break
                    
                    # Look for company name
                    company_elements = experience_section.find_all(['h4', 'span', 'div'])
                    for element in company_elements:
                        text = element.get_text(strip=True)
                        if text and len(text.split()) <= 4:  # Company names are usually short
                            company_indicators = ["inc", "llc", "ltd", "corp", "technologies", "systems"]
                            if (any(indicator in text.lower() for indicator in company_indicators) or 
                                re.match(r"^[A-Za-z0-9 &.\-]+$", text)):
                                profile_data["company"] = text
                                break
            except Exception as e:
                logger.error(f"Error extracting work experience: {e}")

            # Extract education information
            try:
                education_section = soup.find('section', {'id': lambda x: x and 'education' in x.lower()})
                if education_section:
                    # Look for education details
                    edu_elements = education_section.find_all(['h3', 'span', 'div'])
                    for element in edu_elements:
                        text = element.get_text(strip=True)
                        if text:
                            # Look for degree info
                            degree_pattern = r"(?i)(Bachelor|Master|PhD|Doctor|Associate)(?:\s+of\s+(?:Science|Arts|Engineering))?"
                            match = re.search(degree_pattern, text)
                            if match and profile_data["education"] == "Not Found":
                                profile_data["education"] = text
                            
                            # Look for major
                            major_keywords = ["engineering", "computer science", "technology", 
                                           "mathematics", "physics", "chemistry", "biology"]
                            if any(keyword in text.lower() for keyword in major_keywords) and profile_data["major"] == "Not Found":
                                profile_data["major"] = text
                            
                            # Look for graduation year
                            year_match = re.search(r"20\d{2}", text)
                            if year_match and profile_data["graduation_year"] == "Not Found":
                                profile_data["graduation_year"] = year_match.group(0)
            except Exception as e:
                logger.error(f"Error extracting education info: {e}")

                # Log extracted information
                logger.info(f"  ✓ Name: {profile_data['name']} | Headline: {profile_data['headline']} | Location: {profile_data['location']}")
                logger.info(f"  ✓ Job: {profile_data['job_title']} at {profile_data['company']}")
                logger.info(f"  ✓ Education: {profile_data['education']} in {profile_data['major']} ({profile_data['graduation_year']})")

                return profile_data

            #  If no card matched, save minimal info (profile URL only)
            logger.warning(f" No profile details found for {profile_url}, saving minimal data.")
            return {
                "name": "Not Found",
                "headline": "Not Found",
                "location": "Not Found",
                "job_title": "Not Found",
                "company": "Not Found",
                "education": "Not Found",
                "major": "Not Found",
                "graduation_year": "Not Found",
                "profile_url": profile_url,
            }

        except Exception as e:
            logger.error(f" Error extracting profile data: {e}")
            # Even on fatal parsing error, still return minimal record
            return {
                "name": "Not Found",
                "headline": "Not Found",
                "location": "Not Found",
                "job_title": "Not Found",
                "company": "Not Found",
                "education": "Not Found",
                "major": "Not Found",
                "graduation_year": "Not Found",
                "profile_url": profile_url,
            }

  
    # scrape_profile_in_new_tab is defined later in the class and will be used.
    def scrape_profile_in_new_tab(self, profile_url):
        """Open a LinkedIn profile in a new tab and extract job, company, and education info."""
        main_window = self.driver.current_window_handle
        profile_data = {
            "job_title": "Not Found",
            "company": "Not Found",
            "education": "Not Found",
            "major": "Not Found",
            "graduation_year": "Not Found"
        }

        try:
            # Open new tab
            self.driver.execute_script(f"window.open('{profile_url}', '_blank');")
            time.sleep(2)
            self.driver.switch_to.window(self.driver.window_handles[-1])
            time.sleep(3)

            # Scroll multiple times to ensure all sections load
            for _ in range(6):
                self.driver.execute_script("window.scrollBy(0, 400);")
                time.sleep(random.uniform(0.8, 1.2))

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # 🔹 Extract job and company info
            experience_section = soup.find("section", {"id": lambda x: x and "experience" in x.lower()}) or soup.find("div", {"id": lambda x: x and "experience" in x.lower()})
            
            if experience_section:
                # Look for job title in experience section
                title_spans = experience_section.find_all("span", class_="visually-hidden")
                for span in title_spans:
                    text = span.get_text(strip=True)
                    # Match job title patterns
                    if any(word in text.lower() for word in ["intern", "developer", "engineer", "manager", "analyst", "student", "researcher", "scientist", "lead", "director", "specialist", "consultant"]):
                        profile_data["job_title"] = text
                        break
                
                # Look for company in experience section - prefer spans near job title
                company_spans = experience_section.find_all("span", class_="visually-hidden")
                for span in company_spans:
                    text = span.get_text(strip=True)
                    low = text.lower()
                    # Company indicators
                    company_indicators = ["company", "inc", "llc", "ltd", "corp", "co", "technologies", 
                                       "systems", "solutions", "software", "studio", "group", "consulting"]
                    # First try explicit company names
                    if any(x in low for x in company_indicators):
                        profile_data["company"] = text
                        break
                    # Then try organization-like names (short, alphanumeric with basic punctuation)
                    if (len(text.split()) <= 4 and 
                        re.match(r"^[A-Za-z0-9 &.\-]+$", text) and
                        not any(word in low for word in ["intern", "developer", "engineer", "manager"])):
                        profile_data["company"] = text
                        break

            # 🔹 Extract education info
            edu_section = soup.find_all("span", class_="visually-hidden")
            for span in edu_section:
                text = span.get_text(strip=True)
                if "university" in text.lower() or "college" in text.lower():
                    profile_data["education"] = text
                    break

            # 🔹 Extract graduation year and major (if available)
            date_texts = [span.get_text(strip=True) for span in edu_section if re.search(r"\d{4}", span.get_text())]
            if date_texts:
                match = re.search(r"(\d{4})", date_texts[0])
                if match:
                    profile_data["graduation_year"] = match.group(1)

            major_texts = [span.get_text(strip=True) for span in edu_section if any(x in span.get_text().lower() for x in ["bachelor", "master", "computer", "science", "engineering", "degree", "technology"])]
            if major_texts:
                profile_data["major"] = major_texts[0]

            logger.info(f"    ✓ Job: {profile_data['job_title']} | Company: {profile_data['company']}")
            logger.info(f"    ✓ Education: {profile_data['education']} | Major: {profile_data['major']} | Year: {profile_data['graduation_year']}")

        except Exception as e:
            logger.error(f"Error scraping profile {profile_url}: {e}")

        finally:
            # Close tab and return
            try:
                self.driver.close()
                self.driver.switch_to.window(main_window)
            except:
                pass

        return profile_data

    
    def save_profile(self, profile_data):
        """Save a single profile to CSV"""
        try:
            if not any(profile_data.values()):
                return False

            # Clean and normalize the profile data
            out_row = {}
            for col in CSV_COLUMNS:
                val = profile_data.get(col, "Not Found")
                if val is None or str(val).strip() == "" or str(val).lower() in ['nan', 'none', 'null']:
                    val = "Not Found"
                else:
                    # Clean whitespace and normalize
                    val = re.sub(r"\s+", " ", str(val)).strip()
                out_row[col] = val

            if OUTPUT_CSV.exists():
                try:
                    # Read existing CSV with just our columns
                    existing_df = pd.read_csv(OUTPUT_CSV, encoding='utf-8', usecols=CSV_COLUMNS)
                    # Clean up any null or invalid values
                    existing_df = existing_df.fillna("Not Found")
                    # Remove any rows that are all "Not Found"
                    existing_df = existing_df.replace("Not Found", pd.NA).dropna(how='all').fillna("Not Found")
                except Exception as e:
                    logger.warning(f"Could not read existing CSV ({e}), creating new one")
                    existing_df = pd.DataFrame(columns=CSV_COLUMNS)
            else:
                existing_df = pd.DataFrame(columns=CSV_COLUMNS)

            # If graduation_year looks numeric-like, keep it; otherwise fallback
            if out_row.get('graduation_year') and out_row['graduation_year'] == 'Not Found':
                out_row['graduation_year'] = 'Not Found'
                
            # Ensure we have at least name or profile_url
            if out_row['name'] == "Not Found" and out_row['profile_url'] == "Not Found":
                logger.warning("Profile data missing both name and URL - skipping save")
                return False

            new_row = pd.DataFrame([out_row])
            combined_df = pd.concat([existing_df, new_row], ignore_index=True)

            # Ensure only our columns are present and in the right order
            combined_df = combined_df[CSV_COLUMNS]

            # Clean up the data
            for col in combined_df.columns:
                # Convert to string and clean whitespace
                combined_df[col] = combined_df[col].astype(str).apply(lambda x: x.strip())
                # Replace empty or invalid values
                combined_df[col] = combined_df[col].replace(['', 'nan', 'None', 'NaN', 'NA', 'null'], 'Not Found')

            # Remove duplicates (keep latest entry)
            if 'profile_url' in combined_df.columns and combined_df['profile_url'].notna().any():
                combined_df = combined_df.drop_duplicates(subset=['profile_url'], keep='last')
            else:
                combined_df = combined_df.drop_duplicates(subset=['name','graduation_year'], keep='last')

            logger.info(f"Attempting to save to: {OUTPUT_CSV}")
            logger.info(f"Data to save: {out_row}")
            
            # Write with UTF-8 encoding (no BOM)
            combined_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
            
            if not OUTPUT_CSV.exists():
                logger.error(f"File was not created: {OUTPUT_CSV}")
                return False
                
            logger.info(f"✓ Saved. Total: {len(combined_df)}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving profile: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def wait_between_profiles(self):
        """Wait random time between scraping profiles to avoid detection"""
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        logger.info(f"\n⏳ Waiting {delay:.1f}s before next profile (avoiding detection)...\n")
        
        # Show countdown every 10% of the wait time
        increment = delay / 10
        for i in range(10):
            time.sleep(increment)
            remaining = delay - (increment * (i + 1))
            if remaining > 0:
                logger.info(f"   ...{remaining:.0f}s remaining")
    def run(self):
        """Main scraping loop — updated to read names from CSV"""
        try:
            # Setup browser and load any existing scraped profiles
            self.setup_driver()
            self.load_existing_profiles()

            # Login first
            if not self.login():
                logger.error("Failed to login")
                return

            # Load names and details from the CSV file
            input_csv_path = os.getenv("INPUT_CSV", "backend/engineering_graduate.csv")
            csv_path = Path(__file__).resolve().parent.parent / input_csv_path
            logger.info(f"Reading data from CSV: {csv_path}")

            # Read the input CSV with pandas to preserve all columns
            df = pd.read_csv(csv_path)
            
            # Convert column names to lowercase for case-insensitive matching
            df.columns = [c.lower() for c in df.columns]
            
            # Check for required columns
            required_columns = ['name']  # Only name is absolutely required
            if not all(col in df.columns for col in required_columns):
                logger.error(f"CSV must contain at least these columns: {required_columns}")
                return

            # Convert to list of dictionaries for easier processing
            names = []
            for _, row in df.iterrows():
                entry = {
                    'name': row.get('name', '').strip(),
                    'major': row.get('major', '').strip() if 'major' in row else '',
                    'graduation_year': str(row.get('graduation_year', '')) if 'graduation_year' in row else '',
                    'degree': row.get('degree', '').strip() if 'degree' in row else ''
                }
                if entry['name']:  # Only include entries with a name
                    names.append(entry)

            if not names:
                logger.error("No valid names found in CSV file")
                return

            logger.info(f"Loaded {len(names)} valid entries from CSV")
            profiles_scraped = 0
            
            for name_idx, person in enumerate(names, start=1):
                name = person['name']
                major = person['major']
                year = person['graduation_year']
                degree = person['degree']

                logger.info(f"\n{'='*60}")
                logger.info(f"Processing {name_idx}/{len(names)}: {name}")
                if major: logger.info(f"Major: {major}")
                if year: logger.info(f"Year: {year}")
                if degree: logger.info(f"Degree: {degree}")
                logger.info(f"{'='*60}")


                # Build search query using name and additional details
                query_parts = []
                
                # Add name in quotes for exact match
                query_parts.append(f'"{name}"')
                
                # Add major if available (without quotes to allow partial matches)
                if major and not major.lower().endswith('connection'):
                    query_parts.append(major)
                
                # Add graduation year if available
                if year and str(year).isdigit():
                    query_parts.append(str(year))
                
                # Add UNT as school filter
                school_id = "6464"  # University of North Texas
                
                # Combine all parts and encode
                q = urllib.parse.quote_plus(' '.join(query_parts))
                
                search_url = (
                    f"https://www.linkedin.com/search/results/people/?"
                    f"keywords={q}"
                    f"&schoolFilter=%5B%22{school_id}%22%5D"
                    f"&origin=FACETED_SEARCH"
                )

                logger.info("Loading search results page...")
                self.driver.get(search_url)
                time.sleep(5)

                # Scroll to load all profiles
                self.scroll_full_page()
                profile_urls = self.extract_profile_urls_from_page()
                if not profile_urls:
                    logger.info(f"No profiles found for '{name}'.")
                    continue

                # Limit number of results per name (from .env)
                try:
                    limit = int(os.getenv("RESULTS_PER_SEARCH", "5"))
                except:
                    limit = 5
                profile_urls = profile_urls[:limit]

                logger.info(f"\nProcessing {len(profile_urls)} profiles for '{name}'...\n")

                # Process each found profile
                for idx, profile_url in enumerate(profile_urls, start=1):
                    if profile_url in self.existing_profiles:
                        logger.info(f"[{idx}/{len(profile_urls)}] ⊘ Already scraped: {profile_url}")
                        continue

                    logger.info(f"[{idx}/{len(profile_urls)}] Extracting full profile: {profile_url}")

                    try:
                        # Extract summary info from search list HTML
                        profile_data = self.extract_profile_from_search_result(profile_url)
                        if not profile_data:
                            logger.warning("  Could not extract profile data from search list")
                            continue

                        # Fallback: use searched name if extraction failed
                        if not profile_data.get('name') or profile_data['name'] == "Not Found":
                            profile_data['name'] = name

                        # Open detailed profile in new tab and enrich
                        logger.info("  Opening profile in new tab...")
                        detail_data = self.scrape_profile_in_new_tab(profile_url)
                        profile_data.update(detail_data)
                        profile_data['profile_url'] = profile_url

                        # Always use CSV data for education and major when available
                        csv_deg = person.get('degree')
                        csv_major = person.get('major')

                        if csv_major and not csv_major.endswith('degree connection'):
                            # Use CSV major directly if it's not connection info
                            profile_data['major'] = csv_major
                        
                        if csv_deg and not csv_deg.startswith('You both studied'):
                            # Parse degree info from CSV degree field
                            deg_text = str(csv_deg).strip()
                            edu = None
                            major = None

                            # Try to extract degree type (Bachelor/Master/etc)
                            degree_types = {
                                'bachelor': 'Bachelor of',
                                'master': 'Master of',
                                'phd': 'PhD in',
                                'doctor': 'Doctor of',
                                'associate': 'Associate of'
                            }

                            lower_deg = deg_text.lower()
                            for key, value in degree_types.items():
                                if key in lower_deg:
                                    edu = value
                                    break

                            if not edu:
                                # If no degree type found, use the CSV degree as major
                                if csv_major:
                                    profile_data['major'] = csv_major
                                else:
                                    profile_data['major'] = deg_text
                                profile_data['education'] = 'Bachelor of'  # Default to Bachelor
                            else:
                                # We found a degree type
                                profile_data['education'] = edu.rstrip(' in').rstrip(' of')
                                if csv_major:
                                    profile_data['major'] = csv_major
                                else:
                                    # Try to extract major after "in" or "of"
                                    parts = re.split(r'\s+(?:in|of)\s+', deg_text, flags=re.IGNORECASE)
                                    if len(parts) > 1:
                                        profile_data['major'] = parts[-1].strip()

                        # If detailed scrape didn't populate graduation_year, use CSV value
                        if (not profile_data.get('graduation_year')) or profile_data.get('graduation_year') == 'Not Found':
                            csv_year = person.get('graduation_year')
                            if csv_year:
                                profile_data['graduation_year'] = csv_year

                        
                        jt = profile_data.get('job_title')
                        if jt and jt != "Not Found":
                            low_jt = jt.lower()
                            edu_tokens = ["university","college","bachelor","master","phd","degree","engineering","science","biomedical","bioengineering","major","studies"]
                            if any(tok in low_jt for tok in edu_tokens):
                                # move to headline when headline is missing or generic
                                cur_head = profile_data.get('headline')
                                if not cur_head or cur_head == "Not Found" or cur_head.strip() == "":
                                    profile_data['headline'] = jt
                                # clear job_title since it was actually an education/major
                                profile_data['job_title'] = "Not Found"

                        # Save result
                        if self.save_profile(profile_data):
                            self.existing_profiles.add(profile_url)
                            profiles_scraped += 1

                        # Wait a random delay between profiles (shorter if TESTING=True)
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

                # === End of name loop ===
                logger.info(f"\n{'='*60}\nDone! Total profiles scraped: {profiles_scraped}\nSaved to: {OUTPUT_CSV}\n{'='*60}\n")
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
            
if __name__ == "__main__":
    scraper = LinkedInSearchScraper()
    scraper.run()