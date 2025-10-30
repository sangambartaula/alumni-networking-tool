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
from bs4 import BeautifulSoup
import pandas as pd
import re

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


class LinkedInSearchScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.existing_profiles = set()
        self.ensure_csv_headers()
        
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
        """Extract all profile URLs from current page - ONLY main profile links"""
        logger.info("Extracting profile URLs...")
        
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        profile_urls = []
        
        # Find all LI elements (profile cards)
        profile_cards = soup.find_all('li', {'class': lambda x: x and 'oFSVJaxbBHFfrSHdNWzNIBrfZiDqkXRVqJhws' in (x or '')})
        logger.info(f"Found {len(profile_cards)} profile cards")
        
        for card in profile_cards:
            # Find ONLY the main profile link (the first one in the card that has miniProfileUrn)
            # This is the actual person's profile, not mutual connections
            main_link = card.find('a', {'href': lambda x: x and '/in/' in (x or '') and 'miniProfileUrn' in (x or '')})
            
            if main_link:
                href = main_link.get('href', '')
                profile_url = href.split('?')[0]
                if not profile_url.startswith('http'):
                    profile_url = f"https://www.linkedin.com{profile_url}"
                
                if profile_url not in profile_urls:
                    profile_urls.append(profile_url)
        
        logger.info(f"Extracted {len(profile_urls)} unique URLs")
        return profile_urls
    
    def extract_profile_from_search_result(self, profile_url):
        """Extract basic profile info from search result - match by URL correctly"""
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            # Find all profile cards
            profile_cards = soup.find_all('li', {'class': lambda x: x and 'oFSVJaxbBHFfrSHdNWzNIBrfZiDqkXRVqJhws' in (x or '')})
            
            # Find the card that contains this exact profile URL as main link
            profile_card = None
            for card in profile_cards:
                main_link = card.find('a', {'href': lambda x: x and profile_url in (x or '') and 'miniProfileUrn' in (x or '')})
                if main_link:
                    profile_card = card
                    break
            
            if not profile_card:
                return None
            
            # Extract name from the main link ONLY
            name = "Not Found"
            main_link = profile_card.find('a', {'href': lambda x: x and profile_url in (x or '')})
            if main_link:
                all_text = main_link.get_text().strip()
                name = ' '.join(all_text.split())
                if 'View' in name:
                    name = name.split('View')[0].strip()
                name = name.replace('<!---->', '').strip()
                name = ' '.join(name.split())
            
            if not name or len(name) < 2 or 'Status' in name:
                return None
            
            # Extract headline
            headline = "Not Found"
            all_divs = profile_card.find_all('div')
            for div in all_divs:
                text = div.get_text().strip()
                text = ' '.join(text.split())
                
                if (len(text) > 10 and len(text) < 300 and 
                    any(word in text.lower() for word in ['at', 'student', 'engineer', 'developer', 'manager', 'analyst', 'pursuing', 'graduate', 'undergraduate']) and
                    ',' not in text[:50] and
                    'Status is' not in text and
                    'View' not in text and
                    'Connect' not in text and
                    'mutual' not in text.lower() and
                    len(text) < 200):
                    headline = text
                    break
            
            # Extract location
            location = "Not Found"
            states = ['TX', 'CA', 'NY', 'FL', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI', 'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MD', 'MO', 'WI', 'CO', 'MN', 'SC', 'AL', 'LA', 'KY', 'OR', 'OK', 'CT', 'UT', 'NM', 'NV', 'AR', 'MS', 'KS', 'IA', 'NE', 'ID', 'HI', 'NH', 'ME', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY', 'WV']
            for div in all_divs:
                text = div.get_text().strip()
                text = ' '.join(text.split())
                
                if (',' in text and len(text) < 100 and len(text) > 3 and
                    text.count(',') <= 2 and
                    not any(skip in text for skip in ['Student at', 'at the', 'pursuing', 'View', 'Connect', 'mutual']) and
                    any(state in text for state in states)):
                    location = text
                    break
            
            logger.info(f"  Name: {name} | Headline: {headline} | Location: {location}")
            
            return {
                'name': name,
                'headline': headline,
                'location': location,
                'job_title': "Not Found",
                'company': "Not Found",
                'education': "Not Found",
                'major': "Not Found",
                'graduation_year': "Not Found",
                'profile_url': profile_url
            }
        
        except Exception as e:
            logger.debug(f"Error extracting profile: {e}")
            return None
    
    def scrape_profile_in_new_tab(self, profile_url):
        """Open profile in new tab, scrape it, then close tab"""
        main_window = self.driver.current_window_handle
        
        try:
            # Open in new tab
            self.driver.execute_script(f"window.open('{profile_url}');")
            time.sleep(random.uniform(1, 2))
            
            # Switch to new tab
            self.driver.switch_to.window(self.driver.window_handles[-1])
            time.sleep(3)
            
            # Scroll to load content
            for _ in range(3):
                self.driver.execute_script("window.scrollBy(0, 300);")
                time.sleep(random.uniform(0.5, 1))
            
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            profile_data = {}
            
            # Extract job info
            try:
                exp_section = soup.find('div', {'id': 'experience'})
                if exp_section:
                    job_card = exp_section.find('li')
                    if job_card:
                        title_elem = job_card.find('h3', {'class': lambda x: x and 'title' in (x or '').lower()})
                        if title_elem:
                            profile_data['job_title'] = title_elem.get_text().strip()
                            logger.info(f"    ✓ Job: {profile_data['job_title']}")
                        
                        company_elem = job_card.find('span', {'class': lambda x: x and 'subtitle' in (x or '').lower()})
                        if company_elem:
                            profile_data['company'] = company_elem.get_text().strip()
                            logger.info(f"    ✓ Company: {profile_data['company']}")
            except Exception as e:
                logger.debug(f"Error extracting job: {e}")
            
            # Extract education
            try:
                edu_section = soup.find('div', {'id': 'education'})
                if edu_section:
                    edu_li = edu_section.find('li')
                    if edu_li:
                        edu_heading = edu_li.find('h3')
                        if edu_heading:
                            profile_data['education'] = edu_heading.get_text().strip()
                            logger.info(f"    ✓ Education: {profile_data['education']}")
                        
                        subtitle = edu_li.find('span', {'class': lambda x: x and 'subtitle' in (x or '').lower()})
                        if subtitle:
                            profile_data['major'] = subtitle.get_text().strip()
                            logger.info(f"    ✓ Major: {profile_data['major']}")
                        
                        date_elem = edu_li.find('span', {'class': lambda x: x and 'date' in (x or '').lower()})
                        if date_elem:
                            date_text = date_elem.get_text().strip()
                            year_match = re.search(r'(\d{4})', date_text)
                            if year_match:
                                profile_data['graduation_year'] = year_match.group(1)
                                logger.info(f"    ✓ Year: {profile_data['graduation_year']}")
            except Exception as e:
                logger.debug(f"Error extracting education: {e}")
            
            # Close new tab
            self.driver.close()
            
            # Switch back to main window
            self.driver.switch_to.window(main_window)
            time.sleep(random.uniform(1, 2))
            
            return profile_data
        
        except Exception as e:
            logger.error(f"Error scraping profile in new tab: {e}")
            try:
                self.driver.close()
                self.driver.switch_to.window(main_window)
            except:
                pass
            return {}
    
    def save_profile(self, profile_data):
        """Save a single profile to CSV"""
        try:
            if not any(profile_data.values()):
                return False
            
            if OUTPUT_CSV.exists():
                existing_df = pd.read_csv(OUTPUT_CSV)
            else:
                existing_df = pd.DataFrame(columns=CSV_COLUMNS)
            
            # Fill missing keys
            for col in CSV_COLUMNS:
                if col not in profile_data:
                    profile_data[col] = "Not Found"
            
            new_row = pd.DataFrame([profile_data])[CSV_COLUMNS]
            combined_df = pd.concat([existing_df, new_row], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['profile_url'], keep='first')
            combined_df.to_csv(OUTPUT_CSV, index=False)
            
            logger.info(f"✓ Saved. Total: {len(combined_df)}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving profile: {e}")
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
        """Main scraping loop"""
        try:
            self.setup_driver()
            self.load_existing_profiles()
            
            if not self.login():
                logger.error("Failed to login")
                return
            
            base_search_url = "https://www.linkedin.com/search/results/people/?industry=%5B%226%22%2C%2296%22%2C%224%22%5D&origin=FACETED_SEARCH&schoolFilter=%5B%226464%22%5D"
            
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
                    if profile_url in self.existing_profiles:
                        logger.info(f"[{idx + 1}/{len(profile_urls)}] ⊘ Already scraped")
                        continue
                    
                    logger.info(f"[{idx + 1}/{len(profile_urls)}] Extracting from search...")
                    
                    try:
                        # Extract basic info from search page
                        profile_data = self.extract_profile_from_search_result(profile_url)
                        
                        if not profile_data:
                            logger.warning(f"  Could not extract profile data")
                            continue
                        
                        # Scrape detailed info in new tab
                        logger.info(f"  Opening profile in new tab...")
                        detail_data = self.scrape_profile_in_new_tab(profile_url)
                        profile_data.update(detail_data)
                        
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