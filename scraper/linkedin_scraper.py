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
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from repo root
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
logger.info(f"Loading .env from: {env_path}")

# Configuration
LINKEDIN_EMAIL = os.getenv("LINKEDIN_EMAIL")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")
LINKEDIN_COOKIES_PATH = os.getenv("LINKEDIN_COOKIES_PATH", "linkedin_cookies.json")
USE_COOKIES = os.getenv("USE_COOKIES", "true").lower() == "true"
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
DATA_SOURCE = os.getenv("DATA_SOURCE", "csv")  # "database" or "csv"
INPUT_CSV = os.getenv("INPUT_CSV", "names_to_search.csv")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "UNT_Alumni_Data.csv")
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "2"))
SCROLL_PAUSE_TIME = float(os.getenv("SCROLL_PAUSE_TIME", "2"))
RESULTS_PER_SEARCH = int(os.getenv("RESULTS_PER_SEARCH", "15"))
MAX_PROFILES_PER_DAY = int(os.getenv("MAX_PROFILES_PER_DAY", "100"))  # Start conservative

logger.info(f"DATA_SOURCE: {DATA_SOURCE}")

# Add parent directory to path for database imports
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))
DB_AVAILABLE = False
if DATA_SOURCE.lower() == "database":
    try:
        from database import get_connection
        DB_AVAILABLE = True
    except ImportError:
        DB_AVAILABLE = False
        logger.warning("Database module not available. Using CSV mode only.")
else:
    logger.info("Database disabled - using CSV mode only")

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
OUTPUT_PATH = OUTPUT_DIR / OUTPUT_CSV

# Cookies path - look in scraper directory
COOKIES_PATH = Path(__file__).parent / LINKEDIN_COOKIES_PATH
logger.info(f"Cookies will be loaded from/saved to: {COOKIES_PATH}")


class LinkedInScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.existing_profiles = set()
        self.session_active = False
        self.profiles_scraped_today = 0
        self.last_action_time = time.time()
        
    def setup_driver(self):
        """Initialize Selenium WebDriver with anti-bot measures"""
        logger.info("Setting up Chrome WebDriver with anti-bot measures...")
        chrome_options = webdriver.ChromeOptions()
        
        if HEADLESS:
            chrome_options.add_argument("--headless")
        
        # Anti-bot measures
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")  # Faster loading
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-geolocation")
        chrome_options.add_argument("--disable-media-session")
        chrome_options.add_argument("--disable-sync")
        chrome_options.add_argument("--disable-translate")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-hang-monitor")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-prompt-on-repost")
        chrome_options.add_argument("--enable-automation=false")
        
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        # Randomized user agent - DIFFERENT EACH TIME
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
        ]
        chosen_agent = random.choice(user_agents)
        chrome_options.add_argument(f"user-agent={chosen_agent}")
        logger.info(f"Using user agent: {chosen_agent[:60]}...")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        
        # Inject script to hide automation
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false,
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
            '''
        })
        logger.info("✓ WebDriver initialized with anti-bot measures")
    
    def random_mouse_movement(self):
        """Simulate random mouse movements"""
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            actions = ActionChains(self.driver)
            
            # Random movements
            for _ in range(random.randint(2, 5)):
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                actions.move_by_offset(x, y)
                time.sleep(random.uniform(0.1, 0.3))
            
            actions.perform()
        except:
            pass
    
    def random_delay(self, min_secs=60, max_secs=180):
        """Add random delay between min and max seconds"""
        delay = random.uniform(min_secs, max_secs)
        logger.info(f"⏳ Anti-bot delay: {delay:.1f} seconds ({delay/60:.1f} minutes)")
        
        # Break the delay into chunks with random intervals
        remaining = delay
        while remaining > 0:
            chunk = min(random.uniform(5, 15), remaining)
            time.sleep(chunk)
            remaining -= chunk
            
            # Random mouse movements during delay
            if remaining > 0:
                self.random_mouse_movement()
    
    def human_like_scroll(self):
        """Scroll page in a human-like manner"""
        total_height = self.driver.execute_script("return document.body.scrollHeight")
        current_position = 0
        
        while current_position < total_height:
            # Random scroll amount (human scrolling is irregular)
            scroll_amount = random.randint(200, 600)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            current_position += scroll_amount
            
            # Random pause between scrolls
            time.sleep(random.uniform(0.3, SCROLL_PAUSE_TIME))
            
            # Sometimes scroll back up slightly (human behavior)
            if random.random() < 0.2:
                back_scroll = random.randint(50, 150)
                self.driver.execute_script(f"window.scrollBy(0, -{back_scroll});")
                current_position -= back_scroll
                time.sleep(random.uniform(0.2, 0.5))
            
            total_height = self.driver.execute_script("return document.body.scrollHeight")
    
    def random_typing_delay(self):
        """Add delay between typing characters (simulates human typing)"""
        return random.uniform(0.05, 0.15)
    
    def type_human_like(self, element, text):
        """Type text character by character with human-like delays"""
        for char in text:
            element.send_keys(char)
            time.sleep(self.random_typing_delay())
    
    def load_existing_profiles(self):
        """Load existing profiles from CSV to avoid duplicates"""
        try:
            if OUTPUT_PATH.exists():
                df = pd.read_csv(OUTPUT_PATH)
                self.existing_profiles = set(df['profile_url'].dropna())
                logger.info(f"Loaded {len(self.existing_profiles)} existing profiles from CSV")
            else:
                self.existing_profiles = set()
                logger.info("No existing CSV found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading existing profiles: {e}")
            self.existing_profiles = set()
    
    def save_cookies(self):
        """Save LinkedIn cookies to file"""
        try:
            cookies = self.driver.get_cookies()
            with open(COOKIES_PATH, 'w') as f:
                json.dump(cookies, f, indent=4)
            logger.info(f"Cookies saved to {COOKIES_PATH}")
        except Exception as e:
            logger.error(f"Error saving cookies: {e}")
    
    def load_cookies(self):
        """Load LinkedIn cookies from file and add to driver"""
        if not COOKIES_PATH.exists():
            logger.warning(f"Cookies file not found at {COOKIES_PATH}")
            return False
        
        try:
            logger.info("Navigating to linkedin.com...")
            self.driver.get("https://www.linkedin.com")
            time.sleep(random.uniform(2, 4))
            
            with open(COOKIES_PATH, 'r') as f:
                cookies = json.load(f)
            
            logger.info(f"Loading {len(cookies)} cookies...")
            
            for cookie in cookies:
                try:
                    cookie_copy = cookie.copy()
                    
                    if 'expirationDate' in cookie_copy:
                        cookie_copy['expiry'] = int(cookie_copy.pop('expirationDate'))
                    if 'hostOnly' in cookie_copy:
                        cookie_copy.pop('hostOnly')
                    if 'session' in cookie_copy:
                        cookie_copy.pop('session')
                    if 'storeId' in cookie_copy:
                        cookie_copy.pop('storeId')
                    
                    self.driver.add_cookie(cookie_copy)
                except Exception as e:
                    logger.debug(f"Skipped cookie: {str(e)[:50]}")
            
            logger.info("Refreshing page to apply cookies...")
            self.driver.refresh()
            time.sleep(random.uniform(2, 4))
            
            # Verify login by checking for profile icon
            try:
                self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "global-nav__primary-items")))
                logger.info("✓ Cookies loaded - proceeding with scraping")
                self.session_active = True
                return True
            except TimeoutException:
                logger.warning("Could not verify login with cookies")
                return False
                
        except Exception as e:
            logger.error(f"Error loading cookies: {e}")
            return False
    
    def login(self):
        """Login to LinkedIn (manual as fallback)"""
        if USE_COOKIES and self.load_cookies():
            logger.info("Using saved cookies for authentication")
            return True
        
        logger.info(f"Credentials check - Email: {LINKEDIN_EMAIL if LINKEDIN_EMAIL else 'NOT SET'}, Password: {'SET' if LINKEDIN_PASSWORD else 'NOT SET'}")
        
        if not LINKEDIN_EMAIL or not LINKEDIN_PASSWORD:
            logger.error("❌ LinkedIn email/password not provided in .env")
            return False
        
        logger.info("Performing manual login...")
        try:
            self.driver.get("https://www.linkedin.com/login")
            time.sleep(random.uniform(2, 4))
            
            # Enter email with human-like typing
            email_field = self.wait.until(EC.presence_of_element_located((By.ID, "username")))
            self.type_human_like(email_field, LINKEDIN_EMAIL)
            time.sleep(random.uniform(0.5, 1.5))
            
            # Enter password with human-like typing
            password_field = self.driver.find_element(By.ID, "password")
            self.type_human_like(password_field, LINKEDIN_PASSWORD)
            time.sleep(random.uniform(0.5, 1.5))
            
            # Random pause before submitting
            time.sleep(random.uniform(1, 3))
            
            # Submit
            password_field.send_keys(Keys.RETURN)
            
            # Wait for login to complete
            self.wait.until(EC.url_contains("feed"))
            time.sleep(random.uniform(3, 5))
            
            logger.info("✓ Login successful")
            self.session_active = True
            
            # Save cookies for future use
            self.save_cookies()
            return True
        
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False
    
    def scroll_page(self):
        """Scroll page to load all search results"""
        self.human_like_scroll()
    
    def search_name(self, name):
        """Search for a name on LinkedIn"""
        try:
            # Check daily limit
            if self.profiles_scraped_today >= MAX_PROFILES_PER_DAY:
                logger.warning(f"⚠️ Daily limit reached ({MAX_PROFILES_PER_DAY}). Stopping.")
                return False
            
            # Clear any cached data
            self.driver.execute_script("window.history.replaceState({}, document.title, window.location.pathname);")
            time.sleep(random.uniform(1, 2))
            
            search_url = f"https://www.linkedin.com/search/results/people/?keywords={name}&origin=FACETED_SEARCH&schoolFilter=%5B%226464%22%5D"
            self.driver.get(search_url)
            time.sleep(random.uniform(3, 6))  # Random wait for page load
            
            logger.info(f"Searching for: {name}")
            logger.info(f"Current URL: {self.driver.current_url}")
            self.scroll_page()
            return True
        except Exception as e:
            logger.error(f"Error searching for {name}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def extract_profile_urls(self):
        """Extract profile URLs from search results using BeautifulSoup"""
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            profile_links = set()
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if "linkedin.com/in/" in href:
                    full_url = href if href.startswith("http") else f"https://www.linkedin.com{href}"
                    profile_links.add(full_url)
            
            urls_list = list(profile_links)[:RESULTS_PER_SEARCH]
            logger.info(f"Found {len(urls_list)} profile URLs from search results")
            for idx, url in enumerate(urls_list[:3]):
                logger.info(f"  URL {idx+1}: {url}")
            return urls_list
        except Exception as e:
            logger.error(f"Error extracting profile URLs: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def scrape_profile(self, profile_url):
        """Scrape profile details from LinkedIn profile page"""
        try:
            # Add delay before navigating to new page (5-10 seconds)
            delay = random.uniform(5, 10)
            logger.info(f"⏳ Delay before loading profile: {delay:.1f}s")
            time.sleep(delay)
            
            self.driver.get(profile_url)
            time.sleep(random.uniform(2, 4))  # Page load time
            
            # Scroll to load all content
            self.human_like_scroll()
            time.sleep(random.uniform(1, 2))
            
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            # Extract Name
            name_tag = soup.find('h1', {'class': 'text-heading-xlarge'})
            name = name_tag.get_text().strip() if name_tag else "Not Found"
            
            # Extract Headline (Job Title Description)
            headline = "Not Found"
            headline_tag = soup.find('div', {'class': 'text-body-medium'})
            if headline_tag:
                headline = headline_tag.get_text().strip()
            
            # Extract Location
            location = "Not Found"
            try:
                location_elem = soup.find('span', {'class': 'text-body-small'})
                if location_elem and ',' in location_elem.get_text():
                    location = location_elem.get_text().strip()
            except:
                pass
            
            # Extract Current Job Title & Company
            job_title = "Not Found"
            company = "Not Found"
            try:
                exp_section = soup.find('div', {'id': 'experience'})
                if exp_section:
                    job_card = exp_section.find('li')
                    if job_card:
                        title_elem = job_card.find('h3', {'class': 'base-main-card__title'})
                        job_title = title_elem.get_text().strip() if title_elem else "Not Found"
                        
                        company_elem = job_card.find('span', {'class': 'base-main-card__subtitle'})
                        company = company_elem.get_text().strip() if company_elem else "Not Found"
            except Exception as e:
                logger.debug(f"Error extracting job/company: {e}")
            
            # Extract Education (Major & Graduation Year)
            major = "Not Found"
            graduation_year = "Not Found"
            try:
                edu_section = soup.find('div', {'id': 'education'})
                if edu_section:
                    edu_card = edu_section.find('li')
                    if edu_card:
                        # Get major/field (e.g., "Computer Science")
                        major_elem = edu_card.find('span', {'class': 'base-main-card__subtitle'})
                        major = major_elem.get_text().strip() if major_elem else "Not Found"
                        
                        # Get graduation year
                        date_elem = edu_card.find('span', {'class': 'date-range'})
                        if date_elem:
                            date_text = date_elem.get_text().strip()
                            import re
                            year_match = re.search(r'(\d{4})', date_text)
                            if year_match:
                                graduation_year = year_match.group(1)
            except Exception as e:
                logger.debug(f"Error extracting education: {e}")
            
            return {
                'name': name,
                'headline': headline,
                'location': location,
                'job_title': job_title,
                'company': company,
                'major': major,
                'graduation_year': graduation_year,
                'profile_url': profile_url
            }
        
        except Exception as e:
            logger.error(f"Error scraping profile {profile_url}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_names_from_csv(self):
        """Read names from CSV file"""
        csv_path = Path(__file__).parent / INPUT_CSV
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return []
        
        names = []
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'name' in row:
                        names.append({
                            "id": row.get('id', None),
                            "name": row['name']
                        })
            
            logger.info(f"Retrieved {len(names)} names from CSV")
            return names
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            return []
    
    def get_names(self):
        """Get names based on configured data source"""
        return self.get_names_from_csv()
    
    def save_to_csv(self, profile_data):
        """Save profile data to output CSV"""
        try:
            file_exists = OUTPUT_PATH.exists()
            with open(OUTPUT_PATH, 'a', newline='', encoding='utf-8') as f:
                fieldnames = ['name', 'headline', 'location', 'job_title', 'company', 'major', 'graduation_year', 'profile_url']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(profile_data)
            
            logger.info(f"✓ Saved to CSV: {profile_data['name']}")
            return True
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False
    
    def run(self):
        """Main scraping loop"""
        try:
            self.setup_driver()
            self.load_existing_profiles()
            
            if not self.login():
                logger.error("Failed to authenticate with LinkedIn")
                return
            
            names_to_search = self.get_names()
            if not names_to_search:
                logger.warning("No names to search")
                return
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting scrape for {len(names_to_search)} names")
            logger.info(f"Max profiles per day: {MAX_PROFILES_PER_DAY}")
            logger.info(f"{'='*60}\n")
            
            scraped_count = 0
            skipped_count = 0
            
            for idx, name_entry in enumerate(names_to_search):
                # Check daily limit
                if self.profiles_scraped_today >= MAX_PROFILES_PER_DAY:
                    logger.warning(f"⚠️ Daily limit of {MAX_PROFILES_PER_DAY} reached. Stopping.")
                    break
                
                name = name_entry['name']
                alumni_id = name_entry.get('id')
                
                logger.info(f"\n[{idx+1}/{len(names_to_search)}] --- Processing: {name} ---")
                
                try:
                    if not self.search_name(name):
                        logger.warning(f"Failed to search for {name}")
                        skipped_count += 1
                        continue
                    
                    profile_urls = self.extract_profile_urls()
                    if not profile_urls:
                        logger.warning(f"No results found for {name}")
                        skipped_count += 1
                        continue
                    
                    logger.info(f"Scraping first profile from {len(profile_urls)} results...")
                    
                    # Scrape first result
                    profile_data = self.scrape_profile(profile_urls[0])
                    if not profile_data:
                        logger.warning(f"Could not scrape profile for {name}")
                        skipped_count += 1
                        continue
                    
                    logger.info(f"Scraped profile: {profile_data['name']}")
                    
                    if profile_data['profile_url'] in self.existing_profiles:
                        logger.info(f"Skipping (already saved): {profile_data['profile_url']}")
                        skipped_count += 1
                        continue
                    
                    # Save to CSV
                    self.save_to_csv(profile_data)
                    self.existing_profiles.add(profile_data['profile_url'])
                    self.profiles_scraped_today += 1
                    
                    scraped_count += 1
                    logger.info(f"✓ Successfully scraped {scraped_count} profile(s) so far")
                    logger.info(f"Daily progress: {self.profiles_scraped_today}/{MAX_PROFILES_PER_DAY}")
                    
                    # Anti-bot delay between searches (60-180 seconds)
                    if idx < len(names_to_search) - 1 and self.profiles_scraped_today < MAX_PROFILES_PER_DAY:
                        logger.info("Taking anti-bot delay before next search...")
                        self.random_delay(min_secs=60, max_secs=180)
                    
                except Exception as e:
                    logger.error(f"Error processing {name}: {e}")
                    import traceback
                    traceback.print_exc()
                    skipped_count += 1
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Scraping Complete!")
            logger.info(f"Total scraped: {scraped_count}")
            logger.info(f"Total skipped: {skipped_count}")
            logger.info(f"Daily progress: {self.profiles_scraped_today}/{MAX_PROFILES_PER_DAY}")
            logger.info(f"Results saved to: {OUTPUT_PATH}")
            logger.info(f"{'='*60}\n")
        
        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("✓ WebDriver closed")


if __name__ == "__main__":
    scraper = LinkedInScraper()
    scraper.run()