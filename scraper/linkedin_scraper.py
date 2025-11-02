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
    """Read a list of names (column 'name') from a CSV file."""
    try:
        df = pd.read_csv(csv_path)
        if 'name' not in df.columns:
            raise ValueError("Input CSV must contain a 'name' column")
        names = [str(n).strip() for n in df['name'].dropna().unique() if str(n).strip()]
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
        Extract name, headline, and location from a LinkedIn search result (2025 layout).
        Always saves at least the profile URL, even if other info is missing.
        """
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Main result cards (LinkedIn 2025 search layout)
            cards = soup.find_all("li", class_="reusable-search__result-container")

            # Iterate through cards
            for card in cards:
                a_tag = card.find("a", href=True)
                if not a_tag or "/in/" not in a_tag["href"]:
                    continue

                full_url = a_tag["href"].split("?")[0]
                if not full_url.startswith("http"):
                    full_url = "https://www.linkedin.com" + full_url

                # Match the correct card for this profile
                if not profile_url.endswith(full_url.split("/")[-2]):
                    continue

                # Extract name
                name_tag = card.select_one("span[dir='auto'], span.entity-result__title-text")
                name = name_tag.get_text(strip=True) if name_tag else "Not Found"

                # Extract headline
                headline_tag = card.select_one(
                    "div.entity-result__primary-subtitle, div.t-14.t-normal"
                )
                headline = headline_tag.get_text(strip=True) if headline_tag else "Not Found"

                # Extract location
                loc_tag = card.select_one(
                    "div.entity-result__secondary-subtitle, div.t-12.t-normal.t-black--light"
                )
                location = loc_tag.get_text(strip=True) if loc_tag else "Not Found"

                logger.info(f"  ✓ Name: {name} | Headline: {headline} | Location: {location}")

                return {
                    "name": name,
                    "headline": headline,
                    "location": location,
                    "job_title": headline if headline != "Not Found" else "Not Found",
                    "company": "Not Found",
                    "education": "Not Found",
                    "major": "Not Found",
                    "graduation_year": "Not Found",
                    "profile_url": profile_url,
                }

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

            # 🔹 Extract current job info
            job_section = soup.find_all("span", class_="visually-hidden")
            for span in job_section:
                text = span.get_text(strip=True)
                if any(word in text.lower() for word in ["intern", "developer", "engineer", "manager", "analyst", "student", "researcher"]):
                    profile_data["job_title"] = text
                    break

            # 🔹 Extract company name
            for span in job_section:
                text = span.get_text(strip=True)
                if "at " in text.lower() and len(text.split()) <= 6:
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
        """Main scraping loop — updated to read names from CSV"""
        try:
            # Setup browser and load any existing scraped profiles
            self.setup_driver()
            self.load_existing_profiles()

            # Login first
            if not self.login():
                logger.error("Failed to login")
                return

            # === NEW: Load names from CSV and search per name ===
            input_csv_path = os.getenv("INPUT_CSV", "names_to_search.csv")
            names = load_names_from_csv(Path(__file__).resolve().parent.parent / input_csv_path)
            if not names:
                logger.error("No names found to search. Add rows to names_to_search.csv under column 'name'.")
                return

            profiles_scraped = 0

            for name_idx, name in enumerate(names, start=1):
                logger.info(f"\n{'='*60}\nNAME {name_idx}/{len(names)}: {name}\n{'='*60}")

                # Build LinkedIn people search URL for this name
                #q = urllib.parse.quote_plus(f'"{name}"')
                #search_url = f"https://www.linkedin.com/search/results/people/?keywords={q}&origin=GLOBAL_SEARCH_HEADER"
                #q = urllib.parse.quote_plus(f'"{name}"')
                #school_id = "6464"  # University of North Texas
                #search_url = (
                #f"https://www.linkedin.com/search/results/people/?"
                #f"keywords={q}"
                #f"&schoolFilter=%5B%22{school_id}%22%5D"
                #f"&network=%5B%22F%22%2C%22S%22%5D"
                #f"&origin=FACETED_SEARCH"
                #f"&title=alumni"
                #)

                school_id = "6464"  # University of North Texas
                q = urllib.parse.quote_plus(f'"{name}"')

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