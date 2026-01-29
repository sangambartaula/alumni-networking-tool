import os
import sys
import time
import random
import urllib.parse
import threading
import pandas as pd
from pathlib import Path

# Add parent directory to path for backend imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

# Local Modules
import config
import utils
import database_handler
from scraper import LinkedInScraper
from config import logger

# ============================================================
# Exit Control (Graceful Shutdown)
# ============================================================
exit_requested = False  # "exit" - finish current profile then stop
force_exit = False      # "force exit" - stop immediately
_exit_listener_active = False

# Track how many profiles were scraped this session (for post-scrape sync)
session_profiles_scraped = 0

def _exit_listener():
    """Background thread that listens for exit commands."""
    global exit_requested, force_exit, _exit_listener_active
    
    logger.info("💡 Type 'exit' to finish current profile and stop, or 'force exit' to stop immediately.")
    
    while _exit_listener_active:
        try:
            user_input = input().strip().lower()
            if user_input == "force exit":
                logger.warning("\n🔴 FORCE EXIT requested. Stopping immediately...")
                force_exit = True
                break
            elif user_input == "exit":
                logger.warning("\n🟡 EXIT requested. Will stop after current profile...")
                exit_requested = True
                break
        except EOFError:
            break
        except Exception:
            break

def start_exit_listener():
    """Start the exit command listener thread."""
    global _exit_listener_active
    _exit_listener_active = True
    listener = threading.Thread(target=_exit_listener, daemon=True)
    listener.start()

def stop_exit_listener():
    """Stop the exit listener."""
    global _exit_listener_active
    _exit_listener_active = False

def should_stop():
    """Check if scraping should stop (force exit or graceful exit after profile)."""
    return exit_requested or force_exit

def check_force_exit():
    """Check if force exit was requested (immediate stop)."""
    return force_exit

# ============================================================
# Helpers
# ============================================================
def wait_between_profiles():
    """Random delay to avoid bot detection. Checks for force exit during wait."""
    global force_exit
    delay = random.uniform(config.MIN_DELAY, config.MAX_DELAY)
    
    logger.info(f"\n⏳ Waiting {delay:.1f}s before next profile...\n")
    
    # Break delay into small chunks and check for force exit
    increment = delay / 10
    for i in range(10):
        if force_exit:
            logger.warning("⚡ Force exit during wait - stopping immediately")
            return
        time.sleep(increment)

# ============================================================
# Mode: NAMES (Search by list of names)
# ============================================================
def run_names_mode(scraper, history_mgr):
    input_csv = os.getenv("INPUT_CSV", os.path.join("backend", "engineering_graduate.csv"))
    csv_path = Path(__file__).resolve().parent.parent / input_csv
    
    logger.info(f"--- MODE: Names (Source: {csv_path}) ---")
    names = utils.load_names_from_csv(csv_path)

    if not names:
        logger.warning(f"No names found in {csv_path}. Switching to Search Mode.")
        run_search_mode(scraper, history_mgr)
        return

    profiles_scraped = 0
    school_id = "6464" # UNT School ID

    for i, name in enumerate(names, start=1):
        # Check for exit request at start of each name
        if should_stop():
            logger.info("🛑 Exit requested. Stopping names mode.")
            return
        
        logger.info(f"\n{'='*40}\nNAME {i}/{len(names)}: {name}\n{'='*40}")
        
        # specific search URL construction
        q = urllib.parse.quote_plus(f'"{name}"')
        search_url = (
            f"https://www.linkedin.com/search/results/people/?"
            f"keywords={q}&schoolFilter=%5B%22{school_id}%22%5D&origin=FACETED_SEARCH"
        )
        
        scraper.driver.get(search_url)
        time.sleep(5)
        scraper.scroll_full_page()

        profile_urls = scraper.extract_profile_urls_from_page()
        limit = int(os.getenv("RESULTS_PER_SEARCH", "5") or 5)
        profile_urls = profile_urls[:limit]

        if not profile_urls:
            logger.info(f"No profiles found for '{name}'.")
            continue

        for idx, url in enumerate(profile_urls, start=1):
            # Check for force exit before each profile
            if check_force_exit():
                logger.info("🔴 Force exit. Stopping immediately.")
                return
            
            if history_mgr.should_skip(url):
                logger.info(f"[{idx}] ⊘ Skipping (already visited): {url}")
                continue

            logger.info(f"[{idx}] Scraping: {url}")
            data = scraper.scrape_profile_page(url)

            if data:
                # Use search name as fallback if profile name is empty/hidden
                data['name'] = data.get('name') or name
                if database_handler.save_profile_to_csv(data):
                    profiles_scraped += 1
                    history_mgr.mark_as_visited(url, saved=True)
                else:
                    history_mgr.mark_as_visited(url, saved=False)
            else:
                # Failed scrape (blocked or error)
                history_mgr.mark_as_visited(url, saved=False)

            # Check for graceful exit after profile saved
            if should_stop():
                logger.info("🛑 Exit requested. Profile saved, stopping.")
                return

            if idx < len(profile_urls):
                wait_between_profiles()

# ============================================================
# Mode: SEARCH (General Iteration)
# ============================================================
def run_search_mode(scraper, history_mgr):
    logger.info("--- MODE: Search (Iterating through UNT Alumni) ---")
    
    # Base URL: UNT Alumni + Engineering/CS Keywords (approximate filter)
    base_search_url = (
        "https://www.linkedin.com/search/results/people/?origin=FACETED_SEARCH"
        "&network=%5B%22O%22%5D" # 3rd+ connections
        "&schoolFilter=%5B%226464%22%5D" # UNT
    )

    page = 1
    profiles_scraped = 0

    while True:
        # Check for exit at start of each page
        if should_stop():
            logger.info("🛑 Exit requested. Stopping search mode.")
            break
        
        logger.info(f"\n{'='*40}\nPAGE {page}\n{'='*40}")
        search_url = base_search_url if page == 1 else f"{base_search_url}&page={page}"
        
        scraper.driver.get(search_url)
        time.sleep(5)
        scraper.scroll_full_page()
        
        urls = scraper.extract_profile_urls_from_page()
        if not urls:
            logger.info("No more profiles found. Exiting search loop.")
            break

        for idx, url in enumerate(urls, start=1):
            # Check for force exit before each profile
            if check_force_exit():
                logger.info("🔴 Force exit. Stopping immediately.")
                return
            
            if history_mgr.should_skip(url):
                logger.info(f"[{idx}] ⊘ Skipping: {url}")
                continue

            logger.info(f"[{idx}] Scraping: {url}")
            data = scraper.scrape_profile_page(url)

            if data:
                if database_handler.save_profile_to_csv(data):
                    profiles_scraped += 1
                    history_mgr.mark_as_visited(url, saved=True)
                else:
                    history_mgr.mark_as_visited(url, saved=False)
            
            # Check for graceful exit after profile saved
            if should_stop():
                logger.info("🛑 Exit requested. Profile saved, stopping.")
                return
            
            if idx < len(urls):
                wait_between_profiles()

        page += 1

# ============================================================
# Mode: CONNECTIONS (From CSV export)
# ============================================================
def run_connections_mode(scraper, history_mgr):
    csv_path = Path(__file__).resolve().parent.parent / config.CONNECTIONS_CSV_PATH
    logger.info(f"--- MODE: Connections (Source: {csv_path}) ---")

    try:
        df = pd.read_csv(csv_path, skiprows=3) # LinkedIn exports usually have 3 junk rows
    except Exception as e:
        logger.error(f"Failed to read connections CSV: {e}")
        return

    # Clean DataFrame
    df = df.dropna(subset=['URL'])
    df = df[df['URL'].str.contains('linkedin.com/in/', na=False)]
    
    total = len(df)
    logger.info(f"Found {total} valid connection URLs.")

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        # Check for force exit before each profile
        if check_force_exit():
            logger.info("🔴 Force exit. Stopping immediately.")
            return
        
        url = str(row.get('URL', '')).strip()
        fname = str(row.get('First Name', '')).strip()
        lname = str(row.get('Last Name', '')).strip()
        full_name = f"{fname} {lname}"

        logger.info(f"\nCONNECTION {i}/{total}: {full_name}")

        if history_mgr.should_skip(url):
            logger.info("⊘ Skipping (already visited)")
            continue

        data = scraper.scrape_profile_page(url)
        if data:
            data['name'] = data.get('name') or full_name
            if database_handler.save_profile_to_csv(data):
                history_mgr.mark_as_visited(url, saved=True)
            else:
                history_mgr.mark_as_visited(url, saved=False)
        
        # Check for graceful exit after profile saved
        if should_stop():
            logger.info("🛑 Exit requested. Profile saved, stopping.")
            return
        
        if i < total:
            wait_between_profiles()

# ============================================================
# Mode: UPDATE (Refresh old profiles)
# ============================================================
def run_update_mode(scraper, history_mgr, outdated_profiles):
    logger.info(f"--- MODE: Update ({len(outdated_profiles)} profiles) ---")

    for i, (url, fname, lname, last_updated) in enumerate(outdated_profiles, start=1):
        # Check for force exit before each profile
        if check_force_exit():
            logger.info("🔴 Force exit. Stopping immediately.")
            return
        
        full_name = f"{fname} {lname}"
        logger.info(f"\nUPDATE {i}/{len(outdated_profiles)}: {full_name} (Last: {last_updated})")

        # Force scrape even if visited recently (since we are in update mode)
        data = scraper.scrape_profile_page(url)
        
        if data:
            data['name'] = full_name
            if database_handler.save_profile_to_csv(data):
                logger.info(f"✅ Updated: {full_name}")
                history_mgr.mark_as_visited(url, saved=True)
            else:
                history_mgr.mark_as_visited(url, saved=False)
        else:
            # If scrape failed (e.g. 404), mark visited so we don't loop it forever
            history_mgr.mark_as_visited(url, saved=False)

        # Check for graceful exit after profile saved
        if should_stop():
            logger.info("🛑 Exit requested. Profile saved, stopping.")
            return

        if i < len(outdated_profiles):
            wait_between_profiles()

# ============================================================
# Mode: REVIEW (Re-scrape flagged profiles)
# ============================================================
def run_review_mode(scraper, history_mgr):
    """
    Re-scrape profiles listed in flagged_for_review.txt.
    Bypasses history check to force overwrite of existing data.
    Clears the file after processing.
    """
    flagged_file = config.FLAGGED_PROFILES_FILE
    
    if not flagged_file.exists():
        logger.info("No flagged_for_review.txt file found.")
        return False
    
    # Read URLs from file
    with open(flagged_file, 'r') as f:
        lines = f.readlines()
    
    # Parse and validate URLs
    urls = []
    for line in lines:
        url = line.strip()
        if not url or url.startswith('#'):  # Skip empty lines and comments
            continue
        if 'linkedin.com/in/' in url:
            urls.append(url)
        else:
            logger.warning(f"Skipping invalid URL: {url}")
    
    if not urls:
        logger.info("No valid LinkedIn URLs found in flagged_for_review.txt")
        return False
    
    logger.info(f"--- MODE: Review ({len(urls)} flagged profiles) ---")
    logger.info("⚠️ This will OVERWRITE existing data for these profiles.")
    
    reviewed = 0
    failed = []
    
    for i, url in enumerate(urls, start=1):
        # Check for force exit
        if check_force_exit():
            logger.info("🔴 Force exit. Stopping immediately.")
            break
        
        logger.info(f"\nREVIEW {i}/{len(urls)}: {url}")
        
        # Force scrape (bypass history check)
        data = scraper.scrape_profile_page(url)
        
        if data:
            if database_handler.save_profile_to_csv(data):
                logger.info(f"✅ Re-scraped and saved: {data.get('name', url)}")
                history_mgr.mark_as_visited(url, saved=True)
                reviewed += 1
            else:
                logger.warning(f"❌ Failed to save: {url}")
                failed.append(url)
        else:
            logger.warning(f"❌ Failed to scrape: {url}")
            failed.append(url)
        
        # Check for graceful exit
        if should_stop():
            logger.info("🛑 Exit requested. Stopping review mode.")
            break
        
        if i < len(urls):
            wait_between_profiles()
    
    # Calculate which URLs were NOT processed
    # If we broke early (e.g. i=5 out of 10), we processed 1..5. Unprocessed are 6..10.
    # urls is 0-indexed list.
    # If `break` happened at i (1-based), then urls[i-1] was the last one attempted.
    # Remaining are urls[i:] 
    
    remaining = []
    if i < len(urls):
       remaining = urls[i:]

    # Rewrite file with FAILED + REMAINING
    # If everything succeeded and finished -> failed=[] and remaining=[] -> clear file.
    
    to_keep = failed + remaining
    
    if to_keep:
        if remaining:
            logger.info(f"\n⚠️ Process stopped early. Keeping {len(remaining)} unprocessed profiles + {len(failed)} failed profiles.")
        elif failed:
            logger.info(f"\n⚠️ {len(failed)} profiles failed, keeping them in file for retry.")
            
        with open(flagged_file, 'w') as f:
            for url in to_keep:
                f.write(url + '\n')
    else:
        # Clear the file completely
        with open(flagged_file, 'w') as f:
            f.write('')
        logger.info(f"\n✅ Cleared flagged_for_review.txt")
    
    logger.info(f"\n📊 Review complete: {reviewed}/{len(urls) - len(remaining)} attempted profiles scaped successfully.")
    return True


# ============================================================
# Post-Scrape Automation
# ============================================================

def run_post_scrape_sync():
    """
    Automatically sync CSV data to database and geocode new locations.
    Called after any scraping operation completes.
    Skips if no profiles were scraped during this session.
    """
    global session_profiles_scraped
    
    # Skip sync if nothing was scraped this session
    if session_profiles_scraped == 0:
        logger.info("📊 No new profiles scraped this session - skipping database sync.")
        return
    
    try:
        print(f"\n{'='*50}")
        print(f"🔄 POST-SCRAPE SYNC: Syncing {session_profiles_scraped} new profile(s)...")
        print(f"{'='*50}")
        
        # Import backend modules (lazy import to avoid startup overhead)
        from database import seed_alumni_data, sync_alumni_to_visited_profiles
        from geocoding import populate_missing_coordinates
        
        # Step 1: Sync CSV to database
        logger.info("📥 Step 1/3: Syncing CSV data to database...")
        try:
            seed_alumni_data()
            logger.info("✓ CSV data synced to database")
        except Exception as e:
            logger.error(f"✗ Failed to sync CSV to database: {e}")
        
        # Step 2: Sync alumni to visited_profiles table
        logger.info("📥 Step 2/3: Syncing alumni to visited profiles...")
        try:
            sync_alumni_to_visited_profiles()
            logger.info("✓ Alumni synced to visited profiles")
        except Exception as e:
            logger.error(f"✗ Failed to sync visited profiles: {e}")
        
        # Step 3: Geocode new locations
        logger.info("🌍 Step 3/3: Geocoding new locations...")
        try:
            geocoded = populate_missing_coordinates()
            if geocoded > 0:
                logger.info(f"✓ Geocoded {geocoded} new locations")
            else:
                logger.info("✓ No new locations needed geocoding")
        except Exception as e:
            logger.error(f"✗ Failed to geocode locations: {e}")
        
        print(f"\n{'='*50}")
        print("✅ POST-SCRAPE SYNC: Complete!")
        print(f"{'='*50}")
        
    except ImportError as e:
        logger.warning(f"⚠ Post-scrape sync skipped (backend modules not available): {e}")
    except Exception as e:
        logger.error(f"✗ Post-scrape sync failed: {e}")
        import traceback
        traceback.print_exc()


# ============================================================
# Main Execution
# ============================================================

def main():
    # 1. Initialize History
    history_mgr = database_handler.HistoryManager()
    history_mgr.sync_with_db()

    # 2. Setup Scraper
    scraper = LinkedInScraper()
    scraper.setup_driver()

    try:
        # 3. Login
        if not scraper.login():
            logger.error("Login failed. Exiting.")
            return

        # 4. Start exit listener (type 'exit' or 'force exit')
        start_exit_listener()

        # 5. Check for FLAGGED profiles (priority - review mode)
        if config.FLAGGED_PROFILES_FILE.exists():
            with open(config.FLAGGED_PROFILES_FILE, 'r') as f:
                flagged_count = len([l for l in f.readlines() if l.strip() and not l.startswith('#') and 'linkedin.com/in/' in l])
            
            if flagged_count > 0:
                print(f"\n{'='*50}")
                print(f"🔍 Found {flagged_count} profiles flagged for review.")
                print(f"{'='*50}")
                choice = input(">>> Run REVIEW mode to re-scrape them? (y/n): ").strip().lower()
                if choice == 'y':
                    run_review_mode(scraper, history_mgr)
                    return  # Exit after review

        # 6. Check for OUTDATED profiles
        outdated, cutoff = database_handler.get_outdated_profiles_from_db()
        if outdated:
            print(f"\n{'='*50}")
            print(f"🔄 Found {len(outdated)} profiles older than {cutoff.date()}.")
            print(f"{'='*50}")
            choice = input(">>> Run UPDATE mode to refresh them? (y/n): ").strip().lower()
            if choice == 'y':
                run_update_mode(scraper, history_mgr, outdated)
                return # Exit after update

        # 7. Run Selected Mode
        mode = config.SCRAPER_MODE
        if mode == "names":
            run_names_mode(scraper, history_mgr)
        elif mode == "search":
            run_search_mode(scraper, history_mgr)
        elif mode == "connections":
            run_connections_mode(scraper, history_mgr)
        elif mode == "review":
            # Allow explicit SCRAPER_MODE=review
            run_review_mode(scraper, history_mgr)
        else:
            logger.error(f"Unknown SCRAPER_MODE: {mode}")

    except KeyboardInterrupt:
        logger.warning("\n🛑 Scraper stopped by user.")
    except Exception as e:
        logger.error(f"Fatal Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        stop_exit_listener()
        scraper.quit()
        
        # Run post-scrape database sync and geocoding
        run_post_scrape_sync()

if __name__ == "__main__":
    main()