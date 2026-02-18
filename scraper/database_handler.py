import os
import pandas as pd
import mysql.connector
from datetime import datetime
from pathlib import Path
import sys

# Hack for imports if needed, or adjust structure
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'backend'))
from database import save_visited_profile, get_all_visited_profiles

from config import (
    logger, UPDATE_FREQUENCY, VISITED_HISTORY_FILE, 
    VISITED_HISTORY_COLUMNS, OUTPUT_CSV, CSV_COLUMNS
)
from utils import parse_frequency, clean_job_title

def get_outdated_profiles_from_db():
    try:
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

class HistoryManager:
    def __init__(self):
        self.visited_history = {}
        self._ensure_csv_headers()

    def _ensure_csv_headers(self):
        try:
            if VISITED_HISTORY_FILE.exists():
                df = pd.read_csv(VISITED_HISTORY_FILE)
                if list(df.columns) != VISITED_HISTORY_COLUMNS:
                    raise ValueError("Mismatch")
            else:
                pd.DataFrame(columns=VISITED_HISTORY_COLUMNS).to_csv(VISITED_HISTORY_FILE, index=False)
        except Exception:
            pd.DataFrame(columns=VISITED_HISTORY_COLUMNS).to_csv(VISITED_HISTORY_FILE, index=False)

    def load_from_csv(self):
        if VISITED_HISTORY_FILE.exists():
            try:
                df = pd.read_csv(VISITED_HISTORY_FILE)
                self.visited_history = {}
                for _, row in df.iterrows():
                    url = str(row.get('profile_url', '')).strip().rstrip('/')
                    if not url: continue
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
            self.visited_history = {}

    def sync_with_db(self):
        logger.info("\n📊 Initializing visited history from database...")
        db_profiles = get_all_visited_profiles()
        if not db_profiles:
            logger.warning("⚠️ No visited profiles found in database")
            self.load_from_csv()
            return

        frequency_delta = parse_frequency(UPDATE_FREQUENCY)
        now = datetime.now()

        self.visited_history = {}
        for profile in db_profiles:
            url = (profile.get('linkedin_url') or "").strip().rstrip('/')
            if not url: continue

            is_unt = bool(profile.get('is_unt_alum'))
            last_checked = profile.get('last_checked')
            needs_update_db = bool(profile.get('needs_update'))

            update_needed = 'no'
            if needs_update_db:
                update_needed = 'yes'
            elif is_unt and last_checked:
                # Basic date parsing logic if string
                if isinstance(last_checked, str):
                    try: last_checked_dt = datetime.fromisoformat(last_checked.replace('Z', '+00:00'))
                    except: last_checked_dt = now
                else:
                    last_checked_dt = last_checked
                
                if (now - last_checked_dt) > frequency_delta:
                    update_needed = 'yes'

            self.visited_history[url] = {
                'saved': 'yes' if is_unt else 'no',
                'visited_at': str(profile.get('visited_at') or ''),
                'update_needed': update_needed,
                'last_db_update': str(last_checked or '')
            }
        self.save_history_csv()

    def save_history_csv(self):
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
            pd.DataFrame(rows).to_csv(VISITED_HISTORY_FILE, index=False)
        except Exception as e:
            logger.error(f"Error saving visited history: {e}")

    def mark_as_visited(self, url, saved=False, update_needed=False):
        if not url: return
        url = url.strip().rstrip('/')
        save_visited_profile(url, is_unt_alum=bool(saved)) # DB Call
        
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        self.visited_history[url] = {
            'saved': 'yes' if saved else 'no',
            'visited_at': now_str,
            'update_needed': 'yes' if update_needed else 'no',
            'last_db_update': now_str  # Update with current time as we just synced to DB
        }
        self.save_history_csv()

    def should_skip(self, url):
        if not url: return False
        url = url.strip().rstrip('/')
        if url not in self.visited_history: return False
        entry = self.visited_history[url]
        saved = entry.get('saved', 'no').lower()
        update_needed = entry.get('update_needed', 'no').lower()
        
        if saved == 'yes' and update_needed == 'yes':
            logger.info("    🔄 Re-visiting UNT alum (update needed)")
            return False
        if saved == 'yes': return True
        return True

def normalize_text(text):
    """
    Normalize text for safe CSV storage.
    - Replaces fancy Unicode characters with ASCII equivalents
    - Removes/replaces newlines, tabs, and control characters that break CSV
    """
    if not text or not isinstance(text, str):
        return text
    
    # Remove/replace control characters that break CSV format
    # Replace newlines with space (or pipe separator for multi-line content)
    text = text.replace('\r\n', ' | ')
    text = text.replace('\n', ' | ')
    text = text.replace('\r', ' | ')
    text = text.replace('\t', ' ')
    
    # Common Unicode replacements
    replacements = {
        '\u2019': "'",  # RIGHT SINGLE QUOTATION MARK -> apostrophe
        '\u2018': "'",  # LEFT SINGLE QUOTATION MARK -> apostrophe
        '\u201c': '"',  # LEFT DOUBLE QUOTATION MARK
        '\u201d': '"',  # RIGHT DOUBLE QUOTATION MARK
        '\u2013': '-',  # EN DASH
        '\u2014': '-',  # EM DASH
        '\u2026': '...', # ELLIPSIS
        '\xa0': ' ',    # NON-BREAKING SPACE
        '\u00a0': ' ',  # NON-BREAKING SPACE (alt)
        '\u200b': '',   # ZERO WIDTH SPACE
        '\u200c': '',   # ZERO WIDTH NON-JOINER
        '\u200d': '',   # ZERO WIDTH JOINER
        '\ufeff': '',   # BYTE ORDER MARK
    }
    for unicode_char, ascii_char in replacements.items():
        text = text.replace(unicode_char, ascii_char)
    
    # Collapse multiple spaces into one
    import re
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def flag_profile_for_review(profile_data):
    """
    Flag profiles with incomplete data for manual review.
    Appends to flagged_for_review.txt with descriptive comments.
    """
    from config import FLAGGED_PROFILES_FILE
    
    url = profile_data.get('profile_url', '')
    if not url:
        return
    url = url.strip().rstrip('/')
    
    issues = []
    
    job_title = profile_data.get('job_title', '').strip()
    company = profile_data.get('company', '').strip()
    graduation_year = profile_data.get('graduation_year', '')
    major = profile_data.get('major', '').strip()
    
    # Only flag if one of title/company is present but not the other
    # (If both are missing, assume the person doesn't have work experience listed)
    from config import FLAG_MISSING_EXPERIENCE_DATA, FLAG_MISSING_GRAD_YEAR, FLAG_MISSING_DEGREE
    
    if FLAG_MISSING_EXPERIENCE_DATA:
        if job_title and not company:
            issues.append("Missing Company but Job Title Present")
        elif company and not job_title:
            issues.append("Missing Job Title but Company Present")

        # Check Experience 2
        exp2_title = profile_data.get('exp2_title', '').strip()
        exp2_company = profile_data.get('exp2_company', '').strip()
        if exp2_title and not exp2_company:
            issues.append("Missing Company but Job Title Present for Experience 2")
        elif exp2_company and not exp2_title:
            issues.append("Missing Job Title but Company Present for Experience 2")

        # Check Experience 3
        exp3_title = profile_data.get('exp3_title', '').strip()
        exp3_company = profile_data.get('exp3_company', '').strip()
        if exp3_title and not exp3_company:
            issues.append("Missing Company but Job Title Present for Experience 3")
        elif exp3_company and not exp3_title:
            issues.append("Missing Job Title but Company Present for Experience 3")
    
    # Conditional flagging for education data
    if FLAG_MISSING_GRAD_YEAR and not graduation_year:
        issues.append("Missing Grad Year")
    
    if FLAG_MISSING_DEGREE and not major:
        issues.append("Missing Degree/Major Information")
    
    if not issues:
        return  # Nothing to flag
    
    # Format: URL # Issue1; Issue2
    flag_line = f"{url} # {'; '.join(issues)}\n"
    
    try:
        # Read existing lines to avoid duplicates
        existing_lines = set()
        if FLAGGED_PROFILES_FILE.exists():
            with open(FLAGGED_PROFILES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    # Extract just the URL part (before the #)
                    existing_url = line.split('#')[0].strip()
                    existing_lines.add(existing_url)
        
        # Only append if URL not already flagged
        if url not in existing_lines:
            with open(FLAGGED_PROFILES_FILE, 'a', encoding='utf-8') as f:
                f.write(flag_line)
            logger.info(f"🚩 Flagged for review: {url} ({'; '.join(issues)})")
    except Exception as e:
        logger.warning(f"Could not flag profile: {e}")

def save_profile_to_csv(profile_data):
    try:
        if not profile_data.get('profile_url') or not profile_data.get('name'): return False

        # Block fake/placeholder profiles
        from config import is_blocked_url
        if is_blocked_url(profile_data.get('profile_url', '')):
            logger.info(f"🚫 Blocked profile skipped: {profile_data.get('profile_url')}")
            return False
        
        has_data = any([profile_data.get(k) for k in ['headline', 'location', 'job_title', 'school', 'education']])
        if not has_data: return False

        existing_df = pd.read_csv(OUTPUT_CSV, encoding='utf-8') if OUTPUT_CSV.exists() else pd.DataFrame(columns=CSV_COLUMNS)
        
        # Transform data to new schema
        name = str(profile_data.get('name', '')).strip()
        parts = name.split()
        first = parts[0] if len(parts) > 0 else ''
        last = ' '.join(parts[1:]) if len(parts) > 1 else ''

        save_data = {
            'first': first,
            'last': last,
            'linkedin_url': str(profile_data.get('profile_url', '')).strip().rstrip('/'),
            # Primary education
            'school': profile_data.get('school', profile_data.get('education', '')),
            'degree': profile_data.get('degree', ''),
            'major': profile_data.get('major', ''),
            'school_start': profile_data.get('school_start_date'),
            'grad_year': profile_data.get('graduation_year'),
            # Education 2 and 3
            'school2': profile_data.get('school2', ''),
            'degree2': profile_data.get('degree2', ''),
            'major2': profile_data.get('major2', ''),
            'school3': profile_data.get('school3', ''),
            'degree3': profile_data.get('degree3', ''),
            'major3': profile_data.get('major3', ''),
            # Standardized fields
            'standardized_degree': profile_data.get('standardized_degree', ''),
            'standardized_major': profile_data.get('standardized_major', ''),
            'standardized_degree2': profile_data.get('standardized_degree2', ''),
            'standardized_major2': profile_data.get('standardized_major2', ''),
            'standardized_degree3': profile_data.get('standardized_degree3', ''),
            'standardized_major3': profile_data.get('standardized_major3', ''),
            # Other fields
            'location': profile_data.get('location'),
            'working_while_studying': profile_data.get('working_while_studying'),
            'title': clean_job_title(profile_data.get('job_title', '')),
            'company': profile_data.get('company'),
            'job_start': profile_data.get('job_start_date'),
            'job_end': profile_data.get('job_end_date'),
            'exp_2_title': profile_data.get('exp2_title'),
            'exp_2_company': profile_data.get('exp2_company'),
            'exp_2_dates': profile_data.get('exp2_dates'),
            'exp_3_title': profile_data.get('exp3_title'),
            'exp_3_company': profile_data.get('exp3_company'),
            'exp_3_dates': profile_data.get('exp3_dates'),
            'scraped_at': profile_data.get('scraped_at')
        }
        
        # Normalize text fields
        text_fields = ['first', 'last', 'location', 'title', 'company', 'major',
                       'degree', 'major2', 'degree2', 'major3', 'degree3',
                       'exp_2_title', 'exp_2_company', 'exp_3_title', 'exp_3_company']
        for field in text_fields:
            if field in save_data and save_data[field]:
                save_data[field] = normalize_text(str(save_data[field]))
        
        # Ensure all columns exist
        for col in CSV_COLUMNS:
            save_data.setdefault(col, "")
            
        new_row = pd.DataFrame([save_data])[CSV_COLUMNS]
        combined_df = pd.concat([existing_df, new_row], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['linkedin_url'], keep='last')
        
        combined_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        
        # Flag profiles with incomplete data for review
        # Note: flag_profile_for_review still expects original keys, so pass original profile_data
        flag_profile_for_review(profile_data)
        
        return True
    except Exception as e:
        logger.error(f"❌ Error saving profile: {e}")
        return False