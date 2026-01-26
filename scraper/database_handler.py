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
                    url = str(row.get('profile_url', '')).strip()
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
            url = (profile.get('linkedin_url') or "").strip()
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
        url = url.strip()
        save_visited_profile(url, is_unt_alum=bool(saved)) # DB Call
        
        self.visited_history[url] = {
            'saved': 'yes' if saved else 'no',
            'visited_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'update_needed': 'yes' if update_needed else 'no',
            'last_db_update': self.visited_history.get(url, {}).get('last_db_update', '')
        }
        self.save_history_csv()

    def should_skip(self, url):
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
    """Normalize fancy Unicode characters to ASCII equivalents."""
    if not text or not isinstance(text, str):
        return text
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
    }
    for unicode_char, ascii_char in replacements.items():
        text = text.replace(unicode_char, ascii_char)
    return text

def save_profile_to_csv(profile_data):
    try:
        if not profile_data.get('profile_url') or not profile_data.get('name'): return False
        
        has_data = any([profile_data.get(k) for k in ['headline', 'location', 'job_title', 'education']])
        if not has_data: return False

        existing_df = pd.read_csv(OUTPUT_CSV, encoding='utf-8') if OUTPUT_CSV.exists() else pd.DataFrame(columns=CSV_COLUMNS)
        
        save_data = {k: v for k, v in profile_data.items() if k in CSV_COLUMNS}
        save_data['job_title'] = clean_job_title(save_data.get('job_title', ''))
        
        # Normalize text fields to fix fancy quotes/apostrophes
        text_fields = ['name', 'headline', 'location', 'job_title', 'company', 'major', 
                       'exp2_title', 'exp2_company', 'exp3_title', 'exp3_company']
        for field in text_fields:
            if field in save_data and save_data[field]:
                save_data[field] = normalize_text(str(save_data[field]))
        
        for col in CSV_COLUMNS:
            save_data.setdefault(col, "")
            
        new_row = pd.DataFrame([save_data])[CSV_COLUMNS]
        combined_df = pd.concat([existing_df, new_row], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['profile_url'], keep='last')
        
        combined_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        return True
    except Exception as e:
        logger.error(f"❌ Error saving profile: {e}")
        return False