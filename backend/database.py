import mysql.connector
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Import discipline inference function for auto-classification
try:
    from backfill_disciplines import infer_discipline
except ImportError:
    # Fallback if module not available
    def infer_discipline(degree, job_title, headline):
        return "Unknown"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MySQL connection parameters (for direct access when needed)
MYSQL_HOST = os.getenv('MYSQLHOST')
MYSQL_USER = os.getenv('MYSQLUSER')
MYSQL_PASSWORD = os.getenv('MYSQLPASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_PORT = int(os.getenv('MYSQLPORT', 3306))

# Flag to control whether to use fallback system
USE_SQLITE_FALLBACK = os.getenv('USE_SQLITE_FALLBACK', '1') == '1'


def normalize_url(url):
    """Strip trailing slashes from URL."""
    if pd.isna(url) or url is None: return None
    s = str(url).strip()
    if not s or s.lower() == 'nan': return None
    return s.rstrip('/')



def get_connection():
    """
    Get a database connection.
    If USE_SQLITE_FALLBACK is enabled, routes to MySQL or SQLite based on availability.
    Otherwise, returns a direct MySQL connection.
    """
    # Check if DB is explicitly disabled (dev mode)
    # in this mode, we force SQLite if fallback is enabled, regardless of cloud availability
    disable_db = os.getenv("DISABLE_DB", "0") == "1"
    
    if USE_SQLITE_FALLBACK:
        try:
            from sqlite_fallback import get_connection_manager, SQLiteConnectionWrapper
            manager = get_connection_manager()
            
            if disable_db:
                # Force offline/SQLite mode
                return SQLiteConnectionWrapper(manager.get_sqlite_connection(), manager)
            
            return manager.get_connection()
        except ImportError:
            logger.warning("sqlite_fallback module not found, falling back to direct MySQL")
    
    # Direct MySQL connection (original behavior)
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT
    )


def get_direct_mysql_connection():
    """Get a direct MySQL connection (bypasses fallback system)."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT
    )


def init_db():
    """Initialize database tables if they don't exist"""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Create users table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    linkedin_id VARCHAR(255) UNIQUE NOT NULL,
                    email VARCHAR(255),
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)
            logger.info("users table created/verified")
                        # Create authorized_emails table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS authorized_emails (
                    email VARCHAR(255) PRIMARY KEY
                )
            """)
            logger.info("authorized_emails table created/verified")

            # Seed authorized email (ignore if already exists)
            cur.execute("""
                INSERT IGNORE INTO authorized_emails (email)
                VALUES (%s)
            """, ("lamichhaneabishek451@gmail.com",))


            # Create alumni table
            # Added columns:
            # - school_start_date (TEXT) : store "YYYY" or "Mon YYYY"
            # - job_start_date, job_end_date (TEXT) : store "YYYY" or "Mon YYYY" or "Present" (end only)
            # - working_while_studying (BOOLEAN)
            # - major (VARCHAR) : engineering discipline
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alumni (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    grad_year INT,
                    degree VARCHAR(255),
                    major VARCHAR(255) DEFAULT NULL,
                    linkedin_url VARCHAR(500) NOT NULL,
                    current_job_title VARCHAR(255),
                    company VARCHAR(255),
                    location VARCHAR(255),
                    headline VARCHAR(500),
                    school_start_date VARCHAR(20) DEFAULT NULL,
                    job_start_date VARCHAR(20) DEFAULT NULL,
                    job_end_date VARCHAR(20) DEFAULT NULL,
                    working_while_studying BOOLEAN DEFAULT NULL,
                    latitude DOUBLE NULL,
                    longitude DOUBLE NULL,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_alumni_linkedin_url (linkedin_url),
                    INDEX idx_coordinates (latitude, longitude)
                )
            """)
            logger.info("alumni table created/verified")

            # Create visited_profiles table (tracks ALL visited profiles, including non-UNT)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS visited_profiles (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    linkedin_url VARCHAR(500) NOT NULL UNIQUE,
                    is_unt_alum BOOLEAN DEFAULT FALSE,
                    visited_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_checked DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    needs_update BOOLEAN DEFAULT FALSE,
                    notes VARCHAR(255) DEFAULT NULL,
                    INDEX idx_linkedin_url (linkedin_url),
                    INDEX idx_is_unt_alum (is_unt_alum),
                    INDEX idx_needs_update (needs_update)
                )
            """)
            logger.info("visited_profiles table created/verified")

            # Create user_interactions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_interactions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL,
                    alumni_id INT NOT NULL,
                    interaction_type ENUM('bookmarked', 'connected') NOT NULL,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_interaction (user_id, alumni_id, interaction_type),
                    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    CONSTRAINT fk_alumni_id FOREIGN KEY (alumni_id) REFERENCES alumni(id) ON DELETE CASCADE
                )
            """)
            logger.info("user_interactions table created/verified")

            # Create notes table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS notes (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL,
                    alumni_id INT NOT NULL,
                    note_content LONGTEXT,
                    exp3_title TEXT,
                    exp3_company TEXT,
                    exp3_dates TEXT,
                    education TEXT,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_user_alumni (user_id, alumni_id),
                    CONSTRAINT fk_notes_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    CONSTRAINT fk_notes_alumni_id FOREIGN KEY (alumni_id) REFERENCES alumni(id) ON DELETE CASCADE
                )
            """)
            logger.info("notes table created/verified")

            # Create authorized_emails table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS authorized_emails (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    added_by_user_id INT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notes VARCHAR(500),
                    CONSTRAINT fk_added_by_user FOREIGN KEY (added_by_user_id) 
                        REFERENCES users(id) ON DELETE SET NULL,
                    INDEX idx_email (email)
                )
            """)
            logger.info("authorized_emails table created/verified")

            conn.commit()
            logger.info("All tables initialized successfully")

    except Exception as err:
        logger.error(f"Database error: {err}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# MIGRATIONS / COLUMN ENSURANCE
# ============================================================

def ensure_alumni_timestamp_columns():
    """Ensure scraped_at and last_updated columns exist in alumni table"""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Add scraped_at column if it doesn't exist
            try:
                cur.execute("""
                    ALTER TABLE alumni 
                    ADD COLUMN scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """)
                logger.info("Added scraped_at column to alumni table")
            except Exception as err:
                if "duplicate column" in str(err).lower():
                    logger.info("scraped_at column already exists")
                else:
                    raise

            # Add last_updated column if it doesn't exist
            try:
                cur.execute("""
                    ALTER TABLE alumni 
                    ADD COLUMN last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                """)
                logger.info("Added last_updated column to alumni table")
            except Exception as err:
                if "duplicate column" in str(err).lower():
                    logger.info("last_updated column already exists")
                else:
                    raise

            conn.commit()
    except Exception as err:
        logger.error(f"Error ensuring timestamp columns: {err}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def ensure_alumni_work_school_date_columns():
    """
    Ensure new columns exist for:
      - school_start_date
      - job_start_date
      - job_end_date
      - working_while_studying
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            def add_col(sql, name):
                try:
                    cur.execute(sql)
                    logger.info(f"Added {name} column to alumni table")
                except Exception as err:
                    if "duplicate column" in str(err).lower():
                        logger.info(f"{name} column already exists")
                    else:
                        raise

            add_col("ALTER TABLE alumni ADD COLUMN school_start_date VARCHAR(20) DEFAULT NULL", "school_start_date")
            add_col("ALTER TABLE alumni ADD COLUMN job_start_date VARCHAR(20) DEFAULT NULL", "job_start_date")
            add_col("ALTER TABLE alumni ADD COLUMN job_end_date VARCHAR(20) DEFAULT NULL", "job_end_date")
            add_col("ALTER TABLE alumni ADD COLUMN working_while_studying BOOLEAN DEFAULT NULL", "working_while_studying")
            
            # Experience 2 and 3 columns
            add_col("ALTER TABLE alumni ADD COLUMN exp2_title VARCHAR(255) DEFAULT NULL", "exp2_title")
            add_col("ALTER TABLE alumni ADD COLUMN exp2_company VARCHAR(255) DEFAULT NULL", "exp2_company")
            add_col("ALTER TABLE alumni ADD COLUMN exp2_dates VARCHAR(50) DEFAULT NULL", "exp2_dates")
            add_col("ALTER TABLE alumni ADD COLUMN exp3_title VARCHAR(255) DEFAULT NULL", "exp3_title")
            add_col("ALTER TABLE alumni ADD COLUMN exp3_company VARCHAR(255) DEFAULT NULL", "exp3_company")
            add_col("ALTER TABLE alumni ADD COLUMN exp3_dates VARCHAR(50) DEFAULT NULL", "exp3_dates")

            conn.commit()
    except Exception as err:
        logger.error(f"Error ensuring work/school date columns: {err}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def ensure_alumni_major_column():
    """
    Ensure major column exists in alumni table for engineering disciplines.
    This is required for the Engineering Discipline filter to work.
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    ALTER TABLE alumni 
                    ADD COLUMN major VARCHAR(255) DEFAULT NULL
                """)
                logger.info("Added major column to alumni table")
            except Exception as err:
                if "duplicate column" in str(err).lower():
                    logger.info("Major column already exists")
                else:
                    raise
            conn.commit()
    except Exception as err:
        logger.error(f"Error ensuring major column: {err}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# VISITED PROFILES FUNCTIONS
# ============================================================

def save_visited_profile(linkedin_url, is_unt_alum=False, notes=None):
    """
    Save a visited profile to the visited_profiles table.
    This tracks ALL profiles we've ever visited (UNT and non-UNT).
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO visited_profiles (linkedin_url, is_unt_alum, visited_at, last_checked, notes)
                VALUES (%s, %s, NOW(), NOW(), %s)
                ON DUPLICATE KEY UPDATE
                    is_unt_alum = VALUES(is_unt_alum),
                    last_checked = NOW(),
                    notes = COALESCE(VALUES(notes), notes)
            """, (normalize_url(linkedin_url), is_unt_alum, notes))
            conn.commit()

        logger.debug(f"💾 Saved to visited_profiles: {linkedin_url} (UNT: {is_unt_alum})")
        return True

    except Exception as err:
        logger.error(f"Error saving visited profile: {err}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_all_visited_profiles():
    """
    Get all visited profiles from the visited_profiles table.
    Returns a list of dicts with linkedin_url, is_unt_alum, visited_at, last_checked, needs_update.
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT linkedin_url, is_unt_alum, visited_at, last_checked, needs_update
                FROM visited_profiles
            """)
            profiles = cur.fetchall()

        logger.info(f"✓ Retrieved {len(profiles)} visited profiles from database")
        return profiles

    except Exception as err:
        logger.error(f"Error fetching visited profiles: {err}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def mark_profile_needs_update(linkedin_url, needs_update=True):
    """Mark a profile as needing update in the visited_profiles table."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE visited_profiles
                SET needs_update = %s
                WHERE linkedin_url = %s
            """, (needs_update, linkedin_url.strip()))
            conn.commit()

        return True

    except Exception as err:
        logger.error(f"Error updating profile needs_update flag: {err}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def sync_alumni_to_visited_profiles():
    """
    Sync all existing alumni records to the visited_profiles table.
    This ensures all UNT alumni are marked as visited.
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Insert all alumni into visited_profiles (if not already there)
            cur.execute("""
                INSERT INTO visited_profiles (linkedin_url, is_unt_alum, visited_at, last_checked)
                SELECT linkedin_url, TRUE, COALESCE(scraped_at, NOW()), COALESCE(last_updated, NOW())
                FROM alumni
                WHERE linkedin_url IS NOT NULL AND linkedin_url != ''
                ON DUPLICATE KEY UPDATE
                    is_unt_alum = TRUE,
                    last_checked = VALUES(last_checked)
            """)
            synced = cur.rowcount
            conn.commit()

        logger.info(f"✓ Synced {synced} alumni to visited_profiles table")
        return synced

    except Exception as err:
        logger.error(f"Error syncing alumni to visited_profiles: {err}")
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def migrate_visited_history_csv_to_db():
    """
    One-time migration: Import visited_history.csv into the visited_profiles table.
    This preserves all the non-UNT profiles we've already visited.
    """
    # Find the CSV file
    backend_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    project_root = backend_dir.parent
    csv_path = project_root / 'scraper' / 'output' / 'visited_history.csv'

    if not csv_path.exists():
        logger.info("No visited_history.csv found to migrate")
        return 0

    conn = None
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"📂 Migrating {len(df)} entries from visited_history.csv to database...")

        conn = get_connection()
        migrated = 0

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                url = normalize_url(row.get('profile_url'))
                if not url:
                    continue

                saved = str(row.get('saved', 'no')).strip().lower() == 'yes'
                visited_at = row.get('visited_at', None)

                # Handle NaN/empty visited_at
                if pd.isna(visited_at) or visited_at == 'nan' or visited_at == '':
                    visited_at = None

                try:
                    cur.execute("""
                        INSERT INTO visited_profiles (linkedin_url, is_unt_alum, visited_at, last_checked)
                        VALUES (%s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE
                            is_unt_alum = GREATEST(is_unt_alum, VALUES(is_unt_alum)),
                            last_checked = NOW()
                    """, (url, saved, visited_at))
                    migrated += 1
                except Exception as err:
                    logger.warning(f"Skipping {url}: {err}")
                    continue

            conn.commit()

        logger.info(f"✅ Migrated {migrated} profiles from CSV to database")
        return migrated

    except Exception as e:
        logger.error(f"Error migrating visited history: {e}")
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_visited_profiles_stats():
    """Get statistics about visited profiles."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(is_unt_alum) as unt_alumni,
                    SUM(NOT is_unt_alum) as non_unt,
                    SUM(needs_update) as needs_update
                FROM visited_profiles
            """)
            stats = cur.fetchone()

        return stats

    except Exception as err:
        logger.error(f"Error getting visited profiles stats: {err}")
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# AUTHORIZED EMAILS FUNCTIONS
# ============================================================

def get_authorized_emails():
    """
    Get all authorized emails from the database.
    Returns a list of dicts with email, added_at, added_by_user_id, and notes.
    """
    conn = None
    try:
        conn = get_connection()
        use_sqlite = os.getenv("DISABLE_DB", "0") == "1" and USE_SQLITE_FALLBACK
        
        if use_sqlite:
            # SQLite mode
            conn.row_factory = lambda cursor, row: {
                col[0]: row[idx] for idx, col in enumerate(cursor.description)
            }
            cursor = conn.cursor()
            cursor.execute("""
                SELECT email, added_at, added_by_user_id, notes
                FROM authorized_emails
                ORDER BY added_at DESC
            """)
            emails = cursor.fetchall()
        else:
            # MySQL mode
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT email, added_at, added_by_user_id, notes
                    FROM authorized_emails
                    ORDER BY added_at DESC
                """)
                emails = cur.fetchall()
        
        # Convert datetime objects to strings for JSON serialization
        for email_record in emails:
            added_at = email_record.get('added_at')
            if hasattr(added_at, 'isoformat'):
                email_record['added_at'] = added_at.isoformat()
        
        return emails
    except Exception as err:
        logger.error(f"Error fetching authorized emails: {err}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def add_authorized_email(email, added_by_user_id=None, notes=None):
    """
    Add an email to the authorized emails list.
    Returns True if successful, False otherwise.
    """
    conn = None
    try:
        email = email.lower().strip()
        conn = get_connection()
        use_sqlite = os.getenv("DISABLE_DB", "0") == "1" and USE_SQLITE_FALLBACK
        
        if use_sqlite:
            # SQLite mode
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO authorized_emails (email, added_by_user_id, notes, added_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(email) DO UPDATE SET
                    notes = excluded.notes,
                    added_by_user_id = excluded.added_by_user_id
            """, (email, added_by_user_id, notes))
            conn.commit()
        else:
            # MySQL mode
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO authorized_emails (email, added_by_user_id, notes)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        notes = VALUES(notes),
                        added_by_user_id = VALUES(added_by_user_id)
                """, (email, added_by_user_id, notes))
                conn.commit()
        
        logger.info(f"Added authorized email: {email}")
        return True
    except Exception as err:
        logger.error(f"Error adding authorized email {email}: {err}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def remove_authorized_email(email):
    """
    Remove an email from the authorized emails list.
    Returns True if successful, False otherwise.
    """
    conn = None
    try:
        email = email.lower().strip()
        conn = get_connection()
        use_sqlite = os.getenv("DISABLE_DB", "0") == "1" and USE_SQLITE_FALLBACK
        
        if use_sqlite:
            # SQLite mode
            cursor = conn.cursor()
            cursor.execute("DELETE FROM authorized_emails WHERE email = ?", (email,))
            conn.commit()
        else:
            # MySQL mode
            with conn.cursor() as cur:
                cur.execute("DELETE FROM authorized_emails WHERE email = %s", (email,))
                conn.commit()
        
        logger.info(f"Removed authorized email: {email}")
        return True
    except Exception as err:
        logger.error(f"Error removing authorized email {email}: {err}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def migrate_env_emails_to_db():
    """
    One-time migration: Import authorized emails from .env to database.
    Reads AUTHORIZED_EMAILS from environment and adds them to the database.
    """
    env_emails = os.getenv('AUTHORIZED_EMAILS', '')
    if not env_emails:
        logger.info("No authorized emails in .env to migrate")
        return 0
    
    emails = [e.strip().lower() for e in env_emails.split(',') if e.strip()]
    if not emails:
        logger.info("No valid authorized emails in .env to migrate")
        return 0
    
    logger.info(f"Migrating {len(emails)} authorized emails from .env to database...")
    migrated = 0
    
    for email in emails:
        if add_authorized_email(email, added_by_user_id=None, notes="Migrated from .env"):
            migrated += 1
    
    logger.info(f"✅ Migrated {migrated}/{len(emails)} authorized emails to database")
    return migrated


# ============================================================
# EXISTING FUNCTIONS
# ============================================================

def ensure_alumni_education_column():
    """Ensure education column exists in alumni table"""
    conn = get_connection()
    try:
        # Check if column exists
        if USE_SQLITE_FALLBACK:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(alumni)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'education' not in columns:
                logger.info("🔧 Adding 'education' column to alumni table (SQLite)...")
                cursor.execute("ALTER TABLE alumni ADD COLUMN education TEXT")
                conn.commit()
        else:
            with conn.cursor() as cur:
                cur.execute("SHOW COLUMNS FROM alumni LIKE 'education'")
                if not cur.fetchone():
                    logger.info("🔧 Adding 'education' column to alumni table (MySQL)...")
                    cur.execute("ALTER TABLE alumni ADD COLUMN education TEXT")
                    conn.commit()
    except Exception as e:
        logger.error(f"Error ensuring education column: {e}")
    finally:
        if conn:
            conn.close()


def seed_alumni_data():
    """Import alumni data from CSV file. Auto-fixes empty or malformed files."""
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(backend_dir)
    csv_path = os.path.join(project_root, 'scraper', 'output', 'UNT_Alumni_Data.csv')

    # Expected columns for alumni CSV
    EXPECTED_COLUMNS = [
        'name', 'headline', 'location', 'job_title', 'company', 'major', 'degree',
        'graduation_year', 'profile_url', 'scraped_at', 'school_start_date',
        'job_start_date', 'job_end_date', 'working_while_studying',
        'exp2_title', 'exp2_company', 'exp2_dates',
        'exp3_title', 'exp3_company', 'exp3_dates'
    ]

    if not os.path.exists(csv_path):
        logger.warning(f"CSV file not found at {csv_path}, skipping import")
        return

    # Check if file is empty or malformed
    try:
        # Check file size first
        if os.path.getsize(csv_path) == 0:
            raise pd.errors.EmptyDataError("File is empty")
        
        df = pd.read_csv(csv_path)
        
        # Validate that required columns exist
        required_cols = ['name', 'profile_url']
        missing_required = [col for col in required_cols if col not in df.columns]
        if missing_required:
            raise ValueError(f"Missing required columns: {missing_required}")
            
    except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as e:
        logger.warning(f"⚠️ CSV file is empty or malformed: {e}")
        logger.info("🔧 Attempting to fix CSV by exporting from database...")
        
        # Try to regenerate CSV from existing database
        try:
            conn = get_connection()
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT 
                        CONCAT(first_name, ' ', last_name) as name,
                        headline, location, current_job_title as job_title, company,
                        major, degree, grad_year as graduation_year, linkedin_url as profile_url,
                        scraped_at, school_start_date, job_start_date, job_end_date,
                        working_while_studying, exp2_title, exp2_company, exp2_dates,
                        exp3_title, exp3_company, exp3_dates
                    FROM alumni
                """)
                rows = cur.fetchall()
            conn.close()
            
            if rows:
                # Export database data to CSV
                db_df = pd.DataFrame(rows)
                db_df.to_csv(csv_path, index=False)
                logger.info(f"✅ Regenerated CSV with {len(rows)} records from database")
                df = db_df
            else:
                # No data in database, create empty CSV with headers
                empty_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
                empty_df.to_csv(csv_path, index=False)
                logger.info("✅ Created empty CSV with proper headers (no data in database)")
                return  # Nothing to import
                
        except Exception as regen_err:
            logger.error(f"Failed to regenerate CSV: {regen_err}")
            # As last resort, create empty CSV with headers
            empty_df = pd.DataFrame(columns=EXPECTED_COLUMNS)
            empty_df.to_csv(csv_path, index=False)
            logger.info("✅ Created empty CSV with proper headers")
            return

    try:
        logger.info(f"✅ Importing alumni data from {csv_path}")
        logger.info(f"📊 Found {len(df)} records to import")

        conn = get_connection()
        added = 0
        updated = 0
        processed = 0

        try:
            with conn.cursor() as cur:
                for index, row in df.iterrows():
                    processed += 1

                    # Parse name
                    name = str(row['name']).strip() if pd.notna(row['name']) else ''
                    if not name:
                        continue

                    parts = name.split()
                    first_name = parts[0] if len(parts) > 0 else ''
                    last_name = ' '.join(parts[1:]) if len(parts) > 1 else ''

                    # Extract other fields
                    headline = str(row['headline']).strip() if pd.notna(row.get('headline')) else None
                    location = str(row['location']).strip() if pd.notna(row.get('location')) else None

                    # BLOCK SAMPLE USERS
                    FORBIDDEN_LOCATIONS = {
                        'Seattle, WA', 'San Jose, CA', 'San Francisco, CA', 
                        'New York, NY', 'Austin, TX', 'Palo Alto, CA', 
                        'Cupertino, CA', 'Menlo Park, CA'
                    }
                    if location and location in FORBIDDEN_LOCATIONS:
                        logger.info(f"🚫 Skipping sample user: {first_name} {last_name} from {location}")
                        continue
                    job_title = str(row['job_title']).strip() if pd.notna(row.get('job_title')) else None
                    company = str(row['company']).strip() if pd.notna(row.get('company')) else None
                    major = str(row.get('major')).strip() if pd.notna(row.get('major')) else None
                    major = str(row.get('major')).strip() if pd.notna(row.get('major')) else None
                    degree = None # User requested to remove degree variable usage

                    # Auto-infer discipline if major is not set in CSV
                    if not major:
                        major = infer_discipline(degree, job_title, headline)
                    grad_year = int(row['graduation_year']) if pd.notna(row.get('graduation_year')) else None
                    profile_url = normalize_url(row.get('profile_url'))
                    scraped_at = str(row['scraped_at']).strip() if pd.notna(row.get('scraped_at')) else None

                    # New fields (may not exist in older CSVs)
                    school_start_date = str(row.get('school_start_date', '')).strip() if pd.notna(row.get('school_start_date', None)) else None
                    job_start_date = str(row.get('job_start_date', '')).strip() if pd.notna(row.get('job_start_date', None)) else None
                    job_end_date = str(row.get('job_end_date', '')).strip() if pd.notna(row.get('job_end_date', None)) else None
                    wws_raw = row.get('working_while_studying', None)
                    working_while_studying = None
                    if pd.notna(wws_raw):
                        if isinstance(wws_raw, str):
                            v = wws_raw.strip().lower()
                            if v in ("yes", "true", "1"):
                                working_while_studying = True
                            elif v in ("no", "false", "0"):
                                working_while_studying = False
                        elif isinstance(wws_raw, (int, float)):
                            working_while_studying = bool(int(wws_raw))

                    # Experience 2 and 3 fields
                    exp2_title = str(row.get('exp2_title', '')).strip() if pd.notna(row.get('exp2_title', None)) else None
                    exp2_company = str(row.get('exp2_company', '')).strip() if pd.notna(row.get('exp2_company', None)) else None
                    exp2_dates = str(row.get('exp2_dates', '')).strip() if pd.notna(row.get('exp2_dates', None)) else None
                    exp3_title = str(row.get('exp3_title', '')).strip() if pd.notna(row.get('exp3_title', None)) else None
                    exp3_company = str(row.get('exp3_company', '')).strip() if pd.notna(row.get('exp3_company', None)) else None
                    exp3_dates = str(row.get('exp3_dates', '')).strip() if pd.notna(row.get('exp3_dates', None)) else None

                    education = str(row.get('education', '')).strip() if pd.notna(row.get('education')) else None

                    # Insert or update into database
                    try:
                        cur.execute("""
                            INSERT INTO alumni 
                            (first_name, last_name, grad_year, degree, major, linkedin_url, current_job_title, company, location, headline, 
                             school_start_date, job_start_date, job_end_date, working_while_studying,
                             exp2_title, exp2_company, exp2_dates, exp3_title, exp3_company, exp3_dates, education,
                             scraped_at, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(linkedin_url) DO UPDATE SET
                                first_name=excluded.first_name,
                                last_name=excluded.last_name,
                                grad_year=excluded.grad_year,
                                degree=excluded.degree,
                                major=excluded.major,
                                current_job_title=excluded.current_job_title,
                                company=excluded.company,
                                location=excluded.location,
                                headline=excluded.headline,
                                school_start_date=excluded.school_start_date,
                                job_start_date=excluded.job_start_date,
                                job_end_date=excluded.job_end_date,
                                working_while_studying=excluded.working_while_studying,
                                exp2_title=excluded.exp2_title,
                                exp2_company=excluded.exp2_company,
                                exp2_dates=excluded.exp2_dates,
                                exp3_title=excluded.exp3_title,
                                exp3_company=excluded.exp3_company,
                                exp3_dates=excluded.exp3_dates,
                                education=excluded.education,
                                scraped_at=excluded.scraped_at,
                                updated_at=excluded.updated_at
                        """, (
                            first_name, last_name, grad_year, degree, major, profile_url, job_title, company, location, headline,
                            school_start_date, job_start_date, job_end_date, working_while_studying,
                            exp2_title, exp2_company, exp2_dates, exp3_title, exp3_company, exp3_dates, education,
                            scraped_at, scraped_at, scraped_at # created_at and updated_at
                        ))

                        if cur.rowcount == 1:
                            added += 1
                        elif cur.rowcount == 2:
                            updated += 1
                    except Exception as err:
                        logger.warning(f"Skipping record for {name}: {err}")
                        continue

                conn.commit()
                logger.info(f"✅ Added {added} new alumni records")
                logger.info(f"✅ Updated {updated} existing alumni records")
                logger.info(f"Successfully processed {processed} total alumni records")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Error importing alumni data: {e}")
        raise


def truncate_dot_fields():
    """Remove anything after '·' in location, company, and current_job_title"""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE alumni
                SET 
                    location = TRIM(SUBSTRING_INDEX(location, '·', 1)),
                    company = TRIM(SUBSTRING_INDEX(company, '·', 1)),
                    current_job_title = TRIM(SUBSTRING_INDEX(current_job_title, '·', 1))
                WHERE 
                    location LIKE '%·%' 
                    OR company LIKE '%·%'
                    OR current_job_title LIKE '%·%';
            """)
            conn.commit()
            logger.info("✅ Truncated '·' fields in alumni table")
    except Exception as err:
        logger.error(f"Error truncating dot fields: {err}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def cleanup_trailing_slashes():
    """Remove trailing slashes from existing URLs, handling duplicates."""
    logger.info("🧹 Cleaning up trailing slashes from URLs...")
    conn = get_connection()
    try:
        tables = ['visited_profiles', 'alumni']
        with conn.cursor() as cur:
            for table in tables:
                # Find URLs with trailing slash
                cur.execute(f"SELECT id, linkedin_url FROM {table} WHERE linkedin_url LIKE '%/'")
                rows = cur.fetchall()
                if not rows:
                    continue
                
                logger.info(f"Found {len(rows)} URLs with trailing slash in {table}")
                fixed = 0
                deleted = 0
                
                for row_id, url in rows:
                    clean_url = url.rstrip('/')
                    try:
                        # Try to update
                        cur.execute(f"UPDATE {table} SET linkedin_url = %s WHERE id = %s", (clean_url, row_id))
                        fixed += 1
                    except Exception as err:
                        # Check if it's a duplicate entry error (MySQL 1062 or SQLite constraint)
                        err_str = str(err).lower()
                        if 'duplicate' in err_str or 'unique constraint' in err_str:
                            # Collision! The clean URL matches another record.
                            # We delete the current record with the slash, keeping the other one.
                            logger.info(f"  Collision for {clean_url}. Deleting duplicate record ID {row_id}.")
                            cur.execute(f"DELETE FROM {table} WHERE id = %s", (row_id,))
                            deleted += 1
                        else:
                            logger.error(f"Failed to fix {url}: {err}")
                
                conn.commit()
                logger.info(f"✨ Fixed {fixed} URLs, Deleted {deleted} duplicates in {table}")
                
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
    finally:
        if conn: conn.close()


if __name__ == "__main__":
    try:
        # Validate environment variables
        required_vars = ['MYSQLHOST', 'MYSQLUSER', 'MYSQLPASSWORD', 'MYSQL_DATABASE']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        logger.info("All required environment variables validated")
        logger.info("Starting database initialization...")
        logger.info(f"Database '{MYSQL_DATABASE}' ensured")

        # Initialize tables
        init_db()
        ensure_alumni_timestamp_columns()
        ensure_alumni_work_school_date_columns()
        ensure_alumni_major_column()
        ensure_alumni_education_column()

        # Seed alumni data
        seed_alumni_data()
        truncate_dot_fields()
        cleanup_trailing_slashes()

        # Migrate visited_history.csv to database (one-time)
        logger.info("\n" + "="*60)
        logger.info("MIGRATING VISITED HISTORY TO DATABASE")
        logger.info("="*60)
        migrate_visited_history_csv_to_db()

        # Sync alumni to visited_profiles
        sync_alumni_to_visited_profiles()

        # Show stats
        stats = get_visited_profiles_stats()
        if stats:
            logger.info(f"\n📊 Visited Profiles Stats:")
            logger.info(f"   Total visited: {stats['total']}")
            logger.info(f"   UNT Alumni: {stats['unt_alumni']}")
            logger.info(f"   Non-UNT: {stats['non_unt']}")
            logger.info(f"   Needs update: {stats['needs_update']}")

        # Test connection
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT NOW()")
            db_time = cur.fetchone()[0]
            logger.info(f"\nDatabase connection successful. DB time: {db_time}")

            cur.execute("SELECT COUNT(*) FROM alumni")
            count = cur.fetchone()[0]
            logger.info(f"Alumni in database: {count} records")

            cur.execute("SELECT COUNT(*) FROM visited_profiles")
            visited_count = cur.fetchone()[0]
            logger.info(f"Visited profiles in database: {visited_count} records")

            if count > 0:
                cur.execute("""
                    SELECT id, first_name, last_name, current_job_title, headline, grad_year,
                           school_start_date, job_start_date, job_end_date, working_while_studying
                    FROM alumni
                    LIMIT 10
                """)
                for row in cur.fetchall():
                    (
                        alumni_id, fname, lname, job, head, grad,
                        school_start, job_start, job_end, wws
                    ) = row
                    display_job = job or head or 'None'
                    logger.info(
                        f"  - {fname} {lname} ({display_job}) - Grad: {grad} | "
                        f"SchoolStart: {school_start} | Job: {job_start}-{job_end} | WorkingWhileStudying: {wws}"
                    )

        conn.close()

        logger.info("\n" + "="*60)
        logger.info("✅ DATABASE INITIALIZATION COMPLETE")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        exit(1)