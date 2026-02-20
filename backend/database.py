import mysql.connector
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def _get_or_create_normalized_entity(cur, table, column, value):
    """Inline helper to insert normalized strings and return their DB ID."""
    if not value or str(value).strip() == '': return None
    value = str(value).strip()
    try:
        cur.execute(f"INSERT INTO {table} ({column}) VALUES (%s) ON DUPLICATE KEY UPDATE {column}=VALUES({column})", (value,))
    except Exception:
        # SQLite fallback syntax
        cur.execute(f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?)", (value,))
    
    try:
        cur.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
    except Exception:
        cur.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))
    
    row = cur.fetchone()
    if not row: return None
    return row['id'] if isinstance(row, dict) else row[0]

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

            # Create authorized_emails table (full schema with tracking fields)
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
                    working_while_studying_status VARCHAR(20) DEFAULT NULL,
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_user_alumni (user_id, alumni_id),
                    CONSTRAINT fk_notes_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    CONSTRAINT fk_notes_alumni_id FOREIGN KEY (alumni_id) REFERENCES alumni(id) ON DELETE CASCADE
                )
            """)
            logger.info("notes table created/verified")

            # Create normalized_job_titles lookup table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS normalized_job_titles (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    normalized_title VARCHAR(255) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_normalized_title (normalized_title)
                )
            """)
            logger.info("normalized_job_titles table created/verified")

            # Create normalized_degrees lookup table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS normalized_degrees (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    normalized_degree VARCHAR(255) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_normalized_degree (normalized_degree)
                )
            """)
            logger.info("normalized_degrees table created/verified")

            # Create normalized_companies lookup table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS normalized_companies (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    normalized_company VARCHAR(255) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_normalized_company (normalized_company)
                )
            """)
            logger.info("normalized_companies table created/verified")

            conn.commit()
            logger.info("All tables initialized successfully")

    except mysql.connector.Error as err:
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

def ensure_normalized_job_title_column():
    """Ensure normalized_job_title_id column exists in alumni table."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    ALTER TABLE alumni
                    ADD COLUMN normalized_job_title_id INT DEFAULT NULL
                """)
                logger.info("Added normalized_job_title_id column to alumni table")
            except mysql.connector.Error as err:
                if "Duplicate column name" in str(err):
                    logger.info("normalized_job_title_id column already exists")
                else:
                    raise
            except Exception as err:
                # SQLite fallback: "duplicate column name" phrasing differs
                if "duplicate column name" in str(err).lower():
                    logger.info("normalized_job_title_id column already exists (SQLite)")
                else:
                    raise
            conn.commit()
    except Exception as e:
        logger.error(f"Error ensuring normalized_job_title_id column: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def ensure_normalized_degree_column():
    """Ensure normalized_degree_id and raw_degree columns exist in alumni table."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            for col_name, col_def in [
                ("normalized_degree_id", "INT DEFAULT NULL"),
                ("raw_degree", "VARCHAR(255) DEFAULT NULL"),
            ]:
                try:
                    cur.execute(f"""
                        ALTER TABLE alumni
                        ADD COLUMN {col_name} {col_def}
                    """)
                    logger.info(f"Added {col_name} column to alumni table")
                except mysql.connector.Error as err:
                    if "Duplicate column name" in str(err):
                        logger.info(f"{col_name} column already exists")
                    else:
                        raise
                except Exception as err:
                    if "duplicate column name" in str(err).lower():
                        logger.info(f"{col_name} column already exists (SQLite)")
                    else:
                        raise
            conn.commit()
    except Exception as e:
        logger.error(f"Error ensuring degree columns: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def ensure_normalized_company_column():
    """Ensure normalized_company_id column exists in alumni table."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    ALTER TABLE alumni
                    ADD COLUMN normalized_company_id INT DEFAULT NULL
                """)
                logger.info("Added normalized_company_id column to alumni table")
            except mysql.connector.Error as err:
                if "Duplicate column name" in str(err):
                    logger.info("normalized_company_id column already exists")
                else:
                    raise
            except Exception as err:
                if "duplicate column name" in str(err).lower():
                    logger.info("normalized_company_id column already exists (SQLite)")
                else:
                    raise
            conn.commit()
    except Exception as e:
        logger.error(f"Error ensuring normalized_company_id column: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

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
            except mysql.connector.Error as err:
                if "Duplicate column name" in str(err):
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
            except mysql.connector.Error as err:
                if "Duplicate column name" in str(err):
                    logger.info("last_updated column already exists")
                else:
                    raise

            conn.commit()
    except mysql.connector.Error as err:
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
                except mysql.connector.Error as err:
                    if "Duplicate column name" in str(err):
                        logger.info(f"{name} column already exists")
                    else:
                        raise

            add_col("ALTER TABLE alumni ADD COLUMN school_start_date VARCHAR(20) DEFAULT NULL", "school_start_date")
            add_col("ALTER TABLE alumni ADD COLUMN job_start_date VARCHAR(20) DEFAULT NULL", "job_start_date")
            add_col("ALTER TABLE alumni ADD COLUMN job_end_date VARCHAR(20) DEFAULT NULL", "job_end_date")
            add_col("ALTER TABLE alumni ADD COLUMN working_while_studying BOOLEAN DEFAULT NULL", "working_while_studying")
            add_col("ALTER TABLE alumni ADD COLUMN working_while_studying_status VARCHAR(20) DEFAULT NULL", "working_while_studying_status")
            
            # Experience 2 and 3 columns
            add_col("ALTER TABLE alumni ADD COLUMN exp2_title VARCHAR(255) DEFAULT NULL", "exp2_title")
            add_col("ALTER TABLE alumni ADD COLUMN exp2_company VARCHAR(255) DEFAULT NULL", "exp2_company")
            add_col("ALTER TABLE alumni ADD COLUMN exp2_dates VARCHAR(50) DEFAULT NULL", "exp2_dates")
            add_col("ALTER TABLE alumni ADD COLUMN exp3_title VARCHAR(255) DEFAULT NULL", "exp3_title")
            add_col("ALTER TABLE alumni ADD COLUMN exp3_company VARCHAR(255) DEFAULT NULL", "exp3_company")
            add_col("ALTER TABLE alumni ADD COLUMN exp3_dates VARCHAR(50) DEFAULT NULL", "exp3_dates")

            conn.commit()
    except mysql.connector.Error as err:
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
            except mysql.connector.Error as err:
                if "Duplicate column name" in str(err):
                    logger.info("Major column already exists")
                else:
                    raise
            conn.commit()
    except mysql.connector.Error as err:
        logger.error(f"Error ensuring major column: {err}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def ensure_education_columns():
    """Ensure all education refactor columns exist: school*, degree*, major*, standardized_*."""
    NEW_COLS = [
        # Raw columns
        ("school",                "VARCHAR(255) DEFAULT NULL"),
        ("school2",               "VARCHAR(255) DEFAULT NULL"),
        ("school3",               "VARCHAR(255) DEFAULT NULL"),
        ("degree2",               "VARCHAR(255) DEFAULT NULL"),
        ("degree3",               "VARCHAR(255) DEFAULT NULL"),
        ("major2",                "VARCHAR(255) DEFAULT NULL"),
        ("major3",                "VARCHAR(255) DEFAULT NULL"),
        # Standardized columns
        ("standardized_degree",   "VARCHAR(50) DEFAULT NULL"),
        ("standardized_degree2",  "VARCHAR(50) DEFAULT NULL"),
        ("standardized_degree3",  "VARCHAR(50) DEFAULT NULL"),
        ("standardized_major",    "VARCHAR(255) DEFAULT NULL"),
        ("standardized_major2",   "VARCHAR(255) DEFAULT NULL"),
        ("standardized_major3",   "VARCHAR(255) DEFAULT NULL"),
    ]
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            for col_name, col_def in NEW_COLS:
                try:
                    cur.execute(f"ALTER TABLE alumni ADD COLUMN {col_name} {col_def}")
                    logger.info(f"Added {col_name} column to alumni table")
                except mysql.connector.Error as err:
                    if "Duplicate column name" in str(err):
                        pass  # already exists
                    else:
                        raise
                except Exception as err:
                    if "duplicate column name" in str(err).lower():
                        pass  # SQLite: already exists
                    else:
                        raise

            # Migrate: copy education → school where school is still NULL
            try:
                cur.execute("""
                    UPDATE alumni
                    SET school = education
                    WHERE school IS NULL AND education IS NOT NULL
                """)
                migrated = cur.rowcount
                if migrated:
                    logger.info(f"Migrated {migrated} rows: education → school")
            except Exception as e:
                logger.warning(f"education → school migration skipped: {e}")

            conn.commit()
            logger.info("✅ Education columns ensured")
    except Exception as e:
        logger.error(f"Error ensuring education columns: {e}")
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

    except mysql.connector.Error as err:
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

    except mysql.connector.Error as err:
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

    except mysql.connector.Error as err:
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

    except mysql.connector.Error as err:
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
                except mysql.connector.Error as err:
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
                    SUM(CASE WHEN is_unt_alum = 0 THEN 1 ELSE 0 END) as non_unt,
                    SUM(needs_update) as needs_update
                FROM visited_profiles
            """)
            stats = cur.fetchone()

        return stats

    except mysql.connector.Error as err:
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
            # SQLite mode — use dictionary cursor for dict results
            with conn.cursor(dictionary=True) as cursor:
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

def seed_alumni_data():
    """Import alumni data from CSV file"""
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(backend_dir)
    csv_path = os.path.join(project_root, 'scraper', 'output', 'UNT_Alumni_Data.csv')

    if not os.path.exists(csv_path):
        logger.warning(f"CSV file not found at {csv_path}, skipping import")
        return

    try:
        df = pd.read_csv(csv_path)
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

                    # Parse name (Handle New 'first', 'last' OR Old 'name')
                    first_name = str(row.get('first', '')).strip() if pd.notna(row.get('first')) else ''
                    last_name = str(row.get('last', '')).strip() if pd.notna(row.get('last')) else ''
                    
                    if not first_name and not last_name:
                        # Fallback to old 'name' column
                        name = str(row.get('name', '')).strip() if pd.notna(row.get('name')) else ''
                        if name:
                            parts = name.split()
                            first_name = parts[0]
                            last_name = ' '.join(parts[1:]) if len(parts) > 1 else ''

                    if not first_name and not last_name:
                        continue

                    # Extract fields with New/Old keys
                    headline = str(row.get('headline', '')).strip() if pd.notna(row.get('headline')) else None
                    location = str(row.get('location', '')).strip() if pd.notna(row.get('location')) else None
                    
                    # title (new) vs job_title (old)
                    job_title = str(row.get('title', '')).strip() if pd.notna(row.get('title')) else \
                                str(row.get('job_title', '')).strip() if pd.notna(row.get('job_title')) else None

                    company = str(row.get('company', '')).strip() if pd.notna(row.get('company')) else None
                    major = str(row.get('major', '')).strip() if pd.notna(row.get('major')) else None
                    degree = str(row.get('degree', '')).strip() if pd.notna(row.get('degree')) else None

                    # school (new) vs education (old)
                    school = str(row.get('school', '')).strip() if pd.notna(row.get('school')) else \
                             str(row.get('education', '')).strip() if pd.notna(row.get('education')) else None

                    # Education entries 2 and 3
                    school2 = str(row.get('school2', '')).strip() if pd.notna(row.get('school2')) else None
                    school3 = str(row.get('school3', '')).strip() if pd.notna(row.get('school3')) else None
                    degree2 = str(row.get('degree2', '')).strip() if pd.notna(row.get('degree2')) else None
                    degree3 = str(row.get('degree3', '')).strip() if pd.notna(row.get('degree3')) else None
                    major2 = str(row.get('major2', '')).strip() if pd.notna(row.get('major2')) else None
                    major3 = str(row.get('major3', '')).strip() if pd.notna(row.get('major3')) else None

                    # Use discipline already computed by scraper at scrape time.
                    saved_discipline = str(row.get('discipline', '')).strip() if pd.notna(row.get('discipline')) else ''
                    if saved_discipline:
                        major = saved_discipline
                    
                    # grad_year (new) vs graduation_year (old)
                    grad_year = None
                    if pd.notna(row.get('grad_year')):
                        grad_year = int(row['grad_year'])
                    elif pd.notna(row.get('graduation_year')):
                        grad_year = int(row['graduation_year'])

                    # linkedin_url (new) vs profile_url (old)
                    raw_url = row.get('linkedin_url') if pd.notna(row.get('linkedin_url')) else row.get('profile_url')
                    profile_url = normalize_url(raw_url)
                    
                    scraped_at = str(row.get('scraped_at', '')).strip() if pd.notna(row.get('scraped_at')) else None

                    # New fields (may not exist in older CSVs)
                    # school_start (new) vs school_start_date (old)
                    school_start_date = str(row.get('school_start', '')).strip() if pd.notna(row.get('school_start')) else \
                                        str(row.get('school_start_date', '')).strip() if pd.notna(row.get('school_start_date')) else None

                    # job_start (new) vs job_start_date (old)
                    job_start_date = str(row.get('job_start', '')).strip() if pd.notna(row.get('job_start')) else \
                                     str(row.get('job_start_date', '')).strip() if pd.notna(row.get('job_start_date')) else None

                    # job_end (new) vs job_end_date (old)
                    job_end_date = str(row.get('job_end', '')).strip() if pd.notna(row.get('job_end')) else \
                                   str(row.get('job_end_date', '')).strip() if pd.notna(row.get('job_end_date')) else None

                    wws_raw = row.get('working_while_studying', None)
                    working_while_studying = None
                    working_while_studying_status = None
                    if pd.notna(wws_raw):
                        if isinstance(wws_raw, str):
                            v = wws_raw.strip().lower()
                            if v in ("yes", "currently", "true", "1"):
                                # "currently" = actively working while still studying → treat as True
                                working_while_studying = True
                                working_while_studying_status = v if v in ("yes", "no", "currently") else "yes"
                            elif v in ("no", "false", "0"):
                                working_while_studying = False
                                working_while_studying_status = "no"
                        elif isinstance(wws_raw, (int, float, bool)):
                            working_while_studying = bool(wws_raw)
                            working_while_studying_status = "yes" if working_while_studying else "no"

                    # Experience 2 and 3 fields (New: exp_2_title vs Old: exp2_title)
                    exp2_title = str(row.get('exp_2_title', '')).strip() if pd.notna(row.get('exp_2_title')) else \
                                 str(row.get('exp2_title', '')).strip() if pd.notna(row.get('exp2_title'),) else None
                    
                    exp2_company = str(row.get('exp_2_company', '')).strip() if pd.notna(row.get('exp_2_company')) else \
                                   str(row.get('exp2_company', '')).strip() if pd.notna(row.get('exp2_company')) else None
                    
                    exp2_dates = str(row.get('exp_2_dates', '')).strip() if pd.notna(row.get('exp_2_dates')) else \
                                 str(row.get('exp2_dates', '')).strip() if pd.notna(row.get('exp2_dates')) else None
                    
                    exp3_title = str(row.get('exp_3_title', '')).strip() if pd.notna(row.get('exp_3_title')) else \
                                 str(row.get('exp3_title', '')).strip() if pd.notna(row.get('exp3_title')) else None
                    
                    exp3_company = str(row.get('exp_3_company', '')).strip() if pd.notna(row.get('exp_3_company')) else \
                                   str(row.get('exp3_company', '')).strip() if pd.notna(row.get('exp3_company')) else None
                    
                    exp3_dates = str(row.get('exp_3_dates', '')).strip() if pd.notna(row.get('exp_3_dates')) else \
                                 str(row.get('exp3_dates', '')).strip() if pd.notna(row.get('exp3_dates')) else None

                    # Read standardized values from CSV (now handled purely by scraper)
                    std_degree = str(row.get('standardized_degree', '')).strip() if pd.notna(row.get('standardized_degree')) else None
                    std_degree2 = str(row.get('standardized_degree2', '')).strip() if pd.notna(row.get('standardized_degree2')) else None
                    std_degree3 = str(row.get('standardized_degree3', '')).strip() if pd.notna(row.get('standardized_degree3')) else None
                    
                    std_major = str(row.get('standardized_major', '')).strip() if pd.notna(row.get('standardized_major')) else None
                    std_major2 = str(row.get('standardized_major2', '')).strip() if pd.notna(row.get('standardized_major2')) else None
                    std_major3 = str(row.get('standardized_major3', '')).strip() if pd.notna(row.get('standardized_major3')) else None

                    # Insert or update into database
                    try:
                        # Get normalized job title and company IDs directly using SQL helper
                        norm_title = str(row.get('normalized_job_title', '')).strip() if pd.notna(row.get('normalized_job_title')) else None
                        norm_title_id = _get_or_create_normalized_entity(cur, 'normalized_job_titles', 'normalized_title', norm_title)

                        norm_comp = str(row.get('normalized_company', '')).strip() if pd.notna(row.get('normalized_company')) else None
                        norm_company_id = _get_or_create_normalized_entity(cur, 'normalized_companies', 'normalized_company', norm_comp)

                        cur.execute("""
                            INSERT INTO alumni 
                            (first_name, last_name, grad_year, degree, major, linkedin_url, current_job_title, company, location, headline, 
                             school_start_date, job_start_date, job_end_date, working_while_studying, working_while_studying_status,
                             exp2_title, exp2_company, exp2_dates, exp3_title, exp3_company, exp3_dates,
                             school, school2, school3, degree2, degree3, major2, major3,
                             standardized_degree, standardized_degree2, standardized_degree3,
                             standardized_major, standardized_major2, standardized_major3,
                             scraped_at, last_updated, normalized_job_title_id, normalized_company_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s,
                                    %s, %s, %s,
                                    %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                first_name=VALUES(first_name),
                                last_name=VALUES(last_name),
                                grad_year=VALUES(grad_year),
                                degree=VALUES(degree),
                                major=VALUES(major),
                                current_job_title=VALUES(current_job_title),
                                company=VALUES(company),
                                location=VALUES(location),
                                headline=VALUES(headline),
                                school_start_date=VALUES(school_start_date),
                                job_start_date=VALUES(job_start_date),
                                job_end_date=VALUES(job_end_date),
                                working_while_studying=VALUES(working_while_studying),
                                working_while_studying_status=VALUES(working_while_studying_status),
                                exp2_title=VALUES(exp2_title),
                                exp2_company=VALUES(exp2_company),
                                exp2_dates=VALUES(exp2_dates),
                                exp3_title=VALUES(exp3_title),
                                exp3_company=VALUES(exp3_company),
                                exp3_dates=VALUES(exp3_dates),
                                school=VALUES(school),
                                school2=VALUES(school2),
                                school3=VALUES(school3),
                                degree2=VALUES(degree2),
                                degree3=VALUES(degree3),
                                major2=VALUES(major2),
                                major3=VALUES(major3),
                                standardized_degree=VALUES(standardized_degree),
                                standardized_degree2=VALUES(standardized_degree2),
                                standardized_degree3=VALUES(standardized_degree3),
                                standardized_major=VALUES(standardized_major),
                                standardized_major2=VALUES(standardized_major2),
                                standardized_major3=VALUES(standardized_major3),
                                last_updated=VALUES(last_updated),
                                normalized_job_title_id=COALESCE(VALUES(normalized_job_title_id), normalized_job_title_id),
                                normalized_company_id=COALESCE(VALUES(normalized_company_id), normalized_company_id)
                        """, (
                            first_name,
                            last_name,
                            grad_year,
                            degree,
                            major,
                            profile_url,
                            job_title,
                            company,
                            location,
                            headline,
                            school_start_date,
                            job_start_date,
                            job_end_date,
                            working_while_studying,
                            working_while_studying_status,
                            exp2_title,
                            exp2_company,
                            exp2_dates,
                            exp3_title,
                            exp3_company,
                            exp3_dates,
                            school,
                            school2,
                            school3,
                            degree2,
                            degree3,
                            major2,
                            major3,
                            std_degree,
                            std_degree2,
                            std_degree3,
                            std_major,
                            std_major2,
                            std_major3,
                            scraped_at,
                            scraped_at,
                            norm_title_id,
                            norm_company_id
                        ))

                        if cur.rowcount == 1:
                            added += 1
                        elif cur.rowcount == 2:
                            updated += 1
                    except Exception as err:
                        logger.warning(f"Skipping record for {first_name} {last_name}: {err}")
                        if "Lost connection" in str(err) or "MySQL Connection not available" in str(err):
                            logger.error("🛑 MySQL connection lost. Exiting loop to save progress.")
                            break
                        continue

                    # Incremental commit every 5 rows to prevent total data loss on timeout
                    if processed % 5 == 0:
                        try:
                            conn.commit()
                            logger.info(f"💾 Auto-committed batch at record {processed}")
                        except Exception as commit_err:
                            logger.error(f"❌ Auto-commit failed: {commit_err}")

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
    except mysql.connector.Error as err:
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
                        err_str = str(err)
                        # MySQL errno 1062 = Duplicate entry; SQLite raises IntegrityError
                        is_duplicate = (
                            (hasattr(err, 'errno') and err.errno == 1062) or
                            'UNIQUE constraint' in err_str
                        )
                        if is_duplicate:
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
        ensure_education_columns()

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