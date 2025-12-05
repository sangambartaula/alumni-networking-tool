import mysql.connector
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MySQL connection parameters
MYSQL_HOST = os.getenv('MYSQLHOST')
MYSQL_USER = os.getenv('MYSQLUSER')
MYSQL_PASSWORD = os.getenv('MYSQLPASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_PORT = int(os.getenv('MYSQLPORT', 3306))

def get_connection():
    """Get a MySQL database connection"""
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

            # Create alumni table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alumni (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    grad_year INT,
                    degree VARCHAR(255),
                    linkedin_url VARCHAR(500) NOT NULL,
                    current_job_title VARCHAR(255),
                    company VARCHAR(255),
                    location VARCHAR(255),
                    headline VARCHAR(500),
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
            """, (linkedin_url.strip(), is_unt_alum, notes))
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
                url = str(row.get('profile_url', '')).strip()
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
                    SUM(NOT is_unt_alum) as non_unt,
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
# EXISTING FUNCTIONS
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
                    
                    # Parse name
                    name = str(row['name']).strip() if pd.notna(row['name']) else ''
                    if not name:
                        continue
                    
                    parts = name.split()
                    first_name = parts[0] if len(parts) > 0 else ''
                    last_name = ' '.join(parts[1:]) if len(parts) > 1 else ''
                    
                    # Extract other fields
                    headline = str(row['headline']).strip() if pd.notna(row['headline']) else None
                    location = str(row['location']).strip() if pd.notna(row['location']) else None
                    job_title = str(row['job_title']).strip() if pd.notna(row['job_title']) else None
                    company = str(row['company']).strip() if pd.notna(row['company']) else None
                    major = str(row['major']).strip() if pd.notna(row['major']) else None
                    grad_year = int(row['graduation_year']) if pd.notna(row['graduation_year']) else None
                    profile_url = str(row['profile_url']).strip() if pd.notna(row['profile_url']) else None
                    scraped_at = str(row['scraped_at']).strip() if pd.notna(row['scraped_at']) else None
                    
                    # Insert or update into database
                    try:
                        cur.execute("""
                            INSERT INTO alumni 
                            (first_name, last_name, grad_year, degree, linkedin_url, current_job_title, company, location, headline, scraped_at, last_updated)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            first_name = VALUES(first_name),
                            last_name = VALUES(last_name),
                            grad_year = VALUES(grad_year),
                            degree = VALUES(degree),
                            current_job_title = VALUES(current_job_title),
                            company = VALUES(company),
                            location = VALUES(location),
                            headline = VALUES(headline),
                            last_updated = VALUES(last_updated)
                        """, (
                            first_name,
                            last_name,
                            grad_year,
                            major,
                            profile_url,
                            job_title,
                            company,
                            location,
                            headline,
                            scraped_at,
                            scraped_at
                        ))
                        
                        if cur.rowcount == 1:
                            added += 1
                        elif cur.rowcount == 2:
                            updated += 1
                    except mysql.connector.Error as err:
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
    except mysql.connector.Error as err:
        logger.error(f"Error truncating dot fields: {err}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


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
        
        # Seed alumni data
        seed_alumni_data()
        truncate_dot_fields()
        
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
                cur.execute("SELECT id, first_name, last_name, current_job_title, headline, grad_year FROM alumni LIMIT 10")
                for row in cur.fetchall():
                    alumni_id, fname, lname, job, head, grad = row
                    display_job = job or head or 'None'
                    logger.info(f"  - {fname} {lname} ({display_job}) - Grad: {grad}")
        
        conn.close()
        
        logger.info("\n" + "="*60)
        logger.info("✅ DATABASE INITIALIZATION COMPLETE")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        exit(1)