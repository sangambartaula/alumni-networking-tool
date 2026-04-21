try:
    from .db_core_common import *
except ImportError:
    from db_core_common import *

def init_db():
    """Initialize database tables if they don't exist"""
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
            # Create users table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    linkedin_id VARCHAR(255) UNIQUE NOT NULL,
                    email VARCHAR(255),
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    password_hash VARCHAR(255) DEFAULT NULL,
                    auth_type VARCHAR(20) DEFAULT 'linkedin_only',
                    role VARCHAR(10) DEFAULT 'user',
                    must_change_password BOOLEAN DEFAULT FALSE,
                    failed_attempts INT DEFAULT 0,
                    lock_until DATETIME DEFAULT NULL,
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

            # Create scraper_activity table (internal tracking: who scraped how much)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scraper_activity (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    profiles_scraped INT DEFAULT 0,
                    last_scraped_at TIMESTAMP NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_scraper_email (email)
                )
            """)
            logger.info("scraper_activity table created/verified")

            # Seed scraper_activity with all authorized emails (initialized to 0)
            cur.execute("""
                INSERT IGNORE INTO scraper_activity (email)
                SELECT email FROM authorized_emails
            """)

            # Create scrape_runs table (one row per scraper run invocation)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_uuid VARCHAR(64) UNIQUE NOT NULL,
                    scraper_email VARCHAR(255),
                    scraper_mode VARCHAR(50),
                    selected_disciplines VARCHAR(500),
                    status VARCHAR(20) DEFAULT 'running',
                    profiles_scraped INT DEFAULT 0,
                    cloud_disabled BOOLEAN DEFAULT FALSE,
                    geocode_unknown_count INT DEFAULT 0,
                    geocode_network_failure_count INT DEFAULT 0,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP NULL,
                    notes VARCHAR(500),
                    INDEX idx_scrape_runs_started_at (started_at),
                    INDEX idx_scrape_runs_email (scraper_email)
                )
            """)
            logger.info("scrape_runs table created/verified")

            # Create scrape_run_flags table (flagged profile reasons by run)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scrape_run_flags (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    scrape_run_id INT NOT NULL,
                    linkedin_url VARCHAR(500) NOT NULL,
                    reason VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_run_flag (scrape_run_id, linkedin_url, reason),
                    INDEX idx_run_flags_url (linkedin_url),
                    CONSTRAINT fk_run_flags_scrape_run_id
                        FOREIGN KEY (scrape_run_id) REFERENCES scrape_runs(id)
                        ON DELETE CASCADE
                )
            """)
            logger.info("scrape_run_flags table created/verified")


            # Create alumni table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alumni (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    grad_year INT,
                    degree VARCHAR(255),
                    major VARCHAR(255) DEFAULT NULL,
                    discipline VARCHAR(255) DEFAULT NULL,
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
                    scrape_run_id INT DEFAULT NULL,
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
                    # We normalize the URL to ensure 'linkedin.com/in/user/' 
                    # matches 'linkedin.com/in/user' without a slash.
                    linkedin_url VARCHAR(500) NOT NULL UNIQUE,
                    is_unt_alum BOOLEAN DEFAULT FALSE,
                    last_scrape_run_id INT DEFAULT NULL,
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

            # Performance indexes for common dashboard and notes queries.
            # MySQL variants may not support "CREATE INDEX IF NOT EXISTS", so
            # we create directly and ignore duplicate-index errors.
            index_definitions = [
                ("idx_alumni_name_sort", "alumni", "last_name, first_name, id"),
                ("idx_alumni_updated_sort", "alumni", "updated_at, id"),
                ("idx_user_interactions_user_updated", "user_interactions", "user_id, updated_at"),
                ("idx_user_interactions_user_alumni_type", "user_interactions", "user_id, alumni_id, interaction_type"),
                ("idx_notes_user_alumni_lookup", "notes", "user_id, alumni_id"),
            ]
            for index_name, table_name, columns in index_definitions:
                statement = f"CREATE INDEX {index_name} ON {table_name}({columns})"
                try:
                    cur.execute(statement)
                except mysql.connector.Error as idx_err:
                    # MySQL duplicate index name.
                    if getattr(idx_err, "errno", None) == 1061 or "Duplicate key name" in str(idx_err):
                        logger.debug(f"Index already exists: {index_name}")
                    else:
                        logger.warning(f"Index ensure skipped for statement '{statement}': {idx_err}")
                except Exception as idx_err:
                    # SQLite fallback / generic duplicate index phrasing.
                    if "already exists" in str(idx_err).lower():
                        logger.debug(f"Index already exists: {index_name}")
                    else:
                        logger.warning(f"Index ensure skipped for statement '{statement}': {idx_err}")
            logger.info("All tables initialized successfully")

    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        raise


# ============================================================
# MIGRATIONS / COLUMN ENSURANCE
# ============================================================

def ensure_normalized_job_title_column():
    """
    Ensure normalized_job_title_id column exists in alumni table.
    This is part of our retroactive normalization strategy: first we add
    the ID column, then we run a migration script to populate it based on 
    raw text titles.
    """
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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
    except Exception as e:
        logger.error(f"Error ensuring normalized_job_title_id column: {e}")

def ensure_normalized_degree_column():
    """Ensure normalized_degree_id and raw_degree columns exist in alumni table."""
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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
    except Exception as e:
        logger.error(f"Error ensuring degree columns: {e}")

def ensure_normalized_company_column():
    """Ensure normalized_company_id column exists in alumni table."""
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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
    except Exception as e:
        logger.error(f"Error ensuring normalized_company_id column: {e}")

def ensure_alumni_timestamp_columns():
    """Ensure scraped_at and last_updated columns exist in alumni table"""
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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
    except mysql.connector.Error as err:
        logger.error(f"Error ensuring timestamp columns: {err}")
        raise


def ensure_alumni_work_school_date_columns():
    """Ensure columns for school and job dates exist in the alumni table."""
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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
    except mysql.connector.Error as err:
        logger.error(f"Error ensuring work/school date columns: {err}")
        raise


def ensure_alumni_major_column():
    """
    Ensure major column exists in alumni table for major text.
    Engineering discipline is stored in the separate `discipline` column.
    """
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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
    except mysql.connector.Error as err:
        logger.error(f"Error ensuring major column: {err}")
        raise


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
        # Secondary major for multi-entry mapping (CS&E -> CS + CE)
        ("standardized_major_alt","VARCHAR(255) DEFAULT NULL"),
        ("discipline",            "VARCHAR(255) DEFAULT NULL"),
    ]
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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

            # Migrate: copy education ΓåÆ school where school is still NULL
            try:
                cur.execute("""
                    UPDATE alumni
                    SET school = education
                    WHERE school IS NULL AND education IS NOT NULL
                """)
                migrated = cur.rowcount
                if migrated:
                    logger.info(f"Migrated {migrated} rows: education ΓåÆ school")
            except Exception as e:
                logger.warning(f"education ΓåÆ school migration skipped: {e}")
            logger.info("Education columns ensured")
    except Exception as e:
        logger.error(f"Error ensuring education columns: {e}")


def ensure_experience_analysis_columns():
    """Ensure columns for job relevance scoring, relevant experience months, and seniority level exist."""
    NEW_COLS = [
        ("job_1_relevance_score", "FLOAT DEFAULT NULL"),
        ("job_2_relevance_score", "FLOAT DEFAULT NULL"),
        ("job_3_relevance_score", "FLOAT DEFAULT NULL"),
        ("job_1_is_relevant",     "BOOLEAN DEFAULT NULL"),
        ("job_2_is_relevant",     "BOOLEAN DEFAULT NULL"),
        ("job_3_is_relevant",     "BOOLEAN DEFAULT NULL"),
        ("relevant_experience_months", "INT DEFAULT NULL"),
        ("seniority_level",       "VARCHAR(20) DEFAULT NULL"),
        ("job_employment_type",   "VARCHAR(120) DEFAULT NULL"),
        ("exp2_employment_type",  "VARCHAR(120) DEFAULT NULL"),
        ("exp3_employment_type",  "VARCHAR(120) DEFAULT NULL"),
    ]
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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
            logger.info("Experience analysis columns ensured")
    except Exception as e:
        logger.error(f"Error ensuring experience analysis columns: {e}")


def ensure_scrape_run_tracking_schema():
    """Ensure scrape run tracking tables and run_id linkage columns exist."""
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
            # Ensure tables for run metadata and run-level flagged entries.
            try:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scrape_runs (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        run_uuid VARCHAR(64) UNIQUE NOT NULL,
                        scraper_email VARCHAR(255),
                        scraper_mode VARCHAR(50),
                        selected_disciplines VARCHAR(500),
                        status VARCHAR(20) DEFAULT 'running',
                        profiles_scraped INT DEFAULT 0,
                        cloud_disabled BOOLEAN DEFAULT FALSE,
                        geocode_unknown_count INT DEFAULT 0,
                        geocode_network_failure_count INT DEFAULT 0,
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        completed_at TIMESTAMP NULL,
                        notes VARCHAR(500)
                    )
                    """
                )
            except Exception:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scrape_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_uuid TEXT UNIQUE NOT NULL,
                        scraper_email TEXT,
                        scraper_mode TEXT,
                        selected_disciplines TEXT,
                        status TEXT DEFAULT 'running',
                        profiles_scraped INTEGER DEFAULT 0,
                        cloud_disabled INTEGER DEFAULT 0,
                        geocode_unknown_count INTEGER DEFAULT 0,
                        geocode_network_failure_count INTEGER DEFAULT 0,
                        started_at TEXT DEFAULT (datetime('now')),
                        completed_at TEXT,
                        notes TEXT
                    )
                    """
                )

            try:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scrape_run_flags (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        scrape_run_id INT NOT NULL,
                        linkedin_url VARCHAR(500) NOT NULL,
                        reason VARCHAR(255) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uq_run_flag (scrape_run_id, linkedin_url, reason)
                    )
                    """
                )
            except Exception:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scrape_run_flags (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        scrape_run_id INTEGER NOT NULL,
                        linkedin_url TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        created_at TEXT DEFAULT (datetime('now')),
                        UNIQUE(scrape_run_id, linkedin_url, reason)
                    )
                    """
                )

            for statement in (
                "ALTER TABLE alumni ADD COLUMN scrape_run_id INT DEFAULT NULL",
                "ALTER TABLE visited_profiles ADD COLUMN last_scrape_run_id INT DEFAULT NULL",
            ):
                try:
                    cur.execute(statement)
                except mysql.connector.Error as err:
                    if "Duplicate column name" in str(err):
                        pass
                    else:
                        raise
                except Exception as err:
                    if "duplicate column name" in str(err).lower():
                        pass
                    else:
                        raise
            logger.info("Scrape run tracking schema ensured")
    except Exception as e:
        logger.error(f"Error ensuring scrape run tracking schema: {e}")


def ensure_all_alumni_schema_migrations():
    """
    Apply every idempotent ALTER TABLE migration for the `alumni` table.

    Call this after init_db() whenever the web app (or CLI) starts. init_db()
    only creates missing tables; it does not add new columns to existing DBs.
    When you introduce a new alumni column, add it via a dedicated ensure_*
    function above, then register that function here so startup stays in sync
    with SELECT/INSERT in app code.
    """
    ensure_alumni_timestamp_columns()
    ensure_alumni_work_school_date_columns()
    ensure_alumni_major_column()
    ensure_education_columns()
    ensure_normalized_job_title_column()
    ensure_normalized_degree_column()
    ensure_normalized_company_column()
    ensure_experience_analysis_columns()
    ensure_scrape_run_tracking_schema()


# ============================================================
# VISITED PROFILES FUNCTIONS
# ============================================================

