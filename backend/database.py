import os
from pathlib import Path
import mysql.connector
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env from repo root
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Required environment variables
REQUIRED_ENV_VARS = [
    "MYSQLHOST",
    "MYSQLUSER",
    "MYSQLPASSWORD",
    "MYSQL_DATABASE"
]

def validate_env_variables():
    """Verify all required .env variables exist at startup"""
    missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    logger.info("All required environment variables validated")

# Validate on import
validate_env_variables()

MYSQLHOST = os.getenv("MYSQLHOST")
MYSQLUSER = os.getenv("MYSQLUSER")
MYSQLPASSWORD = os.getenv("MYSQLPASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQLPORT = int(os.getenv("MYSQLPORT", "3306"))

def get_connection(with_db=True):
    """Create and return a MySQL database connection"""
    kwargs = {
        "host": MYSQLHOST,
        "user": MYSQLUSER,
        "password": MYSQLPASSWORD,
        "port": MYSQLPORT,
        "connect_timeout": 10,  # 10 seconds timeout
        "connection_timeout": 10,  # 10 seconds timeout
    }
    if with_db and MYSQL_DATABASE:
        kwargs["database"] = MYSQL_DATABASE
    try:
        return mysql.connector.connect(**kwargs)
    except mysql.connector.Error as err:
        if err.errno == 2003:  # Can't connect to MySQL server
            logger.error(f"Cannot connect to MySQL server at {MYSQLHOST}:{MYSQLPORT}. Please check:")
            logger.error("1. The RDS instance is running and accessible")
            logger.error("2. The security group allows inbound traffic from your IP")
            logger.error("3. The VPC and subnet settings are correct")
        raise

def ensure_database():
    """Create database if it doesn't exist"""
    if not MYSQL_DATABASE:
        logger.warning("MYSQL_DATABASE not set, skipping database creation")
        return
    
    conn = None
    try:
        conn = get_connection(with_db=False)
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            conn.commit()
        logger.info(f"Database '{MYSQL_DATABASE}' ensured")
    except mysql.connector.Error as err:
        logger.error(f"Error ensuring database: {err}")
        raise
    finally:
        if conn:
            conn.close()

def init_db():
    """Initialize all database tables"""
    try:
        ensure_database()
    except Exception as err:
        logger.error(f"Failed to ensure database: {err}")
        raise
    
    conn = None
    try:
        conn = get_connection(with_db=True)
        with conn.cursor() as cur:
            # Alumni table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS alumni (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                grad_year SMALLINT UNSIGNED NULL,
                degree VARCHAR(150) NULL,
                linkedin_url VARCHAR(255) NULL,
                current_job_title VARCHAR(150) NULL,
                company VARCHAR(150) NULL,
                location VARCHAR(150) NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_alumni_linkedin_url (linkedin_url),
                KEY idx_alumni_last_name (last_name),
                KEY idx_alumni_grad_year (grad_year),
                KEY idx_alumni_location (location)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("alumni table created/verified")
            
            # Users table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                linkedin_id VARCHAR(255) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL UNIQUE,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                headline VARCHAR(255),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                KEY idx_users_email (email),
                KEY idx_users_linkedin_id (linkedin_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("users table created/verified")
            
            # User interactions table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS user_interactions (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT UNSIGNED NOT NULL,
                alumni_id BIGINT UNSIGNED NOT NULL,
                interaction_type ENUM('bookmarked', 'connected') NOT NULL,
                notes LONGTEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (alumni_id) REFERENCES alumni(id) ON DELETE CASCADE,
                UNIQUE KEY uq_user_alumni_interaction (user_id, alumni_id, interaction_type),
                KEY idx_user_id (user_id),
                KEY idx_alumni_id (alumni_id),
                KEY idx_interaction_type (interaction_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            logger.info("user_interactions table created/verified")
        
        # Batch commit after all tables
        conn.commit()
        logger.info("All tables initialized successfully")
        
    except mysql.connector.Error as err:
        logger.error(f"MySQL error during table initialization: {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error during table initialization: {err}")
        raise
    finally:
        if conn:
            conn.close()

def import_alumni_csv(csv_path):
    """Import alumni data from UNT_Alumni_Data.csv into the database"""
    import pandas as pd
    
    logger.info(f"Importing alumni data from {csv_path}")
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Process each row
        alumni_data = []
        for _, row in df.iterrows():
            # Split name into first and last name
            name_parts = row['name'].strip().split(maxsplit=1)
            first_name = name_parts[0] if name_parts else ""
            last_name = name_parts[1] if len(name_parts) > 1 else ""
            
            # Convert graduation year to integer or None
            try:
                grad_year = int(row['graduation_year']) if pd.notna(row['graduation_year']) else None
            except (ValueError, TypeError):
                grad_year = None
            
            # Create alumni record
            alumni_record = (
                None,  # id will be auto-generated
                first_name,
                last_name,
                grad_year,
                row['major'] if pd.notna(row['major']) else None,
                row['profile_url'] if pd.notna(row['profile_url']) else None,
                row['job_title'] if pd.notna(row['job_title']) else None,
                row['company'] if pd.notna(row['company']) else None,
                row['location'] if pd.notna(row['location']) else None
            )
            alumni_data.append(alumni_record)
        
        # Insert data into database
        conn = get_connection(with_db=True)
        try:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO alumni 
                    (id, first_name, last_name, grad_year, degree, linkedin_url, current_job_title, company, location)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    grad_year = VALUES(grad_year),
                    degree = VALUES(degree),
                    current_job_title = VALUES(current_job_title),
                    company = VALUES(company),
                    location = VALUES(location),
                    updated_at = CURRENT_TIMESTAMP
                """, alumni_data)
                conn.commit()
                logger.info(f"Successfully imported {len(alumni_data)} alumni records")
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error importing alumni data: {e}")
        raise

def seed_alumni_data():
    """Import alumni data from CSV file"""
    csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scraper', 'output', 'UNT_Alumni_Data.csv')
    if os.path.exists(csv_path):
        import_alumni_csv(csv_path)
    else:
        logger.warning(f"Alumni data file not found at {csv_path}")
        logger.info("Database initialized without any seed data - waiting for real data import")
    
    # Database is initialized and ready for data import
    # No initial seed data - waiting for real alumni data

if __name__ == "__main__":
    try:
        logger.info("Starting database initialization...")
        init_db()
        seed_alumni_data()
        
        # Quick connectivity check
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT NOW();")
                db_time = cur.fetchone()[0]
                logger.info(f"Database connection successful. DB time: {db_time}")
                
                # Show alumni records
                cur.execute("SELECT id, first_name, last_name FROM alumni")
                alumni = cur.fetchall()
                logger.info(f"Alumni in database: {alumni}")
        finally:
            conn.close()
            
    except Exception as err:
        logger.error(f"Database initialization failed: {err}")
        exit(1)