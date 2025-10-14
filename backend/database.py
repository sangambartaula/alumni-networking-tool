import os
from pathlib import Path
import mysql.connector
from dotenv import load_dotenv

# Load .env from repo root
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

MYSQLHOST = os.getenv("MYSQLHOST")
MYSQLUSER = os.getenv("MYSQLUSER")
MYSQLPASSWORD = os.getenv("MYSQLPASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQLPORT = int(os.getenv("MYSQLPORT", "3306"))

def get_connection(with_db=True):
    kwargs = {
        "host": MYSQLHOST,
        "user": MYSQLUSER,
        "password": MYSQLPASSWORD,
        "port": MYSQLPORT,
    }
    if with_db and MYSQL_DATABASE:
        kwargs["database"] = MYSQL_DATABASE
    return mysql.connector.connect(**kwargs)

def ensure_database():
    if not MYSQL_DATABASE:
        return
    conn = get_connection(with_db=False)
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        conn.commit()
        cur.close()
    finally:
        conn.close()

def init_db():
    ensure_database()
    conn = get_connection(with_db=True)
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alumni (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            grad_year SMALLINT UNSIGNED NULL,
            degree VARCHAR(150) NULL,
            linkedin_url VARCHAR(255) NULL,
            current_job_title VARCHAR(150) NULL,
            headline VARCHAR(255) NULL,
            location VARCHAR(150) NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_alumni_linkedin_url (linkedin_url),
            KEY idx_alumni_last_name (last_name),
            KEY idx_alumni_grad_year (grad_year),
            KEY idx_alumni_location (location)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        conn.commit()
        cur.close()
        print("alumni table is ready")
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        init_db()
        # quick connectivity check
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        print("Connected. DB time:", cur.fetchone())
        cur.close()
        conn.close()
    except mysql.connector.Error as err:
        print("Error:", err)
