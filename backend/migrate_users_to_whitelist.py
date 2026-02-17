import os
import sys
import logging

# Ensure backend directory is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from pathlib import Path

# Load .env explicitly
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# FORCE env vars for this script to ensure SQLite is used
os.environ["DISABLE_DB"] = "1"
os.environ["USE_SQLITE_FALLBACK"] = "1"

from database import get_connection, add_authorized_email

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_users_to_whitelist():
    """
    Fetch all emails from the users table and add them to the authorized_emails table.
    """
    conn = None
    try:
        conn = get_connection()
        logger.info("Connected to database.")
        
        users = []
        
        # Check if using SQLite (dictionary cursor behavior differs)
        disable_db = os.getenv("DISABLE_DB", "0") == "1"
        use_sqlite_fallback = os.getenv("USE_SQLITE_FALLBACK", "1") == "1"
        using_sqlite = disable_db and use_sqlite_fallback

        if using_sqlite:
            cursor = conn.cursor()
            cursor.execute("SELECT email, first_name, last_name FROM users WHERE email IS NOT NULL AND email != ''")
            # SQLite default cursor returns tuples
            rows = cursor.fetchall()
            for row in rows:
                users.append({
                    'email': row[0],
                    'first_name': row[1],
                    'last_name': row[2]
                })
        else:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("SELECT email, first_name, last_name FROM users WHERE email IS NOT NULL AND email != ''")
                users = cur.fetchall()
        
        logger.info(f"Found {len(users)} users with emails.")
        
        added_count = 0
        
        # Prepare cursor for insertion
        if using_sqlite:
            cursor = conn.cursor()
            for user in users:
                email = user['email'].strip().lower()
                name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                note = f"Existing user: {name}" if name else "Existing user"
                
                try:
                    cursor.execute("""
                        INSERT INTO authorized_emails (email, added_by_user_id, notes, added_at)
                        VALUES (?, NULL, ?, datetime('now'))
                        ON CONFLICT(email) DO UPDATE SET
                            notes = excluded.notes
                    """, (email, note))
                    added_count += 1
                except Exception as e:
                    logger.warning(f"Failed to add {email}: {e}")
            conn.commit()
            
        else:
            with conn.cursor() as cur:
                for user in users:
                    email = user['email'].strip().lower()
                    name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                    note = f"Existing user: {name}" if name else "Existing user"
                    
                    try:
                        cur.execute("""
                            INSERT INTO authorized_emails (email, added_by_user_id, notes)
                            VALUES (%s, NULL, %s)
                            ON DUPLICATE KEY UPDATE
                                notes = VALUES(notes)
                        """, (email, note))
                        added_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to add {email}: {e}")
            conn.commit()
                
        logger.info(f"Successfully added {added_count} users to the whitelist.")
        
    except Exception as e:
        logger.error(f"Error during migration: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

if __name__ == "__main__":
    migrate_users_to_whitelist()
