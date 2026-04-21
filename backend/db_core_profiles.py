try:
    from .db_core_common import *
except ImportError:
    from db_core_common import *

def save_visited_profile(linkedin_url, is_unt_alum=False, notes=None):
    """
    Save a visited profile to the visited_profiles table.
    This tracks ALL profiles we've ever visited (UNT and non-UNT).
    """
    try:
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(cur, """
                INSERT INTO visited_profiles (linkedin_url, is_unt_alum, visited_at, last_checked, notes)
                VALUES (%s, %s, NOW(), NOW(), %s)
                ON DUPLICATE KEY UPDATE
                    is_unt_alum = VALUES(is_unt_alum),
                    last_checked = NOW(),
                    notes = COALESCE(VALUES(notes), notes)
            """, (normalize_url(linkedin_url), is_unt_alum, notes), connection=conn)

        logger.debug(f"Saved to visited_profiles: {linkedin_url} (UNT: {is_unt_alum})")
        return True

    except mysql.connector.Error as err:
        logger.error(f"Error saving visited profile: {err}")
        return False


def get_all_visited_profiles():
    """
    Get all visited profiles from the visited_profiles table.
    Returns a list of dicts with linkedin_url, is_unt_alum, visited_at, last_checked, needs_update.
    """
    try:
        with managed_db_cursor(get_connection, dictionary=True) as (_conn, cur):
            cur.execute("""
                SELECT linkedin_url, is_unt_alum, visited_at, last_checked, needs_update
                FROM visited_profiles
            """)
            profiles = cur.fetchall()

        logger.info(f"Retrieved {len(profiles)} visited profiles from database")
        return profiles

    except mysql.connector.Error as err:
        logger.error(f"Error fetching visited profiles: {err}")
        return []


def mark_profile_needs_update(linkedin_url, needs_update=True):
    """Mark a profile as needing update in the visited_profiles table."""
    try:
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(cur, """
                UPDATE visited_profiles
                SET needs_update = %s
                WHERE linkedin_url = %s
            """, (needs_update, linkedin_url.strip()), connection=conn)

        return True

    except mysql.connector.Error as err:
        logger.error(f"Error updating profile needs_update flag: {err}")
        return False


def sync_alumni_to_visited_profiles():
    """
    Sync all existing alumni records to the visited_profiles table.
    This ensures all UNT alumni are marked as visited.
    """
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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

        logger.info(f"Synced {synced} alumni to visited_profiles table")
        return synced

    except mysql.connector.Error as err:
        logger.error(f"Error syncing alumni to visited_profiles: {err}")
        return 0


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

    try:
        df = pd.read_csv(csv_path)
        logger.info(f"≡ƒôé Migrating {len(df)} entries from visited_history.csv to database...")

        migrated = 0

        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
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

        logger.info(f"Migrated {migrated} profiles from CSV to database")
        return migrated

    except Exception as e:
        logger.error(f"Error migrating visited history: {e}")
        return 0


def get_visited_profiles_stats():
    """Get statistics about visited profiles."""
    try:
        with managed_db_cursor(get_connection, dictionary=True) as (_conn, cur):
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


# ============================================================
# AUTHORIZED EMAILS FUNCTIONS
# ============================================================

def get_authorized_emails():
    """
    Get all authorized emails from the database.
    Returns a list of dicts with email, added_at, added_by_user_id, and notes.
    """
    try:
        with managed_db_cursor(get_connection, dictionary=True) as (_conn, cur):
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


def is_email_authorized(email):
    """
    Fast existence check for authorized email whitelist membership.
    Returns True if email exists in authorized_emails, otherwise False.
    """
    if not email:
        return False

    try:
        email = email.lower().strip()
        with managed_db_cursor(get_connection) as (conn, cur):
            execute_sql(
                cur,
                "SELECT 1 FROM authorized_emails WHERE LOWER(email) = LOWER(%s) LIMIT 1",
                (email,),
                connection=conn,
            )
            row = cur.fetchone()

        return bool(row)
    except Exception as err:
        logger.error(f"Error checking authorized email {email}: {err}")
        return False


def add_authorized_email(email, added_by_user_id=None, notes=None):
    """
    Add an email to the authorized emails list.
    Returns True if successful, False otherwise.
    """
    try:
        email = email.lower().strip()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(cur, """
                INSERT INTO authorized_emails (email, added_by_user_id, notes)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    notes = VALUES(notes),
                    added_by_user_id = VALUES(added_by_user_id)
            """, (email, added_by_user_id, notes), connection=conn, sqlite_query="""
                INSERT INTO authorized_emails (email, added_by_user_id, notes, added_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(email) DO UPDATE SET
                    notes = excluded.notes,
                    added_by_user_id = excluded.added_by_user_id
            """)
        
        logger.info(f"Added authorized email: {email}")
        return True
    except Exception as err:
        logger.error(f"Error adding authorized email {email}: {err}")
        return False


def remove_authorized_email(email):
    """
    Remove an email from the authorized emails list.
    Returns True if successful, False otherwise.
    """
    try:
        email = email.lower().strip()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                "DELETE FROM authorized_emails WHERE email = %s",
                (email,),
                connection=conn,
            )
        
        logger.info(f"Removed authorized email: {email}")
        return True
    except Exception as err:
        logger.error(f"Error removing authorized email {email}: {err}")
        return False


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
    
    logger.info(f"Authorized email migration complete: {migrated}/{len(emails)} added to database")
    return migrated


# ============================================================
# AUTH: USER MANAGEMENT
# ============================================================

def get_user_by_email(email):
    """
    Fetch a user row by email.  Returns dict or None.
    Used by the login and session flows (see backend/app.py ┬º /api/auth/login).
    """
    if not email:
        return None
    try:
        email = email.lower().strip()
        with managed_db_cursor(get_connection, dictionary=True) as (conn, cur):
            execute_sql(
                cur,
                "SELECT * FROM users WHERE LOWER(email) = LOWER(%s)",
                (email,),
                connection=conn,
            )
            return cur.fetchone()
    except Exception as err:
        logger.error(f"Error fetching user by email {email}: {err}")
        return None


def create_user_with_password(email, password_hash, role="user"):
    """
    Create a new user with email/password auth.
    Caller must verify whitelist *before* calling this function.
    Returns True on success.
    """
    if not email:
        return False
    try:
        email = email.lower().strip()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                """INSERT INTO users
                   (email, linkedin_id, password_hash, auth_type, role, must_change_password)
                   VALUES (%s, %s, %s, 'email_password', %s, FALSE)""",
                (email, f"email_{email}", password_hash, role),
                connection=conn,
                sqlite_query="""INSERT INTO users
                   (email, linkedin_id, password_hash, auth_type, role, must_change_password)
                   VALUES (?, ?, ?, 'email_password', ?, 0)""",
            )

        logger.info(f"Created user {email} with role={role}")
        return True
    except Exception as err:
        logger.error(f"Error creating user {email}: {err}")
        return False


def update_user_password(email, password_hash, auth_type=None):
    """
    Update a user's password hash.  Optionally update auth_type
    (e.g. 'both' when a LinkedIn user creates a password).
    """
    if not email:
        return False
    try:
        email = email.lower().strip()
        if auth_type:
            sql_sqlite = "UPDATE users SET password_hash = ?, auth_type = ?, must_change_password = 0 WHERE LOWER(email) = LOWER(?)"
            sql_mysql = "UPDATE users SET password_hash = %s, auth_type = %s, must_change_password = FALSE WHERE LOWER(email) = LOWER(%s)"
            params = (password_hash, auth_type, email)
        else:
            sql_sqlite = "UPDATE users SET password_hash = ?, must_change_password = 0 WHERE LOWER(email) = LOWER(?)"
            sql_mysql = "UPDATE users SET password_hash = %s, must_change_password = FALSE WHERE LOWER(email) = LOWER(%s)"
            params = (password_hash, email)

        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                sql_mysql,
                params,
                connection=conn,
                sqlite_query=sql_sqlite,
            )

        logger.info(f"Updated password for {email}")
        return True
    except Exception as err:
        logger.error(f"Error updating password for {email}: {err}")
        return False


def set_must_change_password(email, value=True):
    """Set the must_change_password flag for a user."""
    if not email:
        return False
    try:
        email = email.lower().strip()
        bool_val = 1 if value else 0
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                "UPDATE users SET must_change_password = %s WHERE LOWER(email) = LOWER(%s)",
                (value, email),
                connection=conn,
                sqlite_query="UPDATE users SET must_change_password = ? WHERE LOWER(email) = LOWER(?)",
            )

        return True
    except Exception as err:
        logger.error(f"Error setting must_change_password for {email}: {err}")
        return False


def update_user_role(email, role):
    """Set the role ('admin' or 'user') for a user."""
    if role not in ("admin", "user"):
        return False
    if not email:
        return False
    try:
        email = email.lower().strip()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                "UPDATE users SET role = %s WHERE LOWER(email) = LOWER(%s)",
                (role, email),
                connection=conn,
                sqlite_query="UPDATE users SET role = ? WHERE LOWER(email) = LOWER(?)",
            )

        logger.info(f"Updated role for {email} to {role}")
        return True
    except Exception as err:
        logger.error(f"Error updating role for {email}: {err}")
        return False


def get_all_users():
    """Return all user rows (admin dashboard). Sensitive fields omitted."""
    try:
        sql = """SELECT id, email, first_name, last_name, auth_type, role,
                        must_change_password, created_at
                 FROM users ORDER BY email"""

        with managed_db_cursor(get_connection, dictionary=True) as (conn, cur):
            execute_sql(cur, sql, connection=conn)
            rows = cur.fetchall()

        # Serialize datetimes
        for row in rows:
            for key in ("created_at",):
                val = row.get(key)
                if hasattr(val, "isoformat"):
                    row[key] = val.isoformat()
            # Coerce must_change_password to bool for JSON
            row["must_change_password"] = bool(row.get("must_change_password"))

        return rows
    except Exception as err:
        logger.error(f"Error fetching all users: {err}")
        return []


def delete_user(email):
    """Remove a user by email."""
    if not email:
        return False
    try:
        email = email.lower().strip()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                "DELETE FROM users WHERE LOWER(email) = LOWER(%s)",
                (email,),
                connection=conn,
            )

        logger.info(f"Deleted user {email}")
        return True
    except Exception as err:
        logger.error(f"Error deleting user {email}: {err}")
        return False


def admin_reset_password(email):
    """
    Admin resets a user's password: clear the hash and flag for change.
    """
    if not email:
        return False
    try:
        email = email.lower().strip()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                "UPDATE users SET password_hash = NULL, must_change_password = TRUE WHERE LOWER(email) = LOWER(%s)",
                (email,),
                connection=conn,
                sqlite_query="UPDATE users SET password_hash = NULL, must_change_password = 1 WHERE LOWER(email) = LOWER(?)",
            )

        logger.info(f"Admin reset password for {email}")
        return True
    except Exception as err:
        logger.error(f"Error in admin_reset_password for {email}: {err}")
        return False



