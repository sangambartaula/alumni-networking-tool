"""
Auth System Migration
=====================
Add authentication columns to the ``users`` table:
  - password_hash   (nullable TEXT — LinkedIn-only users won't have one)
  - auth_type       ('linkedin_only', 'email_password', or 'both')
  - role            ('admin' or 'user')
  - must_change_password (boolean, default False)

Safe to re-run (idempotent).  Works on both MySQL and SQLite.

Usage:
    python migrations/migrate_auth_system.py
"""

import os
import sys
from pathlib import Path

# Ensure backend is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))
os.chdir(Path(__file__).resolve().parent.parent)

from database import get_connection
from auth import DEFAULT_ADMIN_EMAILS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


_NEW_COLUMNS = [
    # (column_name, MySQL definition, SQLite definition)
    ("password_hash", "VARCHAR(255) DEFAULT NULL", "TEXT DEFAULT NULL"),
    ("auth_type", "VARCHAR(20) DEFAULT 'linkedin_only'", "TEXT DEFAULT 'linkedin_only'"),
    ("role", "VARCHAR(10) DEFAULT 'user'", "TEXT DEFAULT 'user'"),
    ("must_change_password", "BOOLEAN DEFAULT FALSE", "INTEGER DEFAULT 0"),
]


def _add_column(cur, col_name, col_def, is_sqlite):
    """Attempt to add a column; silently skip if it already exists."""
    try:
        cur.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")
        logger.info(f"  ✅ Added column users.{col_name}")
    except Exception as err:
        err_str = str(err).lower()
        if "duplicate column" in err_str or "already exists" in err_str:
            logger.info(f"  ⏩ Column users.{col_name} already exists")
        else:
            raise


def _seed_default_admins(cur, is_sqlite):
    """Ensure default admin emails exist in users and authorized_emails."""
    placeholder = "?" if is_sqlite else "%s"

    for email in DEFAULT_ADMIN_EMAILS:
        email_lower = email.lower().strip()

        # Ensure in authorized_emails whitelist
        if is_sqlite:
            cur.execute(
                "INSERT OR IGNORE INTO authorized_emails (email) VALUES (?)",
                (email_lower,),
            )
        else:
            cur.execute(
                "INSERT IGNORE INTO authorized_emails (email) VALUES (%s)",
                (email_lower,),
            )

        # Upsert into users with role=admin
        cur.execute(
            f"SELECT id FROM users WHERE LOWER(email) = LOWER({placeholder})",
            (email_lower,),
        )
        existing = cur.fetchone()

        if existing:
            cur.execute(
                f"UPDATE users SET role = 'admin' WHERE LOWER(email) = LOWER({placeholder})",
                (email_lower,),
            )
            logger.info(f"  👑 Set existing user {email_lower} to admin")
        else:
            if is_sqlite:
                cur.execute(
                    """INSERT OR IGNORE INTO users
                       (email, linkedin_id, auth_type, role, must_change_password)
                       VALUES (?, ?, 'email_password', 'admin', 0)""",
                    (email_lower, f"seed_{email_lower}"),
                )
            else:
                cur.execute(
                    """INSERT IGNORE INTO users
                       (email, linkedin_id, auth_type, role, must_change_password)
                       VALUES (%s, %s, 'email_password', 'admin', FALSE)""",
                    (email_lower, f"seed_{email_lower}"),
                )
            logger.info(f"  👑 Created admin user {email_lower}")


def migrate():
    conn = None
    try:
        conn = get_connection()

        # Detect SQLite vs MySQL
        is_sqlite = hasattr(conn, "_sqlite_conn") or "sqlite" in type(conn).__module__.lower()
        if not is_sqlite:
            try:
                is_sqlite = "sqlite" in str(type(conn._conn)).lower() if hasattr(conn, "_conn") else False
            except Exception:
                pass

        with conn.cursor() as cur:
            logger.info("── Adding auth columns to users table ──")
            for col_name, mysql_def, sqlite_def in _NEW_COLUMNS:
                col_def = sqlite_def if is_sqlite else mysql_def
                _add_column(cur, col_name, col_def, is_sqlite)

            # Set existing users that lack auth_type to linkedin_only
            placeholder = "?" if is_sqlite else "%s"
            try:
                cur.execute(
                    "UPDATE users SET auth_type = 'linkedin_only' WHERE auth_type IS NULL"
                )
            except Exception:
                pass

            logger.info("── Seeding default admin accounts ──")
            _seed_default_admins(cur, is_sqlite)

            conn.commit()
            logger.info("✅ Auth system migration complete")

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    migrate()
