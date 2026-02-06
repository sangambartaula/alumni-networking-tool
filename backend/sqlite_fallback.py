"""
SQLite Fallback Module

This module provides a local SQLite database fallback when the cloud MySQL 
database is unreachable. It implements smart sync/merge logic where:
- Cloud is the source of truth
- Non-conflicting changes from both local and cloud are merged
- Conflicting changes favor cloud

ADDRESSED CONCERNS:
1. ON DUPLICATE KEY → Uses proper ON CONFLICT clauses per table (not INSERT OR REPLACE)
2. Incremental Sync → Uses last_cloud_sync timestamp for incremental updates
3. Timezones → All timestamps stored in UTC
4. Daemon Thread → Clean shutdown with atexit handler
5. Error Logging → Discarded changes logged in _discarded_changes table
6. SQLite Concurrency → Uses WAL mode, timeout, and retry logic
"""

import sqlite3
import mysql.connector
import os
import json
import logging
import threading
import time
import atexit
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from contextlib import contextmanager

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
BACKEND_DIR = Path(__file__).resolve().parent
SQLITE_DB_PATH = BACKEND_DIR / 'alumni_backup.db'

# MySQL connection parameters
MYSQL_HOST = os.getenv('MYSQLHOST')
MYSQL_USER = os.getenv('MYSQLUSER')
MYSQL_PASSWORD = os.getenv('MYSQLPASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_PORT = int(os.getenv('MYSQLPORT', 3306))

# Retry interval for cloud connection (seconds)
CLOUD_RETRY_INTERVAL = 30

# SQLite settings for better concurrency
SQLITE_TIMEOUT = 30  # seconds to wait for locks
SQLITE_RETRY_COUNT = 3
SQLITE_RETRY_DELAY = 0.5  # seconds between retries

# Table configuration with primary keys for proper upserts
TABLE_CONFIG = {
    'users': {
        'pk': ['id'],
        'unique_cols': ['linkedin_id'],
        'timestamp_col': 'updated_at'
    },
    'alumni': {
        'pk': ['id'],
        'unique_cols': ['linkedin_url'],
        'timestamp_col': 'updated_at'
    },
    'visited_profiles': {
        'pk': ['id'],
        'unique_cols': ['linkedin_url'],
        'timestamp_col': 'last_checked'
    },
    'user_interactions': {
        'pk': ['id'],
        'unique_cols': ['user_id', 'alumni_id', 'interaction_type'],
        'timestamp_col': 'updated_at'
    },
    'notes': {
        'pk': ['id'],
        'unique_cols': ['user_id', 'alumni_id'],
        'timestamp_col': 'updated_at'
    }
}


def utc_now() -> str:
    """Get current UTC timestamp as ISO format string."""
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(ts) -> datetime:
    """Parse a timestamp to datetime, handling various formats."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        # Handle various ISO formats
        ts = ts.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            return None
    return None


class ConnectionManager:
    """
    Singleton class that manages database connections.
    Routes connections to MySQL (cloud) or SQLite (local) based on availability.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._is_offline = False
        self._retry_thread = None
        self._stop_retry = threading.Event()
        self._last_cloud_sync = None
        self._sqlite_lock = threading.Lock()  # Protect SQLite writes
        self._shutting_down = False
        
        # Initialize SQLite tables
        self._init_sqlite()
        
        # Load sync state
        self._load_sync_state()
        
        # Register cleanup on shutdown
        atexit.register(self._cleanup)
    
    def _cleanup(self):
        """Clean up resources on shutdown."""
        logger.info("🛑 Shutting down SQLite fallback system...")
        self._shutting_down = True
        
        # Stop retry thread
        self._stop_retry.set()
        if self._retry_thread and self._retry_thread.is_alive():
            self._retry_thread.join(timeout=2)
            if self._retry_thread.is_alive():
                logger.warning("⚠️ Retry thread did not stop cleanly")
        
        logger.info("✅ SQLite fallback system shut down")
    
    def _init_sqlite(self):
        """Initialize SQLite database with required tables and optimizations."""
        conn = sqlite3.connect(str(SQLITE_DB_PATH), timeout=SQLITE_TIMEOUT)
        conn.row_factory = sqlite3.Row
        
        with conn:
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
            
            # Create tables mirroring MySQL schema with proper ON CONFLICT handling
            conn.executescript("""
                -- Users table
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    linkedin_id TEXT UNIQUE NOT NULL,
                    email TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                );
                
                -- Alumni table
                CREATE TABLE IF NOT EXISTS alumni (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT,
                    last_name TEXT,
                    grad_year INTEGER,
                    degree TEXT,
                    major TEXT,
                    linkedin_url TEXT NOT NULL UNIQUE,
                    current_job_title TEXT,
                    company TEXT,
                    location TEXT,
                    headline TEXT,
                    school_start_date TEXT,
                    job_start_date TEXT,
                    job_end_date TEXT,
                    working_while_studying INTEGER,
                    latitude REAL,
                    longitude REAL,
                    exp2_title TEXT,
                    exp2_company TEXT,
                    exp2_dates TEXT,
                    exp3_title TEXT,
                    exp3_company TEXT,
                    exp3_dates TEXT,
                    scraped_at TEXT DEFAULT (datetime('now')),
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),
                    last_updated TEXT DEFAULT (datetime('now'))
                );
                
                -- Visited profiles table
                CREATE TABLE IF NOT EXISTS visited_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    linkedin_url TEXT NOT NULL UNIQUE,
                    is_unt_alum INTEGER DEFAULT 0,
                    visited_at TEXT DEFAULT (datetime('now')),
                    last_checked TEXT DEFAULT (datetime('now')),
                    needs_update INTEGER DEFAULT 0,
                    notes TEXT
                );
                -- Scraper resume state (singleton)
                CREATE TABLE IF NOT EXISTS scrape_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    mode TEXT,
                    search_url TEXT,
                    page INTEGER,
                    updated_at TEXT DEFAULT (datetime('now'))
                );

                -- Initialize singleton row
                INSERT OR IGNORE INTO scrape_state (id, page) VALUES (1, 1);

                -- User interactions table
                CREATE TABLE IF NOT EXISTS user_interactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    alumni_id INTEGER NOT NULL,
                    interaction_type TEXT NOT NULL,
                    notes TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(user_id, alumni_id, interaction_type)
                );
                
                -- Notes table
                CREATE TABLE IF NOT EXISTS notes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    alumni_id INTEGER NOT NULL,
                    note_content TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(user_id, alumni_id)
                );
                
                -- Pending sync table (tracks local changes to push to cloud)
                CREATE TABLE IF NOT EXISTS _pending_sync (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    primary_key TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    old_data TEXT,
                    new_data TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                );
                
                -- Sync state table (singleton)
                CREATE TABLE IF NOT EXISTS _sync_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_cloud_sync TEXT,
                    is_offline INTEGER DEFAULT 0
                );
                
                -- Initialize sync state if not exists
                INSERT OR IGNORE INTO _sync_state (id, is_offline) VALUES (1, 0);
                
                -- Discarded changes log (for user awareness)
                CREATE TABLE IF NOT EXISTS _discarded_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    primary_key TEXT NOT NULL,
                    local_data TEXT,
                    cloud_data TEXT,
                    reason TEXT,
                    discarded_at TEXT DEFAULT (datetime('now'))
                );
                
                -- Create indexes for better query performance
                CREATE INDEX IF NOT EXISTS idx_pending_sync_table ON _pending_sync(table_name);
                CREATE INDEX IF NOT EXISTS idx_pending_sync_created ON _pending_sync(created_at);
            """)
        
        conn.close()
        logger.info(f"✅ SQLite database initialized at {SQLITE_DB_PATH}")
    
    @contextmanager
    def _sqlite_write_context(self):
        """Context manager for SQLite writes with retry logic."""
        with self._sqlite_lock:
            for attempt in range(SQLITE_RETRY_COUNT):
                try:
                    conn = sqlite3.connect(str(SQLITE_DB_PATH), timeout=SQLITE_TIMEOUT)
                    conn.row_factory = sqlite3.Row
                    try:
                        yield conn
                        conn.commit()
                        return
                    except Exception:
                        conn.rollback()
                        raise
                    finally:
                        conn.close()
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < SQLITE_RETRY_COUNT - 1:
                        logger.warning(f"⚠️ SQLite locked, retrying ({attempt + 1}/{SQLITE_RETRY_COUNT})...")
                        time.sleep(SQLITE_RETRY_DELAY * (attempt + 1))
                    else:
                        raise
    
    def _load_sync_state(self):
        """Load sync state from SQLite."""
        conn = sqlite3.connect(str(SQLITE_DB_PATH), timeout=SQLITE_TIMEOUT)
        conn.row_factory = sqlite3.Row
        
        try:
            cur = conn.execute("SELECT last_cloud_sync, is_offline FROM _sync_state WHERE id = 1")
            row = cur.fetchone()
            if row:
                self._last_cloud_sync = row['last_cloud_sync']
                self._is_offline = bool(row['is_offline'])
        finally:
            conn.close()
    
    def _save_sync_state(self):
        """Save sync state to SQLite."""
        with self._sqlite_write_context() as conn:
            conn.execute("""
                UPDATE _sync_state 
                SET last_cloud_sync = ?, is_offline = ?
                WHERE id = 1
            """, (self._last_cloud_sync, int(self._is_offline)))
    
    def check_cloud_connection(self) -> bool:
        """Test if MySQL cloud database is reachable."""
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                port=MYSQL_PORT,
                connection_timeout=5
            )
            conn.close()
            return True
        except Exception as e:
            logger.debug(f"Cloud connection check failed: {e}")
            return False
    
    def get_mysql_connection(self):
        """Get a direct MySQL connection (for sync operations)."""
        return mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            port=MYSQL_PORT
        )
    
    def get_sqlite_connection(self):
        """Get a SQLite connection with proper settings."""
        conn = sqlite3.connect(str(SQLITE_DB_PATH), timeout=SQLITE_TIMEOUT, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_connection(self):
        """
        Get the appropriate database connection.
        Returns MySQL if online, SQLite if offline.
        """
        if self._is_offline:
            return SQLiteConnectionWrapper(self.get_sqlite_connection(), self)
        
        try:
            conn = self.get_mysql_connection()
            return conn
        except Exception as e:
            logger.warning(f"⚠️ Cloud database unreachable: {e}")
            self._go_offline()
            return SQLiteConnectionWrapper(self.get_sqlite_connection(), self)
    
    def is_offline(self) -> bool:
        """Check if we're in offline mode."""
        return self._is_offline
    
    def _go_offline(self):
        """Switch to offline mode and start retry thread."""
        if self._is_offline or self._shutting_down:
            return
        
        self._is_offline = True
        self._save_sync_state()
        logger.warning("📴 Switching to offline mode (SQLite fallback)")
        
        # Start retry thread
        self._stop_retry.clear()
        self._retry_thread = threading.Thread(
            target=self._retry_cloud_connection, 
            daemon=True,
            name="CloudRetryThread"
        )
        self._retry_thread.start()
    
    def _go_online(self):
        """Switch back to online mode."""
        if not self._is_offline:
            return
        
        # Stop retry thread
        self._stop_retry.set()
        if self._retry_thread:
            self._retry_thread.join(timeout=2)
        
        self._is_offline = False
        self._last_cloud_sync = utc_now()
        self._save_sync_state()
        logger.info("📶 Switching to online mode (MySQL cloud)")
    
    def _retry_cloud_connection(self):
        """Background thread that retries cloud connection."""
        logger.info(f"🔄 Starting cloud retry thread (every {CLOUD_RETRY_INTERVAL}s)")
        
        while not self._stop_retry.is_set() and not self._shutting_down:
            self._stop_retry.wait(CLOUD_RETRY_INTERVAL)
            
            if self._stop_retry.is_set() or self._shutting_down:
                break
            
            logger.debug("🔄 Checking cloud connection...")
            if self.check_cloud_connection():
                logger.info("☁️ Cloud connection restored!")
                try:
                    self._sync_and_go_online()
                except Exception as e:
                    logger.error(f"❌ Sync failed: {e}")
                break
        
        logger.info("🔄 Cloud retry thread stopped")
    
    def _sync_and_go_online(self):
        """Sync pending changes and switch to online mode."""
        # Push pending local changes to cloud
        self._push_pending_changes()
        
        # Pull latest from cloud to local (incremental)
        self._pull_cloud_to_local(incremental=True)
        
        # Switch to online
        self._go_online()
    
    def _push_pending_changes(self):
        """Push pending local changes to cloud with smart merge."""
        sqlite_conn = self.get_sqlite_connection()
        
        try:
            pending = sqlite_conn.execute("""
                SELECT id, table_name, primary_key, operation, old_data, new_data, created_at
                FROM _pending_sync
                ORDER BY created_at ASC
            """).fetchall()
            
            if not pending:
                logger.info("📤 No pending changes to sync")
                return
            
            logger.info(f"📤 Syncing {len(pending)} pending changes to cloud...")
            
            mysql_conn = self.get_mysql_connection()
            synced_ids = []
            
            try:
                for row in pending:
                    sync_id = row['id']
                    table_name = row['table_name']
                    pk = json.loads(row['primary_key'])
                    operation = row['operation']
                    old_data = json.loads(row['old_data']) if row['old_data'] else None
                    new_data = json.loads(row['new_data'])
                    
                    try:
                        conflict = self._check_conflict(mysql_conn, table_name, pk, old_data)
                        
                        if conflict:
                            # Log discarded change with details
                            self._log_discarded_change(
                                sqlite_conn, table_name, pk, new_data, conflict,
                                f"Cloud modified same record since {row['created_at']}"
                            )
                            logger.warning(f"⚠️ Conflict in {table_name} (pk={pk}), keeping cloud version. Local change logged.")
                        else:
                            # Apply change to cloud
                            self._apply_change_to_mysql(mysql_conn, table_name, operation, pk, new_data)
                            logger.debug(f"✅ Synced {operation} to {table_name}")
                        
                        synced_ids.append(sync_id)
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to sync change {sync_id}: {e}")
                
                mysql_conn.commit()
                
                # Clear synced pending changes
                if synced_ids:
                    placeholders = ','.join('?' * len(synced_ids))
                    sqlite_conn.execute(f"DELETE FROM _pending_sync WHERE id IN ({placeholders})", synced_ids)
                    sqlite_conn.commit()
                    logger.info(f"✅ Synced {len(synced_ids)} changes to cloud")
            
            finally:
                mysql_conn.close()
        finally:
            sqlite_conn.close()
    
    def _check_conflict(self, mysql_conn, table_name, pk, old_data) -> dict:
        """
        Check if cloud record conflicts with our pending change.
        Returns the cloud record if there's a conflict, None otherwise.
        """
        if old_data is None:
            # INSERT operation, check if record exists
            return None
        
        # Build WHERE clause from primary key
        where_clauses = []
        values = []
        for key, value in pk.items():
            where_clauses.append(f"{key} = %s")
            values.append(value)
        
        where_sql = " AND ".join(where_clauses)
        
        with mysql_conn.cursor(dictionary=True) as cur:
            cur.execute(f"SELECT * FROM {table_name} WHERE {where_sql}", values)
            cloud_record = cur.fetchone()
        
        if not cloud_record:
            # Record deleted on cloud - conflict
            return {"_deleted": True}
        
        # Check if cloud record was modified since we captured old_data
        config = TABLE_CONFIG.get(table_name, {})
        timestamp_col = config.get('timestamp_col', 'updated_at')
        
        if timestamp_col in cloud_record and timestamp_col in old_data:
            cloud_updated = parse_timestamp(cloud_record[timestamp_col])
            old_updated = parse_timestamp(old_data[timestamp_col])
            
            if cloud_updated and old_updated and cloud_updated > old_updated:
                return dict(cloud_record)
        
        return None
    
    def _apply_change_to_mysql(self, mysql_conn, table_name, operation, pk, new_data):
        """Apply a change to MySQL."""
        with mysql_conn.cursor() as cur:
            if operation == 'INSERT':
                columns = list(new_data.keys())
                placeholders = ', '.join(['%s'] * len(columns))
                col_names = ', '.join(columns)
                values = [new_data[c] for c in columns]
                
                # Use INSERT ... ON DUPLICATE KEY UPDATE for upsert
                update_clause = ', '.join([f"{c} = VALUES({c})" for c in columns if c not in pk])
                
                if update_clause:
                    cur.execute(f"""
                        INSERT INTO {table_name} ({col_names})
                        VALUES ({placeholders})
                        ON DUPLICATE KEY UPDATE {update_clause}
                    """, values)
                else:
                    cur.execute(f"""
                        INSERT IGNORE INTO {table_name} ({col_names})
                        VALUES ({placeholders})
                    """, values)
                
            elif operation == 'UPDATE':
                # Build SET clause
                set_clauses = []
                values = []
                for key, value in new_data.items():
                    if key not in pk:
                        set_clauses.append(f"{key} = %s")
                        values.append(value)
                
                # Build WHERE clause
                where_clauses = []
                for key, value in pk.items():
                    where_clauses.append(f"{key} = %s")
                    values.append(value)
                
                set_sql = ", ".join(set_clauses)
                where_sql = " AND ".join(where_clauses)
                
                if set_sql:
                    cur.execute(f"UPDATE {table_name} SET {set_sql} WHERE {where_sql}", values)
                
            elif operation == 'DELETE':
                where_clauses = []
                values = []
                for key, value in pk.items():
                    where_clauses.append(f"{key} = %s")
                    values.append(value)
                
                where_sql = " AND ".join(where_clauses)
                cur.execute(f"DELETE FROM {table_name} WHERE {where_sql}", values)
    
    def _log_discarded_change(self, sqlite_conn, table_name, pk, local_data, cloud_data, reason):
        """Log a discarded local change for user awareness."""
        # Convert cloud_data datetime objects to strings
        if isinstance(cloud_data, dict):
            cloud_data_str = {}
            for k, v in cloud_data.items():
                if hasattr(v, 'isoformat'):
                    cloud_data_str[k] = v.isoformat()
                else:
                    cloud_data_str[k] = v
        else:
            cloud_data_str = cloud_data
            
        sqlite_conn.execute("""
            INSERT INTO _discarded_changes (table_name, primary_key, local_data, cloud_data, reason)
            VALUES (?, ?, ?, ?, ?)
        """, (table_name, json.dumps(pk), json.dumps(local_data), json.dumps(cloud_data_str), reason))
        sqlite_conn.commit()
        
        logger.info(f"📝 Logged discarded change: {table_name} pk={pk} reason={reason}")
    
    def _pull_cloud_to_local(self, incremental: bool = False):
        """
        Pull latest data from cloud to local SQLite.
        
        Args:
            incremental: If True, only sync records updated since last_cloud_sync
        """
        if incremental and self._last_cloud_sync:
            logger.info(f"📥 Pulling incremental updates since {self._last_cloud_sync}...")
        else:
            logger.info("📥 Pulling full data from cloud...")
            incremental = False
        
        mysql_conn = self.get_mysql_connection()
        sqlite_conn = self.get_sqlite_connection()
        
        tables = list(TABLE_CONFIG.keys())
        
        try:
            total_rows = 0
            for table in tables:
                rows = self._sync_table_from_mysql(mysql_conn, sqlite_conn, table, incremental)
                total_rows += rows
            
            sqlite_conn.commit()
            logger.info(f"✅ Cloud data synced to local SQLite ({total_rows} rows)")
        
        finally:
            mysql_conn.close()
            sqlite_conn.close()
    
    def _ensure_sqlite_columns(self, sqlite_conn, table_name: str, columns: list):
        """
        Ensure SQLite table has all columns that exist in MySQL.
        Dynamically adds missing columns to handle schema differences.
        """
        # Get existing columns from SQLite
        cur = sqlite_conn.execute(f"PRAGMA table_info({table_name})")
        existing_cols = {row[1] for row in cur.fetchall()}
        
        # Add any missing columns
        for col in columns:
            if col not in existing_cols:
                # Use TEXT as default type (works for most data)
                try:
                    sqlite_conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")
                    logger.debug(f"  Added missing column {col} to {table_name}")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" not in str(e).lower():
                        logger.warning(f"Failed to add column {col} to {table_name}: {e}")
    
    def _sync_table_from_mysql(self, mysql_conn, sqlite_conn, table_name, incremental: bool = False) -> int:
        """
        Sync a single table from MySQL to SQLite.
        Uses proper UPSERT (INSERT ... ON CONFLICT) instead of INSERT OR REPLACE.
        
        Returns:
            Number of rows synced
        """
        config = TABLE_CONFIG.get(table_name, {})
        timestamp_col = config.get('timestamp_col', 'updated_at')
        unique_cols = config.get('unique_cols', [])
        
        # Build query
        if incremental and self._last_cloud_sync:
            # Fetch only updated records
            query = f"SELECT * FROM {table_name} WHERE {timestamp_col} > %s"
            with mysql_conn.cursor(dictionary=True) as cur:
                cur.execute(query, (self._last_cloud_sync,))
                rows = cur.fetchall()
        else:
            # Full sync - first clear existing data for full sync
            sqlite_conn.execute(f"DELETE FROM {table_name}")
            
            with mysql_conn.cursor(dictionary=True) as cur:
                cur.execute(f"SELECT * FROM {table_name}")
                rows = cur.fetchall()
        
        if not rows:
            return 0
        
        # Get column names from first row
        columns = list(rows[0].keys())
        
        # Ensure SQLite table has all columns from MySQL
        self._ensure_sqlite_columns(sqlite_conn, table_name, columns)
        
        # Build SQLite UPSERT query with ON CONFLICT
        placeholders = ', '.join(['?'] * len(columns))
        col_names = ', '.join(columns)
        
        # Build ON CONFLICT clause based on unique columns
        if unique_cols:
            conflict_cols = ', '.join(unique_cols)
            update_cols = [c for c in columns if c not in unique_cols and c != 'id']
            update_clause = ', '.join([f"{c} = excluded.{c}" for c in update_cols])
            
            if update_clause:
                upsert_sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT({conflict_cols}) DO UPDATE SET {update_clause}
                """
            else:
                upsert_sql = f"""
                    INSERT OR IGNORE INTO {table_name} ({col_names})
                    VALUES ({placeholders})
                """
        else:
            # No unique constraint, just insert
            upsert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
        
        # Insert/update rows
        error_count = 0
        unique_error_count = 0
        last_error = None
        
        for row in rows:
            values = []
            for col in columns:
                val = row[col]
                # Convert datetime to UTC ISO format string
                if hasattr(val, 'isoformat'):
                    val = val.isoformat()
                values.append(val)
            
            try:
                sqlite_conn.execute(upsert_sql, values)
            except sqlite3.Error as e:
                error_str = str(e)
                if "UNIQUE constraint" in error_str:
                    unique_error_count += 1
                else:
                    error_count += 1
                    last_error = error_str
        
        if unique_error_count > 0:
            logger.warning(f"⚠️  Skipped {unique_error_count} duplicate/conflicting rows in {table_name} during sync (UNIQUE constraints). This is expected if IDs conflict.")
            
        if error_count > 0:
            logger.warning(f"❌ Failed to sync {error_count} rows in {table_name}. First error: {last_error}")
        
        logger.debug(f"  ✓ Synced rows to {table_name} (Updated/Ignored)")
        return len(rows)
    
    def record_pending_change(self, table_name: str, primary_key: dict, operation: str, 
                              old_data: dict = None, new_data: dict = None):
        """Record a pending local change for later sync to cloud."""
        if not self._is_offline:
            return  # Only record when offline
        
        with self._sqlite_write_context() as conn:
            conn.execute("""
                INSERT INTO _pending_sync (table_name, primary_key, operation, old_data, new_data, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                table_name,
                json.dumps(primary_key),
                operation,
                json.dumps(old_data) if old_data else None,
                json.dumps(new_data) if new_data else None,
                utc_now()
            ))
    
    def force_sync(self) -> dict:
        """Manually trigger a sync. Returns status dict."""
        if not self.check_cloud_connection():
            return {"success": False, "error": "Cloud is not reachable"}
        
        try:
            self._push_pending_changes()
            self._pull_cloud_to_local(incremental=True)
            self._last_cloud_sync = utc_now()
            self._save_sync_state()
            return {"success": True, "last_sync": self._last_cloud_sync}
        except Exception as e:
            return {"success": False, "error": str(e)}


class SQLiteConnectionWrapper:
    """
    Wrapper around SQLite connection to provide MySQL-compatible interface.
    Allows using SQLite as a drop-in replacement for mysql.connector.
    """
    
    def __init__(self, conn, manager: ConnectionManager = None):
        self._conn = conn
        self._manager = manager
        self._in_transaction = False
    
    def cursor(self, dictionary=False):
        """Return a cursor wrapper."""
        return SQLiteCursorWrapper(self._conn.cursor(), dictionary, self._manager)
    
    def commit(self):
        self._conn.commit()
    
    def rollback(self):
        self._conn.rollback()
    
    def close(self):
        self._conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


class SQLiteCursorWrapper:
    """
    Wrapper around SQLite cursor to provide MySQL-compatible interface.
    Translates MySQL syntax to SQLite and handles common differences.
    """
    
    def __init__(self, cursor, dictionary=False, manager: ConnectionManager = None):
        self._cursor = cursor
        self._dictionary = dictionary
        self._manager = manager
    
    def execute(self, query, params=None):
        """Execute a query, translating MySQL syntax to SQLite."""
        # Translate MySQL placeholder %s to SQLite placeholder ?
        translated_query = query.replace('%s', '?')
        
        # Handle ON DUPLICATE KEY UPDATE -> ON CONFLICT DO UPDATE
        if 'ON DUPLICATE KEY UPDATE' in translated_query.upper():
            translated_query = self._convert_upsert(translated_query)
        
        # Handle MySQL NOW() -> SQLite datetime('now')
        translated_query = translated_query.replace('NOW()', "datetime('now', 'utc')")
        
        # Handle CURRENT_TIMESTAMP
        import re
        translated_query = re.sub(
            r'\bCURRENT_TIMESTAMP\b', 
            "datetime('now', 'utc')", 
            translated_query, 
            flags=re.IGNORECASE
        )
        
        if params:
            self._cursor.execute(translated_query, params)
        else:
            self._cursor.execute(translated_query)
    
    def _convert_upsert(self, query):
        """
        Convert MySQL ON DUPLICATE KEY UPDATE to SQLite ON CONFLICT DO UPDATE.
        This is more sophisticated than simple INSERT OR REPLACE to preserve FKs.
        """
        query_upper = query.upper()
        if 'ON DUPLICATE KEY UPDATE' not in query_upper:
            return query
        
        # Find the split point
        split_idx = query_upper.find('ON DUPLICATE KEY UPDATE')
        insert_part = query[:split_idx].strip()
        update_part = query[split_idx + len('ON DUPLICATE KEY UPDATE'):].strip()
        
        # Try to determine the table name and find its unique constraint
        import re
        match = re.search(r'INSERT\s+INTO\s+(\w+)', insert_part, re.IGNORECASE)
        if match:
            table_name = match.group(1).lower()
            config = TABLE_CONFIG.get(table_name, {})
            unique_cols = config.get('unique_cols', [])
            
            if unique_cols:
                # Parse the update clause and convert VALUES(col) -> excluded.col
                update_part = re.sub(r'VALUES\s*\(\s*(\w+)\s*\)', r'excluded.\1', update_part, flags=re.IGNORECASE)
                
                conflict_cols = ', '.join(unique_cols)
                return f"{insert_part} ON CONFLICT({conflict_cols}) DO UPDATE SET {update_part}"
        
        # Fallback: use INSERT OR REPLACE (less ideal but works)
        insert_part = re.sub(r'INSERT\s+INTO', 'INSERT OR REPLACE INTO', insert_part, flags=re.IGNORECASE)
        return insert_part
    
    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if self._dictionary:
            return dict(row)
        return tuple(row)
    
    def fetchall(self):
        rows = self._cursor.fetchall()
        if self._dictionary:
            return [dict(row) for row in rows]
        return [tuple(row) for row in rows]
    
    @property
    def rowcount(self):
        return self._cursor.rowcount
    
    @property
    def lastrowid(self):
        return self._cursor.lastrowid
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


# Singleton accessor
_connection_manager = None


def get_connection_manager() -> ConnectionManager:
    """Get the singleton ConnectionManager instance."""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager()
    return _connection_manager


def init_fallback_system():
    """
    Initialize the SQLite fallback system.
    Call this on application startup.
    """
    logger.info("=" * 60)
    logger.info("INITIALIZING SQLITE FALLBACK SYSTEM")
    logger.info("=" * 60)
    
    manager = get_connection_manager()
    
    if manager.check_cloud_connection():
        logger.info("☁️ Cloud database is reachable")
        
        try:
            # Sync from cloud to local
            manager._pull_cloud_to_local(incremental=bool(manager._last_cloud_sync))
            manager._last_cloud_sync = utc_now()
            manager._save_sync_state()
            logger.info("✅ Synced cloud data to local SQLite backup")
        except Exception as e:
            logger.error(f"❌ Failed to sync from cloud: {e}")
    else:
        logger.warning("📴 Cloud database is UNREACHABLE")
        logger.info("📂 Using local SQLite fallback")
        manager._go_offline()
    
    logger.info("=" * 60)
    logger.info("SQLITE FALLBACK SYSTEM READY")
    logger.info("=" * 60)


def get_fallback_status() -> dict:
    """Get the current status of the fallback system."""
    manager = get_connection_manager()
    
    sqlite_conn = manager.get_sqlite_connection()
    try:
        pending_count = sqlite_conn.execute("SELECT COUNT(*) FROM _pending_sync").fetchone()[0]
        discarded_count = sqlite_conn.execute("SELECT COUNT(*) FROM _discarded_changes").fetchone()[0]
        
        # Also get counts of data in each table
        table_counts = {}
        for table in TABLE_CONFIG.keys():
            count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            table_counts[table] = count
    finally:
        sqlite_conn.close()
    
    return {
        "is_offline": manager.is_offline(),
        "last_cloud_sync": manager._last_cloud_sync,
        "pending_changes": pending_count,
        "discarded_changes": discarded_count,
        "sqlite_path": str(SQLITE_DB_PATH),
        "table_counts": table_counts
    }


def get_discarded_changes() -> list:
    """Get list of all discarded changes for review."""
    manager = get_connection_manager()
    
    sqlite_conn = manager.get_sqlite_connection()
    try:
        rows = sqlite_conn.execute("""
            SELECT id, table_name, primary_key, local_data, cloud_data, reason, discarded_at
            FROM _discarded_changes
            ORDER BY discarded_at DESC
        """).fetchall()
        
        return [dict(row) for row in rows]
    finally:
        sqlite_conn.close()


def test_offline_mode():
    """
    Test function to verify SQLite fallback is working.
    Returns a dict with test results.
    """
    results = {
        "tests": [],
        "passed": 0,
        "failed": 0
    }
    
    def add_result(name, passed, message=""):
        results["tests"].append({"name": name, "passed": passed, "message": message})
        if passed:
            results["passed"] += 1
        else:
            results["failed"] += 1
    
    # Test 1: SQLite database exists
    add_result(
        "SQLite database exists",
        SQLITE_DB_PATH.exists(),
        str(SQLITE_DB_PATH)
    )
    
    # Test 2: Can connect to SQLite
    try:
        conn = sqlite3.connect(str(SQLITE_DB_PATH))
        conn.close()
        add_result("SQLite connection", True)
    except Exception as e:
        add_result("SQLite connection", False, str(e))
    
    # Test 3: Tables exist
    try:
        conn = sqlite3.connect(str(SQLITE_DB_PATH))
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]
        conn.close()
        
        required_tables = list(TABLE_CONFIG.keys()) + ['_pending_sync', '_sync_state', '_discarded_changes']
        missing = [t for t in required_tables if t not in tables]
        
        add_result(
            "Required tables exist",
            len(missing) == 0,
            f"Found: {tables}" if len(missing) == 0 else f"Missing: {missing}"
        )
    except Exception as e:
        add_result("Required tables exist", False, str(e))
    
    # Test 4: Alumni data synced
    try:
        status = get_fallback_status()
        alumni_count = status.get('table_counts', {}).get('alumni', 0)
        add_result(
            "Alumni data synced",
            alumni_count > 0,
            f"{alumni_count} alumni records"
        )
    except Exception as e:
        add_result("Alumni data synced", False, str(e))
    
    # Test 5: WAL mode enabled
    try:
        conn = sqlite3.connect(str(SQLITE_DB_PATH))
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode")
        mode = cur.fetchone()[0]
        conn.close()
        add_result(
            "WAL mode enabled",
            mode.lower() == 'wal',
            f"Current mode: {mode}"
        )
    except Exception as e:
        add_result("WAL mode enabled", False, str(e))
    
    return results


if __name__ == "__main__":
    # Test the fallback system
    init_fallback_system()
    
    print("\n" + "=" * 60)
    print("FALLBACK SYSTEM STATUS")
    print("=" * 60)
    
    status = get_fallback_status()
    print(f"  Offline Mode: {status['is_offline']}")
    print(f"  Last Cloud Sync: {status['last_cloud_sync']}")
    print(f"  Pending Changes: {status['pending_changes']}")
    print(f"  Discarded Changes: {status['discarded_changes']}")
    print(f"  SQLite Path: {status['sqlite_path']}")
    print("\n  Table Counts:")
    for table, count in status.get('table_counts', {}).items():
        print(f"    {table}: {count}")
    
    print("\n" + "=" * 60)
    print("RUNNING TESTS")
    print("=" * 60)
    
    test_results = test_offline_mode()
    for test in test_results["tests"]:
        status_icon = "✅" if test["passed"] else "❌"
        msg = f" - {test['message']}" if test['message'] else ""
        print(f"  {status_icon} {test['name']}{msg}")
    
    print(f"\n  Results: {test_results['passed']} passed, {test_results['failed']} failed")
