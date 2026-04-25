try:
    from .sf_core_shared import *
except ImportError:
    from sf_core_shared import *

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
        self._mysql_table_columns_cache = {}
        
        # Initialize SQLite tables
        self._init_sqlite()
        
        # Load sync state
        self._load_sync_state()

        startup_cloud_pull_done = False
        # Do not stay offline by default if cloud is currently reachable.
        if self._is_offline:
            try:
                if self.check_cloud_connection():
                    logger.info("☁️ Cloud reachable at startup; leaving offline fallback mode.")
                    self._sync_and_go_online()
                    startup_cloud_pull_done = True
            except Exception as e:
                logger.warning(f"⚠️ Cloud startup recovery failed; staying in fallback mode: {e}")

        # When already online, refresh local SQLite from cloud (pull-only, no _pending_sync push).
        # Skips if we just pulled in _sync_and_go_online, if still offline, or if disabled via env.
        if (
            not startup_cloud_pull_done
            and not self._is_offline
            and os.environ.get("SKIP_SQLITE_STARTUP_PULL", "").strip().lower() not in ("1", "true", "yes")
        ):
            try:
                if self.check_cloud_connection():
                    self._pull_cloud_to_local(incremental=bool(self._last_cloud_sync))
                    self._last_cloud_sync = utc_now()
                    self._save_sync_state()
                    logger.info("✅ Local SQLite updated from cloud at startup (pull-only).")
            except Exception as e:
                logger.warning(f"⚠️ Startup cloud→SQLite pull failed: {e}")

        # Register cleanup on shutdown
        atexit.register(self._cleanup)
    
    def _cleanup(self):
        """Clean up resources on shutdown."""
        try:
            logger.info("🛑 Shutting down SQLite fallback system...")
        except Exception:
            pass
        self._shutting_down = True
        
        # Stop retry thread
        self._stop_retry.set()
        if self._retry_thread and self._retry_thread.is_alive():
            self._retry_thread.join(timeout=2)
            if self._retry_thread.is_alive():
                logger.warning("⚠️ Retry thread did not stop cleanly")
        
        try:
            logger.info("✅ SQLite fallback system shut down")
        except Exception:
            pass
    
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
                    password_hash TEXT DEFAULT NULL,
                    auth_type TEXT DEFAULT 'linkedin_only',
                    role TEXT DEFAULT 'user',
                    must_change_password INTEGER DEFAULT 0,
                    failed_attempts INTEGER DEFAULT 0,
                    lock_until TEXT DEFAULT NULL,
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
                    scrape_run_id INTEGER,
                    latitude REAL,
                    longitude REAL,
                    normalized_job_title_id INTEGER,
                    normalized_company_id INTEGER,
                    exp2_title TEXT,
                    exp2_company TEXT,
                    exp2_dates TEXT,
                    exp3_title TEXT,
                    exp3_company TEXT,
                    exp3_dates TEXT,
                    job_employment_type TEXT,
                    exp2_employment_type TEXT,
                    exp3_employment_type TEXT,
                    school TEXT,
                    school2 TEXT,
                    school3 TEXT,
                    degree2 TEXT,
                    degree3 TEXT,
                    major2 TEXT,
                    major3 TEXT,
                    standardized_degree TEXT,
                    standardized_degree2 TEXT,
                    standardized_degree3 TEXT,
                    standardized_major TEXT,
                    standardized_major2 TEXT,
                    standardized_major3 TEXT,
                    standardized_major_alt TEXT,
                    job_1_relevance_score REAL,
                    job_2_relevance_score REAL,
                    job_3_relevance_score REAL,
                    job_1_is_relevant INTEGER,
                    job_2_is_relevant INTEGER,
                    job_3_is_relevant INTEGER,
                    relevant_experience_months INTEGER,
                    seniority_level TEXT,
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
                    last_scrape_run_id INTEGER,
                    visited_at TEXT DEFAULT (datetime('now')),
                    last_checked TEXT DEFAULT (datetime('now')),
                    needs_update INTEGER DEFAULT 0,
                    notes TEXT
                );

                -- Scrape runs table
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
                );

                -- Scrape run flagged profile reasons
                CREATE TABLE IF NOT EXISTS scrape_run_flags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scrape_run_id INTEGER NOT NULL,
                    linkedin_url TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(scrape_run_id, linkedin_url, reason)
                );
                
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
                
                -- Authorized emails table (whitelist for non-UNT emails)
                CREATE TABLE IF NOT EXISTS authorized_emails (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE NOT NULL,
                    added_by_user_id INTEGER,
                    added_at TEXT DEFAULT (datetime('now')),
                    notes TEXT
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
                
                -- Normalized job titles lookup table
                CREATE TABLE IF NOT EXISTS normalized_job_titles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    normalized_title TEXT NOT NULL UNIQUE,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                -- Normalized degrees lookup table
                CREATE TABLE IF NOT EXISTS normalized_degrees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    normalized_degree TEXT NOT NULL UNIQUE,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                -- Normalized companies lookup table
                CREATE TABLE IF NOT EXISTS normalized_companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    normalized_company TEXT NOT NULL UNIQUE,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                -- Scraper activity tracking (internal: who scraped how much)
                CREATE TABLE IF NOT EXISTS scraper_activity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE NOT NULL,
                    profiles_scraped INTEGER DEFAULT 0,
                    last_scraped_at TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                -- Create indexes for better query performance
                CREATE INDEX IF NOT EXISTS idx_pending_sync_table ON _pending_sync(table_name);
                CREATE INDEX IF NOT EXISTS idx_pending_sync_created ON _pending_sync(created_at);
                CREATE INDEX IF NOT EXISTS idx_normalized_title ON normalized_job_titles(normalized_title);
                CREATE INDEX IF NOT EXISTS idx_normalized_degree ON normalized_degrees(normalized_degree);
                CREATE INDEX IF NOT EXISTS idx_normalized_company ON normalized_companies(normalized_company);
                CREATE INDEX IF NOT EXISTS idx_alumni_name_sort ON alumni(last_name, first_name, id);
                CREATE INDEX IF NOT EXISTS idx_alumni_linkedin_url ON alumni(linkedin_url);
                CREATE INDEX IF NOT EXISTS idx_notes_user_alumni_lookup ON notes(user_id, alumni_id);
                CREATE INDEX IF NOT EXISTS idx_user_interactions_user_alumni ON user_interactions(user_id, alumni_id);
                CREATE INDEX IF NOT EXISTS idx_user_interactions_user_updated ON user_interactions(user_id, updated_at);
            """)
        
        # ── Migrations for existing databases ──
        # Add relevance scoring columns if they don't exist yet
        # (covers databases created before these columns were added to CREATE TABLE)
        _migration_cols = [
            ("job_1_relevance_score", "REAL"),
            ("job_2_relevance_score", "REAL"),
            ("job_3_relevance_score", "REAL"),
            ("job_1_is_relevant", "INTEGER"),
            ("job_2_is_relevant", "INTEGER"),
            ("job_3_is_relevant", "INTEGER"),
            ("relevant_experience_months", "INTEGER"),
            ("seniority_level", "TEXT"),
            # Normalization foreign keys (used by app.py LEFT JOINs)
            ("normalized_company_id", "INTEGER"),
            ("normalized_job_title_id", "INTEGER"),
            # Working-while-studying status
            ("working_while_studying_status", "TEXT"),
            ("updated_at", "TEXT"),
            # Discipline classification
            ("discipline", "TEXT"),
            # Secondary major for multi-entry mapping (CS&E -> CS + CE)
            ("standardized_major_alt", "TEXT"),
            ("job_employment_type", "TEXT"),
            ("exp2_employment_type", "TEXT"),
            ("exp3_employment_type", "TEXT"),
        ]
        for col_name, col_type in _migration_cols:
            try:
                conn.execute(f"ALTER TABLE alumni ADD COLUMN {col_name} {col_type}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Auth columns for users table (covers pre-auth databases)
        _user_auth_cols = [
            ("password_hash", "TEXT DEFAULT NULL"),
            ("auth_type", "TEXT DEFAULT 'linkedin_only'"),
            ("role", "TEXT DEFAULT 'user'"),
            ("must_change_password", "INTEGER DEFAULT 0"),
        ]
        for col_name, col_def in _user_auth_cols:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        conn.commit()
        
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
            port=MYSQL_PORT,
            connection_timeout=5,
        )
    
    def _register_mysql_functions(self, conn):
        """Register MySQL-compatible functions for use in SQLite queries."""
        def _substring_index(s, delim, count):
            """SQLite equivalent of MySQL's SUBSTRING_INDEX."""
            if s is None:
                return None
            count = int(count)
            parts = s.split(delim)
            if count > 0:
                return delim.join(parts[:count]) if count < len(parts) else s
            elif count < 0:
                return delim.join(parts[count:]) if abs(count) < len(parts) else s
            return ''

        conn.create_function("SUBSTRING_INDEX", 3, _substring_index)

    def get_sqlite_connection(self):
        """Get a SQLite connection with proper settings."""
        conn = sqlite3.connect(str(SQLITE_DB_PATH), timeout=SQLITE_TIMEOUT, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self._register_mysql_functions(conn)
        return conn
    
    def get_connection(self):
        """
        Get the appropriate database connection.
        Returns MySQL if online, SQLite if offline.
        """
        try:
            from .sf_core_wrappers import SQLiteConnectionWrapper
        except ImportError:
            from sf_core_wrappers import SQLiteConnectionWrapper

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
        valid_columns = self._get_mysql_table_columns(mysql_conn, table_name)
        filtered_pk = {k: v for k, v in (pk or {}).items() if k in valid_columns}
        filtered_new_data = {k: v for k, v in (new_data or {}).items() if k in valid_columns}

        dropped_columns = sorted(set((new_data or {}).keys()) - set(filtered_new_data.keys()))
        if dropped_columns:
            logger.warning(
                f"⚠️ Skipping unknown cloud columns for {table_name}: {', '.join(dropped_columns)}"
            )

        with mysql_conn.cursor() as cur:
            if operation == 'INSERT':
                columns = list(filtered_new_data.keys())
                if not columns:
                    logger.warning(f"⚠️ Skipping INSERT for {table_name}: no schema-compatible columns")
                    return
                placeholders = ', '.join(['%s'] * len(columns))
                col_names = ', '.join(columns)
                values = [filtered_new_data[c] for c in columns]
                
                # Use INSERT ... ON DUPLICATE KEY UPDATE for upsert
                update_clause = ', '.join([f"{c} = VALUES({c})" for c in columns if c not in filtered_pk])
                
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
                if not filtered_pk:
                    logger.warning(f"⚠️ Skipping UPDATE for {table_name}: primary key not present in cloud schema")
                    return
                # Build SET clause
                set_clauses = []
                values = []
                for key, value in filtered_new_data.items():
                    if key not in filtered_pk:
                        set_clauses.append(f"{key} = %s")
                        values.append(value)
                
                # Build WHERE clause
                where_clauses = []
                for key, value in filtered_pk.items():
                    where_clauses.append(f"{key} = %s")
                    values.append(value)
                
                set_sql = ", ".join(set_clauses)
                where_sql = " AND ".join(where_clauses)
                
                if set_sql:
                    cur.execute(f"UPDATE {table_name} SET {set_sql} WHERE {where_sql}", values)
                
            elif operation == 'DELETE':
                if not filtered_pk:
                    logger.warning(f"⚠️ Skipping DELETE for {table_name}: primary key not present in cloud schema")
                    return
                where_clauses = []
                values = []
                for key, value in filtered_pk.items():
                    where_clauses.append(f"{key} = %s")
                    values.append(value)
                
                where_sql = " AND ".join(where_clauses)
                cur.execute(f"DELETE FROM {table_name} WHERE {where_sql}", values)

    def _get_mysql_table_columns(self, mysql_conn, table_name) -> set:
        """Return available cloud columns for a table, cached for sync performance."""
        cached = self._mysql_table_columns_cache.get(table_name)
        if cached is not None:
            return cached

        with mysql_conn.cursor() as cur:
            cur.execute(f"SHOW COLUMNS FROM {table_name}")
            cols = {row[0] for row in cur.fetchall()}

        self._mysql_table_columns_cache[table_name] = cols
        return cols
    
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
                              old_data: dict = None, new_data: dict = None,
                              force: bool = False):
        """Record a pending local change for later sync to cloud.
        
        Args:
            force: If True, record the change even when not officially offline.
                   Used when cloud writes are disabled mid-run (e.g., after
                   5 consecutive failures) but the app hasn't gone fully offline.
        """
        if not force and not self._is_offline:
            return  # Only record when offline (unless forced)
        
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

    def get_pending_count(self) -> int:
        """Return the number of pending local changes waiting to be synced to cloud."""
        try:
            conn = self.get_sqlite_connection()
            try:
                row = conn.execute("SELECT COUNT(*) FROM _pending_sync").fetchone()
                return row[0] if row else 0
            finally:
                conn.close()
        except Exception:
            return 0

    def get_local_alumni_count(self) -> int:
        """Return the number of alumni records in local SQLite."""
        try:
            conn = self.get_sqlite_connection()
            try:
                row = conn.execute("SELECT COUNT(*) FROM alumni").fetchone()
                return row[0] if row else 0
            finally:
                conn.close()
        except Exception:
            return 0
    
    def force_sync(self) -> dict:
        """Manually trigger a sync. Returns status dict."""
        if not self.check_cloud_connection():
            return {"success": False, "error": "Cloud is not reachable"}
        
        try:
            pending_before = self.get_pending_count()
            self._push_pending_changes()
            self._pull_cloud_to_local(incremental=True)
            pending_after = self.get_pending_count()
            self._last_cloud_sync = utc_now()
            self._save_sync_state()

            # If we were offline, go online now
            if self._is_offline:
                self._go_online()

            return {
                "success": True,
                "last_sync": self._last_cloud_sync,
                "synced_count": max(0, pending_before - pending_after),
                "remaining": pending_after,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def pull_from_cloud_only(self, incremental: bool = True) -> dict:
        """Copy cloud (MySQL) into local SQLite without pushing _pending_sync."""
        if not self.check_cloud_connection():
            return {"success": False, "error": "Cloud is not reachable"}
        try:
            self._pull_cloud_to_local(incremental=incremental)
            self._last_cloud_sync = utc_now()
            self._save_sync_state()
            return {"success": True, "last_sync": self._last_cloud_sync}
        except Exception as e:
            return {"success": False, "error": str(e)}

