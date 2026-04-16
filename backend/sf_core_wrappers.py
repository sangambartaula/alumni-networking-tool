try:
    from .sf_core_shared import *
    from .sf_core_manager import ConnectionManager
except ImportError:
    from sf_core_shared import *
    from sf_core_manager import ConnectionManager

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
    
    def is_connected(self):
        """Duck-typed method to emulate mysql-connector connection interface."""
        return True
    
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
        import re

        query_stripped = query.strip()

        # Handle CREATE TABLE IF NOT EXISTS — skip if table already exists.
        # SQLite tables are pre-created by _init_sqlite with proper schema,
        # so we can safely skip MySQL-syntax CREATE statements.
        create_match = re.match(
            r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)',
            query_stripped, re.IGNORECASE
        )
        if create_match:
            table_name = create_match.group(1)
            self._cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if self._cursor.fetchone():
                return  # Table already exists, skip MySQL-specific CREATE

        # Handle ALTER TABLE ... ADD COLUMN — skip if column already exists.
        alter_match = re.match(
            r'ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)',
            query_stripped, re.IGNORECASE
        )
        if alter_match:
            table_name = alter_match.group(1)
            col_name = alter_match.group(2)
            self._cursor.execute(f"PRAGMA table_info({table_name})")
            existing_cols = [row[1] for row in self._cursor.fetchall()]
            if col_name in existing_cols:
                return  # Column already exists, skip
            # Column doesn't exist yet — clean up MySQL-specific DDL syntax
            query_stripped = re.sub(
                r'\s+ON\s+UPDATE\s+CURRENT_TIMESTAMP',
                '', query_stripped, flags=re.IGNORECASE
            )

        # Translate MySQL placeholder %s to SQLite placeholder ?
        translated_query = query_stripped.replace('%s', '?')

        # Handle INSERT IGNORE -> INSERT OR IGNORE
        translated_query = re.sub(
            r'INSERT\s+IGNORE\s+INTO',
            'INSERT OR IGNORE INTO',
            translated_query,
            flags=re.IGNORECASE
        )

        # Handle ON DUPLICATE KEY UPDATE -> ON CONFLICT DO UPDATE
        if 'ON DUPLICATE KEY UPDATE' in translated_query.upper():
            translated_query = self._convert_upsert(translated_query)

        # Handle MySQL NOW() -> SQLite datetime('now')
        translated_query = translated_query.replace('NOW()', "datetime('now', 'utc')")

        # Handle CURRENT_TIMESTAMP only in DML (INSERT/UPDATE/SELECT/DELETE).
        # In DDL (CREATE/ALTER), SQLite natively supports DEFAULT CURRENT_TIMESTAMP.
        if not re.match(r'\s*(CREATE|ALTER)\s+', translated_query, re.IGNORECASE):
            translated_query = re.sub(
                r'\bCURRENT_TIMESTAMP\b',
                "datetime('now', 'utc')",
                translated_query,
                flags=re.IGNORECASE
            )

        # Handle MySQL GREATEST() -> SQLite MAX()
        translated_query = re.sub(
            r'\bGREATEST\s*\(',
            'MAX(',
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
    
    @property
    def description(self):
        return self._cursor.description
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


# Singleton accessor
_connection_manager = None


