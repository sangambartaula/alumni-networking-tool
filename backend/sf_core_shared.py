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

for _enc in ('utf-8', 'latin-1'):
    try:
        load_dotenv(encoding=_enc)
        break
    except Exception:
        continue

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
    },
    'authorized_emails': {
        'pk': ['id'],
        'unique_cols': ['email'],
        'timestamp_col': 'added_at'
    },
    'normalized_job_titles': {
        'pk': ['id'],
        'unique_cols': ['normalized_title'],
        'timestamp_col': 'created_at'
    },
    'normalized_degrees': {
        'pk': ['id'],
        'unique_cols': ['normalized_degree'],
        'timestamp_col': 'created_at'
    },
    'normalized_companies': {
        'pk': ['id'],
        'unique_cols': ['normalized_company'],
        'timestamp_col': 'created_at'
    },
    'scraper_activity': {
        'pk': ['id'],
        'unique_cols': ['email'],
        'timestamp_col': 'created_at'
    },
    'scrape_runs': {
        'pk': ['id'],
        'unique_cols': ['run_uuid'],
        'timestamp_col': 'started_at'
    },
    'scrape_run_flags': {
        'pk': ['id'],
        'unique_cols': ['scrape_run_id', 'linkedin_url', 'reason'],
        'timestamp_col': 'created_at'
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


