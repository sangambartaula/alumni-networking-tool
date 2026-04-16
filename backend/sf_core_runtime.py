try:
    from .sf_core_shared import *
    from .sf_core_manager import ConnectionManager
    from .sf_core_wrappers import SQLiteConnectionWrapper, SQLiteCursorWrapper
except ImportError:
    from sf_core_shared import *
    from sf_core_manager import ConnectionManager
    from sf_core_wrappers import SQLiteConnectionWrapper, SQLiteCursorWrapper


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
