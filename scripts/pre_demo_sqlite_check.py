#!/usr/bin/env python3
"""
Pre-demo SQLite fallback verification.

What this does:
1) Forces cloud -> local SQLite sync.
2) Runs built-in fallback integrity tests.
3) Optionally smoke-tests key web app routes in SQLite-only mode.

Usage:
  python scripts/pre_demo_sqlite_check.py
  python scripts/pre_demo_sqlite_check.py --skip-web-smoke
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))


def _print_kv(key: str, value) -> None:
    print(f"{key}: {value}")


def run_sync_and_integrity_checks() -> int:
    from sqlite_fallback import get_connection_manager, get_fallback_status
    from sqlite_fallback_core import init_fallback_system, test_offline_mode

    print("=== FORCE SYNC (cloud -> sqlite) ===")
    mgr = get_connection_manager()
    sync_res = mgr.force_sync()
    _print_kv("force_sync.success", sync_res.get("success"))
    _print_kv("force_sync.error", sync_res.get("error", ""))
    _print_kv("force_sync.last_sync", sync_res.get("last_sync"))
    _print_kv("force_sync.remaining", sync_res.get("remaining"))
    if not sync_res.get("success"):
        return 1

    print("\n=== INIT FALLBACK + INTEGRITY TESTS ===")
    init_fallback_system()
    tests = test_offline_mode()
    _print_kv("tests.passed", tests.get("passed"))
    _print_kv("tests.failed", tests.get("failed"))
    for t in tests.get("tests", []):
        status = "PASS" if t.get("passed") else "FAIL"
        msg = t.get("message", "")
        print(f"- {status}: {t.get('name')} {msg}")
    if tests.get("failed", 0) > 0:
        return 1

    status = get_fallback_status()
    print("\n=== STATUS SNAPSHOT ===")
    _print_kv("offline_mode", status.get("is_offline"))
    _print_kv("last_cloud_sync", status.get("last_cloud_sync"))
    _print_kv("pending_changes", status.get("pending_changes"))
    _print_kv("alumni_rows", status.get("table_counts", {}).get("alumni"))
    _print_kv("normalized_job_titles", status.get("table_counts", {}).get("normalized_job_titles"))
    return 0


def run_sqlite_only_web_smoke() -> int:
    print("\n=== SQLITE-ONLY WEB APP SMOKE ===")
    os.environ["USE_SQLITE_FALLBACK"] = "1"
    os.environ["DISABLE_DB"] = "1"
    os.environ.setdefault("FLASK_ENV", "production")

    try:
        import app as backend_app
    except Exception as exc:
        print(f"Could not import backend app for smoke test: {exc}")
        print("This usually means a missing dependency in this Python environment.")
        return 2

    client = backend_app.app.test_client()
    paths = [
        "/",
        "/alumni.html",
        "/api/alumni/filter-options",
        "/api/alumni?limit=5&offset=0",
        "/api/alumni/filter?limit=5&offset=0",
        "/api/analytics/overview",
    ]
    failures = 0
    for path in paths:
        try:
            res = client.get(path)
            code = int(res.status_code)
            print(f"{path} -> {code}")
            if code >= 500:
                failures += 1
        except Exception as exc:
            failures += 1
            print(f"{path} -> EXCEPTION: {exc}")
    return 0 if failures == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-demo SQLite fallback checker.")
    parser.add_argument(
        "--skip-web-smoke",
        action="store_true",
        help="Skip Flask route smoke test step.",
    )
    args = parser.parse_args()

    rc = run_sync_and_integrity_checks()
    if rc != 0:
        return rc

    if not args.skip_web_smoke:
        smoke_rc = run_sqlite_only_web_smoke()
        if smoke_rc == 2:
            # Dependency/environment issue, not necessarily product issue.
            return 2
        if smoke_rc != 0:
            return smoke_rc

    print("\n✅ Pre-demo SQLite fallback checks completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
