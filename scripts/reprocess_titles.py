#!/usr/bin/env python3
"""
Review-first retroactive title reprocessing.

Step 1 (report only):
  python scripts/reprocess_titles.py report --use-groq

Step 2 (after manual review of CSV):
  python scripts/reprocess_titles.py apply --input <report.csv>

CSV includes one row per profile + title slot (current/exp2/exp3) where the
new normalization differs from the old normalization.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scraper"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("reprocess_titles")

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass


REPORT_HEADERS = [
    "profile_id",
    "profile_url",
    "slot",
    "raw_title",
    "old_norm_title",
    "new_norm_title",
    "change_type",
    "approved",
    "review_note",
]


def _use_sqlite() -> bool:
    return str(os.getenv("USE_SQLITE_FALLBACK", "")).strip().lower() in {"1", "true", "yes"}


def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _default_report_path() -> Path:
    out_dir = ROOT / "scraper" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"title_reprocess_report_{_timestamp_slug()}.csv"


def _truthy(text: str) -> bool:
    return (text or "").strip().lower() in {"1", "true", "yes", "y", "approved"}


def _fetch_existing_norm_title_map(conn, sqlite: bool) -> dict[int, str]:
    cur = conn.cursor(dictionary=not sqlite)
    try:
        cur.execute("SELECT id, normalized_title FROM normalized_job_titles")
        rows = cur.fetchall()
        out: dict[int, str] = {}
        for row in rows:
            if isinstance(row, dict):
                key = row.get("id")
                val = row.get("normalized_title")
            else:
                key, val = row[0], row[1]
            if key is not None:
                out[int(key)] = str(val or "").strip()
        return out
    finally:
        cur.close()


def _fetch_profiles(conn, sqlite: bool) -> list[dict]:
    cur = conn.cursor(dictionary=not sqlite)
    try:
        cur.execute(
            """
            SELECT id, linkedin_url, current_job_title, exp2_title, exp3_title, normalized_job_title_id
            FROM alumni
            ORDER BY id
            """
        )
        rows = cur.fetchall()
        if sqlite:
            return [
                {
                    "id": row[0],
                    "linkedin_url": row[1],
                    "current_job_title": row[2],
                    "exp2_title": row[3],
                    "exp3_title": row[4],
                    "normalized_job_title_id": row[5],
                }
                for row in rows
            ]
        return rows
    finally:
        cur.close()


def _old_norm_for_slot(profile: dict, slot: str, norm_map: dict[int, str], normalize_title_deterministic) -> str:
    if slot == "current":
        norm_id = profile.get("normalized_job_title_id")
        if norm_id is None:
            return ""
        return norm_map.get(int(norm_id), "")
    raw = (profile.get("exp2_title") if slot == "exp2" else profile.get("exp3_title")) or ""
    return normalize_title_deterministic(str(raw).strip()) if str(raw).strip() else ""


def _make_report_rows(conn, sqlite: bool, use_groq: bool, batch_size: int):
    from job_title_normalization import (
        NEW_GROQ_TITLES,
        get_title_normalization_session_stats,
        normalize_title_deterministic,
        reset_title_normalization_session_counters,
        resolve_title_for_scrape,
    )

    reset_title_normalization_session_counters()
    norm_map = _fetch_existing_norm_title_map(conn, sqlite)
    profiles = _fetch_profiles(conn, sqlite)
    total = len(profiles)
    rows: list[dict[str, str]] = []

    for idx, profile in enumerate(profiles, start=1):
        profile_id = profile.get("id")
        profile_url = (profile.get("linkedin_url") or "").strip()
        slots = [
            ("current", (profile.get("current_job_title") or "").strip()),
            ("exp2", (profile.get("exp2_title") or "").strip()),
            ("exp3", (profile.get("exp3_title") or "").strip()),
        ]
        for slot, raw in slots:
            if not raw:
                continue
            old_norm = _old_norm_for_slot(profile, slot, norm_map, normalize_title_deterministic).strip()
            new_norm = (
                resolve_title_for_scrape(raw, extra_existing=None).strip()
                if use_groq
                else normalize_title_deterministic(raw).strip()
            )
            if old_norm == new_norm:
                continue
            rows.append(
                {
                    "profile_id": str(profile_id or ""),
                    "profile_url": profile_url,
                    "slot": slot,
                    "raw_title": raw,
                    "old_norm_title": old_norm,
                    "new_norm_title": new_norm,
                    "change_type": "new_label" if not old_norm else "changed_label",
                    "approved": "",
                    "review_note": "",
                }
            )
        if idx % max(1, batch_size) == 0:
            logger.info("Report progress: %s / %s profiles", idx, total)

    return rows, get_title_normalization_session_stats(), sorted(NEW_GROQ_TITLES)


def _write_report(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def _get_or_create_norm_id_from_title(cur, sqlite: bool, title: str) -> int | None:
    t = (title or "").strip()
    if not t:
        return None
    if sqlite:
        cur.execute("INSERT OR IGNORE INTO normalized_job_titles (normalized_title) VALUES (?)", (t,))
        cur.execute("SELECT id FROM normalized_job_titles WHERE normalized_title = ?", (t,))
    else:
        cur.execute(
            "INSERT INTO normalized_job_titles (normalized_title) VALUES (%s) "
            "ON DUPLICATE KEY UPDATE normalized_title = VALUES(normalized_title)",
            (t,),
        )
        cur.execute("SELECT id FROM normalized_job_titles WHERE normalized_title = %s", (t,))
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0] if not isinstance(row, dict) else row.get("id"))


def _apply_from_report(conn, sqlite: bool, input_csv: Path, batch_size: int, dry_run: bool) -> tuple[int, int, int]:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    approved_rows = 0
    skipped_rows = 0
    updated_profiles = 0
    ph = "?" if sqlite else "%s"

    with input_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        if not _truthy(row.get("approved", "")):
            skipped_rows += 1
            continue
        if (row.get("slot") or "").strip() != "current":
            skipped_rows += 1
            continue

        profile_id = (row.get("profile_id") or "").strip()
        new_norm = (row.get("new_norm_title") or "").strip()
        if not profile_id:
            skipped_rows += 1
            continue

        cur = conn.cursor(dictionary=not sqlite)
        try:
            new_id = _get_or_create_norm_id_from_title(cur, sqlite, new_norm)
            if new_id is None:
                cur.execute(
                    f"UPDATE alumni SET normalized_job_title_id = NULL WHERE id = {ph}",
                    (int(profile_id),),
                )
            else:
                cur.execute(
                    f"UPDATE alumni SET normalized_job_title_id = {ph} WHERE id = {ph}",
                    (new_id, int(profile_id)),
                )
            updated_profiles += int(cur.rowcount or 0)
            approved_rows += 1
        finally:
            cur.close()

        if idx % max(1, batch_size) == 0:
            logger.info("Apply progress: %s / %s report rows", idx, total)
            if not dry_run:
                conn.commit()

    return approved_rows, skipped_rows, updated_profiles


def main() -> int:
    from database import ensure_normalized_job_title_column, get_connection, init_db

    parser = argparse.ArgumentParser(description="Review-first title reprocessing.")
    parser.add_argument("--use-groq", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")

    sub = parser.add_subparsers(dest="command", required=False)
    p_report = sub.add_parser("report", help="Generate CSV report of proposed title changes.")
    p_report.add_argument("--use-groq", action=argparse.BooleanOptionalAction, default=None)
    p_report.add_argument("--output", default="", help="Output CSV path. Default: scraper/output/<timestamp>.csv")
    p_apply = sub.add_parser("apply", help="Apply only approved rows from report CSV.")
    p_apply.add_argument("--use-groq", action=argparse.BooleanOptionalAction, default=None)
    p_apply.add_argument("--input", required=True, help="Reviewed report CSV path.")

    args = parser.parse_args()
    command = args.command or "report"
    use_groq = True if args.use_groq is None else bool(args.use_groq)

    init_db()
    ensure_normalized_job_title_column()

    sqlite = _use_sqlite()
    conn = get_connection()
    try:
        try:
            conn.autocommit = False
        except Exception:
            pass

        if command == "report":
            rows, stats, new_groq = _make_report_rows(
                conn=conn,
                sqlite=sqlite,
                use_groq=use_groq,
                batch_size=int(args.batch_size),
            )
            out_path = Path(args.output).resolve() if args.output else _default_report_path()
            _write_report(out_path, rows)
            logger.info("Report written: %s", out_path)
            logger.info("Rows requiring review: %s", len(rows))
            logger.info(
                "Session stats: groq_calls=%s deterministic_fallbacks=%s new_groq_titles=%s",
                stats.get("groq_calls", 0),
                stats.get("deterministic_fallbacks", 0),
                len(new_groq),
            )
            if new_groq:
                logger.info("New Groq titles sample: %s", new_groq[:40])
            conn.rollback()
            return 0

        if command == "apply":
            approved_rows, skipped_rows, updated_profiles = _apply_from_report(
                conn=conn,
                sqlite=sqlite,
                input_csv=Path(args.input).resolve(),
                batch_size=int(args.batch_size),
                dry_run=bool(args.dry_run),
            )
            if args.dry_run:
                conn.rollback()
                logger.info("Dry run: rolled back apply changes.")
            else:
                conn.commit()
                logger.info("Committed approved changes.")
            logger.info(
                "Apply summary: approved_rows=%s skipped_rows=%s updated_profiles=%s",
                approved_rows,
                skipped_rows,
                updated_profiles,
            )
            return 0

        logger.error("Unknown command: %s", command)
        return 2
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("reprocess_titles failed")
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
