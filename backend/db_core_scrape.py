try:
    from .db_core_common import *
    from .db_core_common import (
        _clean_optional_text,
        _parse_bool,
        _parse_float,
        _parse_int,
        _normalize_person_name,
        _normalize_primary_education_dates,
        _sanitize_major_and_discipline,
        _get_or_create_normalized_entity,
    )
    from .db_core_schema import ensure_scrape_run_tracking_schema
except ImportError:
    from db_core_common import *
    from db_core_common import (
        _clean_optional_text,
        _parse_bool,
        _parse_float,
        _parse_int,
        _normalize_person_name,
        _normalize_primary_education_dates,
        _sanitize_major_and_discipline,
        _get_or_create_normalized_entity,
    )
    from db_core_schema import ensure_scrape_run_tracking_schema

def increment_scraper_activity(email):
    """
    Increment the profiles_scraped counter for a scraper email.
    Upserts: if the email doesn't exist yet, creates a new row.
    Works with both MySQL and SQLite fallback.
    Always mirrors to local SQLite so the GUI can display counts offline.
    """
    if not email:
        return
    email = email.strip().lower()
    wrote_to_sqlite_via_primary = False
    try:
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            is_sqlite_routed = conn.__class__.__name__ == "SQLiteConnectionWrapper"
            execute_sql(
                cur,
                """
                    INSERT INTO scraper_activity (email, profiles_scraped, last_scraped_at)
                    VALUES (%s, 1, NOW())
                    ON DUPLICATE KEY UPDATE
                        profiles_scraped = profiles_scraped + 1,
                        last_scraped_at = NOW()
                """,
                (email,),
                connection=conn,
                sqlite_query="""
                    INSERT INTO scraper_activity (email, profiles_scraped, last_scraped_at)
                    VALUES (?, 1, datetime('now'))
                    ON CONFLICT(email) DO UPDATE SET
                        profiles_scraped = profiles_scraped + 1,
                        last_scraped_at = datetime('now')
                """,
            )
            if is_sqlite_routed:
                wrote_to_sqlite_via_primary = True
    except Exception as err:
        logger.error(f"Error incrementing scraper activity for {email}: {err}")

    # Mirror to local SQLite so GUI scrape count works offline.
    # Skip if the primary write already went to SQLite (avoid double-count).
    if not wrote_to_sqlite_via_primary:
        try:
            from sqlite_fallback import get_connection_manager
            manager = get_connection_manager()
            sqlite_conn = manager.get_sqlite_connection()
            try:
                sqlite_conn.execute("""
                    INSERT INTO scraper_activity (email, profiles_scraped, last_scraped_at)
                    VALUES (?, 1, datetime('now'))
                    ON CONFLICT(email) DO UPDATE SET
                        profiles_scraped = profiles_scraped + 1,
                        last_scraped_at = datetime('now')
                """, (email,))
                sqlite_conn.commit()
            finally:
                try:
                    sqlite_conn.close()
                except Exception:
                    pass
        except Exception as sqlite_err:
            logger.debug(f"SQLite mirror of scraper activity skipped: {sqlite_err}")


def get_scraper_activity():
    """
    Get all scraper activity records.
    Returns a list of dicts with email, profiles_scraped, last_scraped_at, created_at.
    Works with both MySQL and SQLite fallback.
    """
    try:
        with managed_db_cursor(get_connection, dictionary=True) as (_conn, cur):
            cur.execute("""
                SELECT email, profiles_scraped, last_scraped_at, created_at
                FROM scraper_activity
                ORDER BY profiles_scraped DESC
            """)
            rows = cur.fetchall()

        # Convert datetime objects to strings for JSON serialization
        for row in rows:
            for key in ('last_scraped_at', 'created_at'):
                val = row.get(key)
                if hasattr(val, 'isoformat'):
                    row[key] = val.isoformat()

        return rows
    except Exception as err:
        logger.error(f"Error fetching scraper activity: {err}")
        return []


def create_scrape_run(run_uuid, scraper_email=None, scraper_mode=None, selected_disciplines=None):
    """Create a scrape run metadata row and return the numeric run id."""
    if not run_uuid:
        return None

    selected_text = None
    if isinstance(selected_disciplines, (list, tuple, set)):
        selected_text = ",".join([str(x).strip() for x in selected_disciplines if str(x).strip()]) or None
    elif selected_disciplines:
        selected_text = str(selected_disciplines).strip() or None

    clean_email = _clean_optional_text(scraper_email)
    try:
        ensure_scrape_run_tracking_schema()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            if clean_email:
                execute_sql(
                    cur,
                    """
                    UPDATE scrape_runs
                    SET status = 'interrupted',
                        completed_at = NOW(),
                        notes = %s
                    WHERE LOWER(COALESCE(scraper_email, '')) = LOWER(%s)
                      AND LOWER(COALESCE(status, '')) = 'running'
                      AND run_uuid <> %s
                    """,
                    ("Auto-closed due to newer run start", clean_email, run_uuid),
                    connection=conn,
                    sqlite_query="""
                    UPDATE scrape_runs
                    SET status = 'interrupted',
                        completed_at = datetime('now'),
                        notes = ?
                    WHERE LOWER(COALESCE(scraper_email, '')) = LOWER(?)
                      AND LOWER(COALESCE(status, '')) = 'running'
                      AND run_uuid <> ?
                    """,
                )
            execute_sql(
                cur,
                """
                INSERT INTO scrape_runs (run_uuid, scraper_email, scraper_mode, selected_disciplines, status)
                VALUES (%s, %s, %s, %s, 'running')
                ON DUPLICATE KEY UPDATE
                    scraper_email = VALUES(scraper_email),
                    scraper_mode = VALUES(scraper_mode),
                    selected_disciplines = VALUES(selected_disciplines),
                    status = 'running'
                """,
                (run_uuid, clean_email, _clean_optional_text(scraper_mode), selected_text),
                connection=conn,
                sqlite_query="""
                INSERT INTO scrape_runs (run_uuid, scraper_email, scraper_mode, selected_disciplines, status)
                VALUES (?, ?, ?, ?, 'running')
                ON CONFLICT(run_uuid) DO UPDATE SET
                    scraper_email = excluded.scraper_email,
                    scraper_mode = excluded.scraper_mode,
                    selected_disciplines = excluded.selected_disciplines,
                    status = 'running'
                """,
            )
            execute_sql(
                cur,
                "SELECT id FROM scrape_runs WHERE run_uuid = %s",
                (run_uuid,),
                connection=conn,
            )
            row = cur.fetchone()
            run_id = row[0] if row and not isinstance(row, dict) else (row or {}).get("id")
            return run_id
    except Exception as err:
        logger.warning(f"Could not create scrape run metadata for run_uuid={run_uuid}: {err}")
        return None


def finalize_scrape_run(
    run_id,
    status="completed",
    profiles_scraped=0,
    cloud_disabled=False,
    geocode_unknown_count=0,
    geocode_network_failure_count=0,
    notes=None,
):
    """Finalize scrape run metadata after a run exits."""
    if not run_id:
        return False

    try:
        ensure_scrape_run_tracking_schema()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                """
                UPDATE scrape_runs
                SET status = %s,
                    profiles_scraped = %s,
                    cloud_disabled = %s,
                    geocode_unknown_count = %s,
                    geocode_network_failure_count = %s,
                    completed_at = NOW(),
                    notes = %s
                WHERE id = %s
                """,
                (
                    _clean_optional_text(status) or "completed",
                    int(profiles_scraped or 0),
                    bool(cloud_disabled),
                    int(geocode_unknown_count or 0),
                    int(geocode_network_failure_count or 0),
                    _clean_optional_text(notes),
                    int(run_id),
                ),
                connection=conn,
                sqlite_query="""
                UPDATE scrape_runs
                SET status = ?,
                    profiles_scraped = ?,
                    cloud_disabled = ?,
                    geocode_unknown_count = ?,
                    geocode_network_failure_count = ?,
                    completed_at = datetime('now'),
                    notes = ?
                WHERE id = ?
                """,
            )
            return True
    except Exception as err:
        logger.warning(f"Could not finalize scrape run id={run_id}: {err}")
        return False


def increment_scrape_run_profiles(run_id, delta=1):
    """Increment profiles_scraped for an active scrape run in real time.
    Always mirrors to local SQLite so run history shows correct counts offline.
    """
    if not run_id:
        return False

    wrote_to_sqlite_via_primary = False
    try:
        ensure_scrape_run_tracking_schema()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            is_sqlite_routed = conn.__class__.__name__ == "SQLiteConnectionWrapper"
            execute_sql(
                cur,
                """
                UPDATE scrape_runs
                SET profiles_scraped = COALESCE(profiles_scraped, 0) + %s
                WHERE id = %s
                """,
                (int(delta or 1), int(run_id)),
                connection=conn,
                sqlite_query="""
                UPDATE scrape_runs
                SET profiles_scraped = COALESCE(profiles_scraped, 0) + ?
                WHERE id = ?
                """,
            )
            if is_sqlite_routed:
                wrote_to_sqlite_via_primary = True
            return True
    except Exception as err:
        logger.debug(f"Could not increment scrape run profiles for run_id={run_id}: {err}")
        return False
    finally:
        # Mirror to local SQLite (skip if primary was already SQLite)
        if not wrote_to_sqlite_via_primary:
            try:
                from sqlite_fallback import get_connection_manager
                manager = get_connection_manager()
                sqlite_conn = manager.get_sqlite_connection()
                try:
                    sqlite_conn.execute(
                        """
                        UPDATE scrape_runs
                        SET profiles_scraped = COALESCE(profiles_scraped, 0) + ?
                        WHERE id = ?
                        """,
                        (int(delta or 1), int(run_id)),
                    )
                    sqlite_conn.commit()
                finally:
                    try:
                        sqlite_conn.close()
                    except Exception:
                        pass
            except Exception as sqlite_err:
                logger.debug(f"SQLite mirror of scrape run profiles skipped: {sqlite_err}")


def record_scrape_run_flag(run_id, linkedin_url, reason):
    """Record a flagged profile reason against the scrape run."""
    normalized_url = normalize_url(linkedin_url)
    if not run_id or not normalized_url or not reason:
        return False

    try:
        ensure_scrape_run_tracking_schema()
        with managed_db_cursor(get_connection, commit=True) as (conn, cur):
            execute_sql(
                cur,
                """
                INSERT INTO scrape_run_flags (scrape_run_id, linkedin_url, reason)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE reason = VALUES(reason)
                """,
                (int(run_id), normalized_url, str(reason).strip()),
                connection=conn,
                sqlite_query="""
                INSERT INTO scrape_run_flags (scrape_run_id, linkedin_url, reason)
                VALUES (?, ?, ?)
                ON CONFLICT(scrape_run_id, linkedin_url, reason) DO NOTHING
                """,
            )
            return True
    except Exception as err:
        logger.debug(f"Could not record scrape run flag for run_id={run_id}: {err}")
        return False


def _build_alumni_upsert_payload(profile_data):
    """Normalize a scraped profile dict into the alumni upsert payload."""
    if not isinstance(profile_data, dict):
        return None

    profile_url = normalize_url(profile_data.get('profile_url'))
    name = _normalize_person_name(profile_data.get('name'))
    if not profile_url or not name:
        return None

    name_parts = name.split()
    first_name = name_parts[0] if name_parts else ''
    last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

    grad_year, school_start_date = _normalize_primary_education_dates(
        profile_data.get('graduation_year'),
        profile_data.get('school_start_date'),
    )

    payload = {
        'first_name': first_name,
        'last_name': last_name,
        'grad_year': grad_year,
        'degree': _clean_optional_text(profile_data.get('degree')),
        'major': _clean_optional_text(profile_data.get('major')),
        'discipline': _clean_optional_text(profile_data.get('discipline')),
        'linkedin_url': profile_url,
        'current_job_title': _clean_optional_text(profile_data.get('job_title')),
        'company': _clean_optional_text(profile_data.get('company')),
        'location': _clean_optional_text(profile_data.get('location')),
        'latitude': _parse_float(profile_data.get('latitude')),
        'longitude': _parse_float(profile_data.get('longitude')),
        'headline': _clean_optional_text(profile_data.get('headline')),
        'school_start_date': school_start_date,
        'job_start_date': _clean_optional_text(profile_data.get('job_start_date')),
        'job_end_date': _clean_optional_text(profile_data.get('job_end_date')),
        'working_while_studying': _parse_bool(profile_data.get('working_while_studying')),
        'working_while_studying_status': _clean_optional_text(profile_data.get('working_while_studying_status')),
        'scrape_run_id': _parse_int(profile_data.get('scrape_run_id')),
        'exp2_title': _clean_optional_text(profile_data.get('exp2_title')),
        'exp2_company': _clean_optional_text(profile_data.get('exp2_company')),
        'exp2_dates': _clean_optional_text(profile_data.get('exp2_dates')),
        'exp3_title': _clean_optional_text(profile_data.get('exp3_title')),
        'exp3_company': _clean_optional_text(profile_data.get('exp3_company')),
        'exp3_dates': _clean_optional_text(profile_data.get('exp3_dates')),
        'job_employment_type': _clean_optional_text(profile_data.get('job_employment_type')),
        'exp2_employment_type': _clean_optional_text(profile_data.get('exp2_employment_type')),
        'exp3_employment_type': _clean_optional_text(profile_data.get('exp3_employment_type')),
        'school': _clean_optional_text(profile_data.get('school') or profile_data.get('education')),
        'school2': _clean_optional_text(profile_data.get('school2')),
        'school3': _clean_optional_text(profile_data.get('school3')),
        'degree2': _clean_optional_text(profile_data.get('degree2')),
        'degree3': _clean_optional_text(profile_data.get('degree3')),
        'major2': _clean_optional_text(profile_data.get('major2')),
        'major3': _clean_optional_text(profile_data.get('major3')),
        'standardized_degree': _clean_optional_text(profile_data.get('standardized_degree')),
        'standardized_degree2': _clean_optional_text(profile_data.get('standardized_degree2')),
        'standardized_degree3': _clean_optional_text(profile_data.get('standardized_degree3')),
        'standardized_major': _clean_optional_text(profile_data.get('standardized_major')),
        'standardized_major_alt': _clean_optional_text(profile_data.get('standardized_major_alt')),
        'standardized_major2': _clean_optional_text(profile_data.get('standardized_major2')),
        'standardized_major3': _clean_optional_text(profile_data.get('standardized_major3')),
        'scraped_at': _clean_optional_text(profile_data.get('scraped_at')),
        'normalized_job_title': _clean_optional_text(profile_data.get('normalized_job_title')),
        'normalized_company': _clean_optional_text(profile_data.get('normalized_company')),
        'job_1_relevance_score': _parse_float(profile_data.get('job_1_relevance_score')),
        'job_2_relevance_score': _parse_float(profile_data.get('job_2_relevance_score')),
        'job_3_relevance_score': _parse_float(profile_data.get('job_3_relevance_score')),
        'job_1_is_relevant': _parse_bool(profile_data.get('job_1_is_relevant')),
        'job_2_is_relevant': _parse_bool(profile_data.get('job_2_is_relevant')),
        'job_3_is_relevant': _parse_bool(profile_data.get('job_3_is_relevant')),
        'relevant_experience_months': _parse_int(profile_data.get('relevant_experience_months')),
        'seniority_level': _clean_optional_text(profile_data.get('seniority_level')),
    }

    major, discipline, _ = _sanitize_major_and_discipline(
        major=payload.get('major'),
        standardized_major=payload.get('standardized_major'),
        discipline=payload.get('discipline'),
    )
    payload['major'] = major
    payload['discipline'] = discipline

    if _parse_bool(profile_data.get("skip_offline_sync_queue")) or _parse_bool(
        profile_data.get("skip_cloud_sync_queue")
    ):
        payload["skip_offline_sync_queue"] = True

    return payload


def _should_queue_alumni_for_pending_cloud_sync(payload: dict) -> bool:
    """If False, local SQLite is updated but the row is not added to _pending_sync."""
    if not payload:
        return True
    if payload.get("skip_offline_sync_queue"):
        return False
    url = (payload.get("linkedin_url") or "").lower()
    # Synthetic slugs from raw-field / immutability lab tests
    for marker in ("raw-immutability-test", "raw-overwrite-test"):
        if marker in url:
            return False
    extra = os.getenv("ALUMNI_SKIP_OFFLINE_SYNC_QUEUE", "").strip()
    for part in extra.split(","):
        p = part.strip().lower()
        if p and p in url:
            return False
    return True


def _upsert_alumni_payload(cur, payload):
    """Execute alumni upsert for a normalized payload using an active cursor."""
    norm_title_id = _get_or_create_normalized_entity(
        cur,
        'normalized_job_titles',
        'normalized_title',
        payload.get('normalized_job_title'),
    )
    norm_company_id = _get_or_create_normalized_entity(
        cur,
        'normalized_companies',
        'normalized_company',
        payload.get('normalized_company'),
    )

    cur.execute(
        """
        INSERT INTO alumni
        (first_name, last_name, grad_year, degree, major, discipline, linkedin_url, current_job_title, company, location, latitude, longitude, headline,
         school_start_date, job_start_date, job_end_date, working_while_studying, working_while_studying_status, scrape_run_id,
         exp2_title, exp2_company, exp2_dates, exp3_title, exp3_company, exp3_dates,
         job_employment_type, exp2_employment_type, exp3_employment_type,
         school, school2, school3, degree2, degree3, major2, major3,
         standardized_degree, standardized_degree2, standardized_degree3,
         standardized_major, standardized_major_alt, standardized_major2, standardized_major3,
         scraped_at, last_updated, normalized_job_title_id, normalized_company_id,
         job_1_relevance_score, job_2_relevance_score, job_3_relevance_score,
         job_1_is_relevant, job_2_is_relevant, job_3_is_relevant,
         relevant_experience_months, seniority_level)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name=VALUES(first_name),
            last_name=VALUES(last_name),
            grad_year=VALUES(grad_year),
            degree=VALUES(degree),
            major=VALUES(major),
            discipline=COALESCE(NULLIF(VALUES(discipline), ''), discipline),
            current_job_title=VALUES(current_job_title),
            company=VALUES(company),
            location=VALUES(location),
            latitude=COALESCE(VALUES(latitude), latitude),
            longitude=COALESCE(VALUES(longitude), longitude),
            headline=VALUES(headline),
            school_start_date=VALUES(school_start_date),
            job_start_date=VALUES(job_start_date),
            job_end_date=VALUES(job_end_date),
            working_while_studying=VALUES(working_while_studying),
            working_while_studying_status=VALUES(working_while_studying_status),
            scrape_run_id=COALESCE(VALUES(scrape_run_id), scrape_run_id),
            exp2_title=VALUES(exp2_title),
            exp2_company=VALUES(exp2_company),
            exp2_dates=VALUES(exp2_dates),
            exp3_title=VALUES(exp3_title),
            exp3_company=VALUES(exp3_company),
            exp3_dates=VALUES(exp3_dates),
            job_employment_type=VALUES(job_employment_type),
            exp2_employment_type=VALUES(exp2_employment_type),
            exp3_employment_type=VALUES(exp3_employment_type),
            school=VALUES(school),
            school2=VALUES(school2),
            school3=VALUES(school3),
            degree2=VALUES(degree2),
            degree3=VALUES(degree3),
            major2=VALUES(major2),
            major3=VALUES(major3),
            standardized_degree=VALUES(standardized_degree),
            standardized_degree2=VALUES(standardized_degree2),
            standardized_degree3=VALUES(standardized_degree3),
            standardized_major=VALUES(standardized_major),
            standardized_major_alt=VALUES(standardized_major_alt),
            standardized_major2=VALUES(standardized_major2),
            standardized_major3=VALUES(standardized_major3),
            last_updated=VALUES(last_updated),
            normalized_job_title_id=VALUES(normalized_job_title_id),
            normalized_company_id=VALUES(normalized_company_id),
            job_1_relevance_score=VALUES(job_1_relevance_score),
            job_2_relevance_score=VALUES(job_2_relevance_score),
            job_3_relevance_score=VALUES(job_3_relevance_score),
            job_1_is_relevant=VALUES(job_1_is_relevant),
            job_2_is_relevant=VALUES(job_2_is_relevant),
            job_3_is_relevant=VALUES(job_3_is_relevant),
            relevant_experience_months=VALUES(relevant_experience_months),
            seniority_level=VALUES(seniority_level)
        """,
        (
            payload.get('first_name'),
            payload.get('last_name'),
            payload.get('grad_year'),
            payload.get('degree'),
            payload.get('major'),
            payload.get('discipline'),
            payload.get('linkedin_url'),
            payload.get('current_job_title'),
            payload.get('company'),
            payload.get('location'),
            payload.get('latitude'),
            payload.get('longitude'),
            payload.get('headline'),
            payload.get('school_start_date'),
            payload.get('job_start_date'),
            payload.get('job_end_date'),
            payload.get('working_while_studying'),
            payload.get('working_while_studying_status'),
            payload.get('scrape_run_id'),
            payload.get('exp2_title'),
            payload.get('exp2_company'),
            payload.get('exp2_dates'),
            payload.get('exp3_title'),
            payload.get('exp3_company'),
            payload.get('exp3_dates'),
            payload.get('job_employment_type'),
            payload.get('exp2_employment_type'),
            payload.get('exp3_employment_type'),
            payload.get('school'),
            payload.get('school2'),
            payload.get('school3'),
            payload.get('degree2'),
            payload.get('degree3'),
            payload.get('major2'),
            payload.get('major3'),
            payload.get('standardized_degree'),
            payload.get('standardized_degree2'),
            payload.get('standardized_degree3'),
            payload.get('standardized_major'),
            payload.get('standardized_major_alt'),
            payload.get('standardized_major2'),
            payload.get('standardized_major3'),
            payload.get('scraped_at'),
            payload.get('scraped_at'),
            norm_title_id,
            norm_company_id,
            payload.get('job_1_relevance_score'),
            payload.get('job_2_relevance_score'),
            payload.get('job_3_relevance_score'),
            payload.get('job_1_is_relevant'),
            payload.get('job_2_is_relevant'),
            payload.get('job_3_is_relevant'),
            payload.get('relevant_experience_months'),
            payload.get('seniority_level'),
        ),
    )


def upsert_scraped_profile(profile_data, allow_cloud=True, run_id=None):
    """
    Persist one scraped profile immediately.

        Behavior:
        - Attempts cloud write when allowed and DISABLE_DB != 1.
    - Also mirrors to local SQLite when fallback module is available, so local backup
      stays current even when cloud is reachable.
    """
    payload = _build_alumni_upsert_payload(profile_data)
    if not payload:
        return False

    if run_id is not None and payload.get("scrape_run_id") is None:
        payload["scrape_run_id"] = _parse_int(run_id)

    status = {
        "cloud_attempted": False,
        "cloud_written": False,
        "sqlite_written": False,
        "cloud_routed_to_sqlite": False,
        "cloud_queued": False,
        "cloud_mode": "not_attempted",
        "cloud_reason": "",
    }

    disable_db = os.getenv("DISABLE_DB", "0") == "1"
    should_attempt_cloud = allow_cloud and not disable_db
    if disable_db:
        status["cloud_reason"] = "DISABLE_DB=1"
    elif not allow_cloud:
        status["cloud_reason"] = "cloud_disabled_for_run"

    wrote_primary = False
    conn = None
    if should_attempt_cloud:
        status["cloud_attempted"] = True
        try:
            conn = get_connection()
            is_sqlite_routed = conn.__class__.__name__ == "SQLiteConnectionWrapper"
            with conn.cursor() as cur:
                _upsert_alumni_payload(cur, payload)
            conn.commit()
            wrote_primary = True
            if is_sqlite_routed:
                status["sqlite_written"] = True
                status["cloud_routed_to_sqlite"] = True
                status["cloud_mode"] = "routed_to_sqlite"
                status["cloud_reason"] = "cloud_unreachable"
                # When cloud is unreachable and writes are routed to SQLite fallback,
                # queue this upsert for later cloud synchronization.
                try:
                    manager = getattr(conn, "_manager", None)
                    if manager and _should_queue_alumni_for_pending_cloud_sync(payload):
                        manager.record_pending_change(
                            table_name="alumni",
                            primary_key={"linkedin_url": payload.get("linkedin_url")},
                            operation="INSERT",
                            old_data=None,
                            new_data=payload,
                        )
                        status["cloud_queued"] = True
                except Exception as queue_err:
                    logger.debug(
                        f"Pending cloud-sync queueing skipped for {payload.get('linkedin_url')}: {queue_err}"
                    )
            else:
                status["cloud_written"] = True
                status["cloud_mode"] = "cloud_written"
                status["cloud_reason"] = ""
        except Exception as err:
            status["cloud_mode"] = "cloud_failed"
            status["cloud_reason"] = str(err)
            logger.warning(f"Primary DB upsert failed for {payload.get('linkedin_url')}: {err}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # Mirror to local SQLite when possible so offline backup is always fresh.
    wrote_sqlite_mirror = False
    try:
        from sqlite_fallback import get_connection_manager, SQLiteConnectionWrapper

        manager = get_connection_manager()
        sqlite_conn = SQLiteConnectionWrapper(manager.get_sqlite_connection(), manager)
        try:
            with sqlite_conn.cursor() as cur:
                _upsert_alumni_payload(cur, payload)
            sqlite_conn.commit()
            wrote_sqlite_mirror = True
            status["sqlite_written"] = True
        finally:
            try:
                sqlite_conn.close()
            except Exception:
                pass

        # Queue for cloud sync if the primary write didn't go to cloud
        # (e.g., cloud disabled after 5 consecutive failures, or DISABLE_DB=1)
        if (
            not status.get("cloud_written")
            and not status.get("cloud_queued")
            and _should_queue_alumni_for_pending_cloud_sync(payload)
        ):
            try:
                manager.record_pending_change(
                    table_name="alumni",
                    primary_key={"linkedin_url": payload.get("linkedin_url")},
                    operation="INSERT",
                    old_data=None,
                    new_data=payload,
                    force=True,  # Queue even when not officially offline
                )
                status["cloud_queued"] = True
            except Exception as queue_err:
                logger.debug(
                    f"Pending cloud-sync queueing skipped for {payload.get('linkedin_url')}: {queue_err}"
                )
    except Exception as err:
        logger.debug(f"SQLite mirror upsert skipped for {payload.get('linkedin_url')}: {err}")

    if wrote_primary or wrote_sqlite_mirror:
        return status
    return status


# ============================================================
# EXISTING FUNCTIONS
# ============================================================

