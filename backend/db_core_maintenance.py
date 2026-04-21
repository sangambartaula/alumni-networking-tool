try:
    from .db_core_common import *
    from .db_core_schema import ensure_all_alumni_schema_migrations
except ImportError:
    from db_core_common import *
    from db_core_schema import ensure_all_alumni_schema_migrations

def seed_alumni_data():
    """Import alumni data from CSV file"""
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(backend_dir)
    csv_path = os.path.join(project_root, 'scraper', 'output', 'UNT_Alumni_Data.csv')

    if not os.path.exists(csv_path):
        logger.warning(f"CSV file not found at {csv_path}, skipping import")
        return

    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Importing alumni data from {csv_path}")
        logger.info(f"≡ƒôè Found {len(df)} records to import")

        conn = get_connection()
        added = 0
        updated = 0
        processed = 0
        flagged_major_issue_urls = {}
        try:
            commit_every = max(1, int(os.getenv("SEED_COMMIT_EVERY", "50")))
        except Exception:
            commit_every = 50

        try:
            with conn.cursor() as cur:
                for index, row in df.iterrows():
                    processed += 1

                    # Parse name (Handle New 'first', 'last' OR Old 'name')
                    first_name = str(row.get('first', '')).strip() if pd.notna(row.get('first')) else ''
                    last_name = str(row.get('last', '')).strip() if pd.notna(row.get('last')) else ''
                    first_name = _normalize_person_name(first_name)
                    last_name = _normalize_person_name(last_name)
                    
                    if not first_name and not last_name:
                        # Fallback to old 'name' column
                        name = str(row.get('name', '')).strip() if pd.notna(row.get('name')) else ''
                        name = _normalize_person_name(name)
                        if name:
                            parts = name.split()
                            first_name = parts[0]
                            last_name = ' '.join(parts[1:]) if len(parts) > 1 else ''

                    if not first_name and not last_name:
                        continue

                    # Extract fields with New/Old keys
                    headline = str(row.get('headline', '')).strip() if pd.notna(row.get('headline')) else None
                    location = str(row.get('location', '')).strip() if pd.notna(row.get('location')) else None
                    
                    # title (new) vs job_title (old)
                    job_title = str(row.get('title', '')).strip() if pd.notna(row.get('title')) else \
                                str(row.get('job_title', '')).strip() if pd.notna(row.get('job_title')) else None

                    company = str(row.get('company', '')).strip() if pd.notna(row.get('company')) else None
                    major = str(row.get('major', '')).strip() if pd.notna(row.get('major')) else None
                    degree = str(row.get('degree', '')).strip() if pd.notna(row.get('degree')) else None

                    # school (new) vs education (old)
                    school = str(row.get('school', '')).strip() if pd.notna(row.get('school')) else \
                             str(row.get('education', '')).strip() if pd.notna(row.get('education')) else None

                    # Education entries 2 and 3
                    school2 = str(row.get('school2', '')).strip() if pd.notna(row.get('school2')) else None
                    school3 = str(row.get('school3', '')).strip() if pd.notna(row.get('school3')) else None
                    degree2 = str(row.get('degree2', '')).strip() if pd.notna(row.get('degree2')) else None
                    degree3 = str(row.get('degree3', '')).strip() if pd.notna(row.get('degree3')) else None
                    major2 = str(row.get('major2', '')).strip() if pd.notna(row.get('major2')) else None
                    major3 = str(row.get('major3', '')).strip() if pd.notna(row.get('major3')) else None

                    # Guard DB upload against oversized education fields.
                    school = _truncate_optional_text(school, 255)
                    school2 = _truncate_optional_text(school2, 255)
                    school3 = _truncate_optional_text(school3, 255)
                    degree = _truncate_optional_text(degree, 255)
                    degree2 = _truncate_optional_text(degree2, 255)
                    degree3 = _truncate_optional_text(degree3, 255)
                    major = _truncate_optional_text(major, 255)
                    major2 = _truncate_optional_text(major2, 255)
                    major3 = _truncate_optional_text(major3, 255)

                    # Preserve discipline from CSV and normalize separately from major.
                    saved_discipline = str(row.get('discipline', '')).strip() if pd.notna(row.get('discipline')) else ''
                    
                    # grad_year (new) vs graduation_year (old)
                    raw_grad_year = _coerce_grad_year(row.get('grad_year'))
                    if raw_grad_year is None:
                        raw_grad_year = _coerce_grad_year(row.get('graduation_year'))

                    # linkedin_url (new) vs profile_url (old)
                    raw_url = row.get('linkedin_url') if pd.notna(row.get('linkedin_url')) else row.get('profile_url')
                    profile_url = normalize_url(raw_url)
                    
                    scraped_at = str(row.get('scraped_at', '')).strip() if pd.notna(row.get('scraped_at')) else None

                    # New fields (may not exist in older CSVs)
                    # school_start (new) vs school_start_date (old)
                    raw_school_start_date = str(row.get('school_start', '')).strip() if pd.notna(row.get('school_start')) else \
                        str(row.get('school_start_date', '')).strip() if pd.notna(row.get('school_start_date')) else None
                    grad_year, school_start_date = _normalize_primary_education_dates(
                        raw_grad_year,
                        raw_school_start_date,
                    )
                    inferred_grad_from_school_start = (
                        raw_grad_year is None
                        and grad_year is not None
                        and bool(raw_school_start_date)
                        and school_start_date is None
                    )

                    # job_start (new) vs job_start_date (old)
                    job_start_date = str(row.get('job_start', '')).strip() if pd.notna(row.get('job_start')) else \
                                     str(row.get('job_start_date', '')).strip() if pd.notna(row.get('job_start_date')) else None

                    # job_end (new) vs job_end_date (old)
                    job_end_date = str(row.get('job_end', '')).strip() if pd.notna(row.get('job_end')) else \
                                   str(row.get('job_end_date', '')).strip() if pd.notna(row.get('job_end_date')) else None

                    wws_raw = row.get('working_while_studying', None)
                    working_while_studying = None
                    working_while_studying_status = None
                    if pd.notna(wws_raw):
                        if isinstance(wws_raw, str):
                            v = wws_raw.strip().lower()
                            if v in ("yes", "currently", "true", "1"):
                                # "currently" = actively working while still studying ΓåÆ treat as True
                                working_while_studying = True
                                working_while_studying_status = v if v in ("yes", "no", "currently") else "yes"
                            elif v in ("no", "false", "0"):
                                working_while_studying = False
                                working_while_studying_status = "no"
                        elif isinstance(wws_raw, (int, float, bool)):
                            working_while_studying = bool(wws_raw)
                            working_while_studying_status = "yes" if working_while_studying else "no"

                    # Experience 2 and 3 fields (New: exp_2_title vs Old: exp2_title)
                    exp2_title = str(row.get('exp_2_title', '')).strip() if pd.notna(row.get('exp_2_title')) else \
                                 str(row.get('exp2_title', '')).strip() if pd.notna(row.get('exp2_title'),) else None
                    
                    exp2_company = str(row.get('exp_2_company', '')).strip() if pd.notna(row.get('exp_2_company')) else \
                                   str(row.get('exp2_company', '')).strip() if pd.notna(row.get('exp2_company')) else None
                    
                    exp2_dates = str(row.get('exp_2_dates', '')).strip() if pd.notna(row.get('exp_2_dates')) else \
                                 str(row.get('exp2_dates', '')).strip() if pd.notna(row.get('exp2_dates')) else None
                    
                    exp3_title = str(row.get('exp_3_title', '')).strip() if pd.notna(row.get('exp_3_title')) else \
                                 str(row.get('exp3_title', '')).strip() if pd.notna(row.get('exp3_title')) else None
                    
                    exp3_company = str(row.get('exp_3_company', '')).strip() if pd.notna(row.get('exp_3_company')) else \
                                   str(row.get('exp3_company', '')).strip() if pd.notna(row.get('exp3_company')) else None
                    
                    exp3_dates = str(row.get('exp_3_dates', '')).strip() if pd.notna(row.get('exp_3_dates')) else \
                                 str(row.get('exp3_dates', '')).strip() if pd.notna(row.get('exp3_dates')) else None

                    job_employment_type = _csv_optional_str(row, 'job_employment_type')
                    exp2_employment_type = _csv_optional_str(row, 'exp_2_employment_type', 'exp2_employment_type')
                    exp3_employment_type = _csv_optional_str(row, 'exp_3_employment_type', 'exp3_employment_type')

                    if inferred_grad_from_school_start:
                        try:
                            from working_while_studying_status import (
                                recompute_working_while_studying_status,
                                status_to_bool,
                            )
                            recomputed_status = (recompute_working_while_studying_status({
                                "grad_year": grad_year,
                                "school_start_date": school_start_date,
                                "school": school,
                                "school2": school2,
                                "school3": school3,
                                "current_job_title": job_title,
                                "company": company,
                                "job_start_date": job_start_date,
                                "job_end_date": job_end_date,
                                "exp2_title": exp2_title,
                                "exp2_company": exp2_company,
                                "exp2_dates": exp2_dates,
                                "exp3_title": exp3_title,
                                "exp3_company": exp3_company,
                                "exp3_dates": exp3_dates,
                            }) or "").strip().lower()
                            if recomputed_status in {"yes", "no", "currently"}:
                                working_while_studying_status = recomputed_status
                                working_while_studying = status_to_bool(recomputed_status)
                        except Exception as recompute_err:
                            logger.warning(
                                "Could not recompute working_while_studying for %s: %s",
                                profile_url or f"{first_name} {last_name}".strip(),
                                recompute_err,
                            )

                    # Read standardized values from CSV (now handled purely by scraper)
                    std_degree = str(row.get('standardized_degree', '')).strip() if pd.notna(row.get('standardized_degree')) else None
                    std_degree2 = str(row.get('standardized_degree2', '')).strip() if pd.notna(row.get('standardized_degree2')) else None
                    std_degree3 = str(row.get('standardized_degree3', '')).strip() if pd.notna(row.get('standardized_degree3')) else None
                    
                    std_major = str(row.get('standardized_major', '')).strip() if pd.notna(row.get('standardized_major')) else None
                    std_major_alt = str(row.get('standardized_major_alt', '')).strip() if pd.notna(row.get('standardized_major_alt')) else None
                    std_major2 = str(row.get('standardized_major2', '')).strip() if pd.notna(row.get('standardized_major2')) else None
                    std_major3 = str(row.get('standardized_major3', '')).strip() if pd.notna(row.get('standardized_major3')) else None

                    major, saved_discipline, major_review_reason = _sanitize_major_and_discipline(
                        major=major,
                        standardized_major=std_major,
                        discipline=saved_discipline,
                    )
                    if major_review_reason and profile_url:
                        flagged_major_issue_urls[profile_url] = major_review_reason

                    # Insert or update into database
                    try:
                        # Get normalized job title and company IDs directly using SQL helper
                        norm_title = str(row.get('normalized_job_title', '')).strip() if pd.notna(row.get('normalized_job_title')) else None
                        norm_title_id = _get_or_create_normalized_entity(cur, 'normalized_job_titles', 'normalized_title', norm_title)

                        norm_comp = str(row.get('normalized_company', '')).strip() if pd.notna(row.get('normalized_company')) else None
                        norm_company_id = _get_or_create_normalized_entity(cur, 'normalized_companies', 'normalized_company', norm_comp)

                        cur.execute("""
                            INSERT INTO alumni 
                            (first_name, last_name, grad_year, degree, major, discipline, linkedin_url, current_job_title, company, location, headline, 
                             school_start_date, job_start_date, job_end_date, working_while_studying, working_while_studying_status,
                             exp2_title, exp2_company, exp2_dates, exp3_title, exp3_company, exp3_dates,
                             job_employment_type, exp2_employment_type, exp3_employment_type,
                             school, school2, school3, degree2, degree3, major2, major3,
                             standardized_degree, standardized_degree2, standardized_degree3,
                             standardized_major, standardized_major_alt, standardized_major2, standardized_major3,
                             scraped_at, last_updated, normalized_job_title_id, normalized_company_id,
                             job_1_relevance_score, job_2_relevance_score, job_3_relevance_score,
                             job_1_is_relevant, job_2_is_relevant, job_3_is_relevant,
                             relevant_experience_months, seniority_level)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
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
                                headline=VALUES(headline),
                                school_start_date=VALUES(school_start_date),
                                job_start_date=VALUES(job_start_date),
                                job_end_date=VALUES(job_end_date),
                                working_while_studying=VALUES(working_while_studying),
                                working_while_studying_status=VALUES(working_while_studying_status),
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
                        """, (
                            first_name,
                            last_name,
                            grad_year,
                            degree,
                            major,
                            saved_discipline,
                            profile_url,
                            job_title,
                            company,
                            location,
                            headline,
                            school_start_date,
                            job_start_date,
                            job_end_date,
                            working_while_studying,
                            working_while_studying_status,
                            exp2_title,
                            exp2_company,
                            exp2_dates,
                            exp3_title,
                            exp3_company,
                            exp3_dates,
                            job_employment_type,
                            exp2_employment_type,
                            exp3_employment_type,
                            school,
                            school2,
                            school3,
                            degree2,
                            degree3,
                            major2,
                            major3,
                            std_degree,
                            std_degree2,
                            std_degree3,
                            std_major,
                            std_major_alt,
                            std_major2,
                            std_major3,
                            scraped_at,
                            scraped_at,
                            norm_title_id,
                            norm_company_id,
                            _parse_float(row.get('job_1_relevance_score')),
                            _parse_float(row.get('job_2_relevance_score')),
                            _parse_float(row.get('job_3_relevance_score')),
                            _parse_bool(row.get('job_1_is_relevant')),
                            _parse_bool(row.get('job_2_is_relevant')),
                            _parse_bool(row.get('job_3_is_relevant')),
                            _parse_int(row.get('relevant_experience_months')),
                            _clean_optional_text(row.get('seniority_level')),
                        ))

                        if cur.rowcount == 1:
                            added += 1
                        elif cur.rowcount == 2:
                            updated += 1
                    except Exception as err:
                        logger.warning(f"Skipping record for {first_name} {last_name}: {err}")
                        if "Lost connection" in str(err) or "MySQL Connection not available" in str(err):
                            logger.error("≡ƒ¢æ MySQL connection lost. Exiting loop to save progress.")
                            break
                        continue

                    # Incremental commit protects progress if a long import is interrupted.
                    if processed % commit_every == 0:
                        try:
                            conn.commit()
                            logger.debug(f"Auto-committed batch at record {processed}")
                        except Exception as commit_err:
                            logger.error(f"Γ¥î Auto-commit failed: {commit_err}")

                conn.commit()
                logger.info(f"Added {added} new alumni records")
                logger.info(f"Updated {updated} existing alumni records")
                logger.info(f"Successfully processed {processed} total alumni records")
                _append_flagged_review_urls(flagged_major_issue_urls)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Γ¥î Critical setup failed: {e}")
        raise


def has_alumni_records():
    """Return True when the alumni table already has at least one record."""
    try:
        with managed_db_cursor(get_connection) as (_conn, cur):
            cur.execute("SELECT COUNT(*) FROM alumni")
            row = cur.fetchone()
            if isinstance(row, dict):
                count = next(iter(row.values()), 0)
            elif isinstance(row, (tuple, list)):
                count = row[0]
            else:
                count = int(row or 0)
            return int(count) > 0
    except Exception as e:
        logger.warning(f"Could not determine alumni table size: {e}")
        return False


def truncate_dot_fields():
    """Remove anything after the corrupted marker in location, company, and current_job_title."""
    try:
        with managed_db_cursor(get_connection, commit=True) as (_conn, cur):
            cur.execute("""
                UPDATE alumni
                SET 
                    location = TRIM(SUBSTRING_INDEX(location, '┬╖', 1)),
                    company = TRIM(SUBSTRING_INDEX(company, '┬╖', 1)),
                    current_job_title = TRIM(SUBSTRING_INDEX(current_job_title, '┬╖', 1))
                WHERE 
                    location LIKE '%┬╖%' 
                    OR company LIKE '%┬╖%'
                    OR current_job_title LIKE '%┬╖%';
            """)
            logger.info("Truncated fields containing the corrupted marker in alumni table")
    except mysql.connector.Error as err:
        logger.error(f"Γ¥î Error truncating dot fields: {err}")
        raise


def cleanup_trailing_slashes():
    """Remove trailing slashes from existing URLs, handling duplicates."""
    logger.info("≡ƒº╣ Cleaning up trailing slashes from URLs...")
    try:
        tables = ['visited_profiles', 'alumni']
        with managed_db_cursor(get_connection) as (conn, cur):
            for table in tables:
                # Find URLs with trailing slash
                cur.execute(f"SELECT id, linkedin_url FROM {table} WHERE linkedin_url LIKE '%/'")
                rows = cur.fetchall()
                if not rows:
                    continue
                
                logger.info(f"Found {len(rows)} URLs with trailing slash in {table}")
                fixed = 0
                deleted = 0
                
                for row_id, url in rows:
                    clean_url = url.rstrip('/')
                    try:
                        # Try to update
                        cur.execute(f"UPDATE {table} SET linkedin_url = %s WHERE id = %s", (clean_url, row_id))
                        fixed += 1
                    except Exception as err:
                        err_str = str(err)
                        # MySQL errno 1062 = Duplicate entry; SQLite raises IntegrityError
                        is_duplicate = (
                            (hasattr(err, 'errno') and err.errno == 1062) or
                            'UNIQUE constraint' in err_str
                        )
                        if is_duplicate:
                            # Collision! The clean URL matches another record.
                            # We delete the current record with the slash, keeping the other one.
                            logger.info(f"  Collision for {clean_url}. Deleting duplicate record ID {row_id}.")
                            cur.execute(f"DELETE FROM {table} WHERE id = %s", (row_id,))
                            deleted += 1
                        else:
                            logger.error(f"Γ¥î Failed to fix {url}: {err}")
                
                conn.commit()
                logger.info(f"Γ£¿ Fixed {fixed} URLs, Deleted {deleted} duplicates in {table}")
                
    except Exception as e:
        logger.error(f"Γ¥î Error during cleanup: {e}")


def normalize_existing_grad_years():
    """
    Retroactively normalize alumni.grad_year to integer values where possible.
    Leaves unparseable values unchanged.
    """
    normalized = 0
    scanned = 0

    try:
        with managed_db_cursor(get_connection, dictionary=True, commit=True) as (_conn, cur):
            cur.execute("SELECT id, grad_year FROM alumni WHERE grad_year IS NOT NULL")
            rows = cur.fetchall() or []
            scanned = len(rows)

            for row in rows:
                row_id = row.get("id")
                raw_year = row.get("grad_year")
                parsed_year = _coerce_grad_year(raw_year)

                if parsed_year is None:
                    continue

                if isinstance(raw_year, int) and not isinstance(raw_year, bool) and raw_year == parsed_year:
                    continue

                cur.execute(
                    "UPDATE alumni SET grad_year = %s WHERE id = %s",
                    (parsed_year, row_id)
                )
                normalized += 1

        logger.info(f"Grad year normalization complete: updated {normalized} of {scanned} rows")
    except Exception as e:
        logger.error(f"Γ¥î Error normalizing grad years: {e}")


def normalize_single_date_education_semantics():
    """
    Retroactively apply primary education single-date semantics:
    if grad_year is missing and school_start_date has a single date/year, move it
    into grad_year and clear school_start_date.

    Recomputes working_while_studying_status for updated rows so filter behavior
    stays aligned with the corrected education dates.
    """
    scanned = 0
    updated = 0

    try:
        from working_while_studying_status import (
            recompute_working_while_studying_status,
            status_to_bool,
        )
    except Exception as import_err:
        logger.error(f"Γ¥î Could not import working_while_studying_status helpers: {import_err}")
        return

    try:
        with managed_db_cursor(get_connection, dictionary=True, commit=True) as (_conn, cur):
            cur.execute(
                """
                SELECT id,
                       grad_year,
                       school_start_date,
                       school,
                       school2,
                       school3,
                       current_job_title,
                       company,
                       job_start_date,
                       job_end_date,
                       exp2_title,
                       exp2_company,
                       exp2_dates,
                       exp3_title,
                       exp3_company,
                       exp3_dates
                FROM alumni
                WHERE grad_year IS NULL
                  AND school_start_date IS NOT NULL
                  AND school_start_date <> ''
                """
            )
            rows = cur.fetchall() or []
            scanned = len(rows)

            for row in rows:
                inferred_grad_year = _infer_grad_year_from_school_start_date(row.get("school_start_date"))
                if inferred_grad_year is None:
                    continue

                row_for_status = dict(row)
                row_for_status["grad_year"] = inferred_grad_year
                row_for_status["school_start_date"] = None
                recomputed_status = (recompute_working_while_studying_status(row_for_status) or "").strip().lower()
                if recomputed_status not in {"yes", "no", "currently"}:
                    recomputed_status = ""

                cur.execute(
                    """
                    UPDATE alumni
                    SET grad_year = %s,
                        school_start_date = NULL,
                        working_while_studying = %s,
                        working_while_studying_status = %s
                    WHERE id = %s
                    """,
                    (
                        inferred_grad_year,
                        status_to_bool(recomputed_status),
                        recomputed_status or None,
                        row.get("id"),
                    ),
                )
                updated += 1

        logger.info(
            "Single-date education normalization complete: updated %s of %s candidate rows",
            updated,
            scanned,
        )
    except Exception as e:
        logger.error(f"Γ¥î Error normalizing single-date education semantics: {e}")


if __name__ == "__main__":
    try:
        # Validate environment variables
        required_vars = ['MYSQLHOST', 'MYSQLUSER', 'MYSQLPASSWORD', 'MYSQL_DATABASE']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        logger.info("All required environment variables validated")
        logger.info("≡ƒÜÇ Starting database initialization...")
        logger.info(f"≡ƒôª Database '{MYSQL_DATABASE}' ensured")

        # Initialize tables and apply alumni column migrations (same as Flask startup)
        init_db()
        ensure_all_alumni_schema_migrations()

        run_seed_mode = os.getenv("DB_RUN_SEED", "1").strip().lower()
        run_seed = run_seed_mode in {"1", "true", "yes", "sync", "force"}
        run_visited_migration = os.getenv("DB_RUN_VISITED_MIGRATION", "0") == "1"
        run_maintenance = os.getenv("DB_RUN_MAINTENANCE", "0") == "1"

        if run_seed:
            seed_alumni_data()
        else:
            logger.info("ΓÅ¡∩╕Å  Skipping alumni seed (set DB_RUN_SEED=1|sync to run)")

        if run_maintenance:
            normalize_existing_grad_years()
            truncate_dot_fields()
            cleanup_trailing_slashes()
            sync_alumni_to_visited_profiles()
        else:
            logger.info("ΓÅ¡∩╕Å  Skipping maintenance pass (set DB_RUN_MAINTENANCE=1 to run)")

        if run_visited_migration:
            logger.info("\n" + "=" * 60)
            logger.info("≡ƒôª MIGRATING VISITED HISTORY TO DATABASE")
            logger.info("=" * 60)
            migrate_visited_history_csv_to_db()
        else:
            logger.info("ΓÅ¡∩╕Å  Skipping visited_history migration (set DB_RUN_VISITED_MIGRATION=1 to run)")

        # Show stats
        stats = get_visited_profiles_stats()
        if stats:
            logger.info(f"\n≡ƒôè Visited Profiles Stats:")
            logger.info(f"   Total visited: {stats['total']}")
            logger.info(f"   UNT Alumni: {stats['unt_alumni']}")
            logger.info(f"   Non-UNT: {stats['non_unt']}")
            logger.info(f"   Needs update: {stats['needs_update']}")

        # Test connection
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT NOW()")
            db_time = cur.fetchone()[0]
            logger.info(f"\nDatabase connection successful. DB time: {db_time}")

            cur.execute("SELECT COUNT(*) FROM alumni")
            count = cur.fetchone()[0]
            logger.info(f"Alumni in database: {count} records")

            cur.execute("SELECT COUNT(*) FROM visited_profiles")
            visited_count = cur.fetchone()[0]
            logger.info(f"Visited profiles in database: {visited_count} records")

            if count > 0:
                cur.execute("""
                    SELECT id, first_name, last_name, current_job_title, headline, grad_year,
                           school_start_date, job_start_date, job_end_date, working_while_studying
                    FROM alumni
                    LIMIT 10
                """)
                for row in cur.fetchall():
                    (
                        alumni_id, fname, lname, job, head, grad,
                        school_start, job_start, job_end, wws
                    ) = row
                    display_job = job or head or 'None'
                    logger.info(
                        f"  - {fname} {lname} ({display_job}) - Grad: {grad} | "
                        f"SchoolStart: {school_start} | Job: {job_start}-{job_end} | WorkingWhileStudying: {wws}"
                    )

        conn.close()

        logger.info("=" * 60)
        logger.info("Γ£ô ALUMNI NETWORKING TOOL - BACKEND")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Γ¥î Database initialization failed: {e}")
        exit(1)
