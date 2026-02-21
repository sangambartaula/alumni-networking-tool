"""
Work While Studying Analysis Module

Determines if a user was working while studying based on graduation date 
and job experience records.

Tables used:
- education(user_id, graduation_year INT NULL, graduation_month INT NULL, 
            graduation_date DATE NULL, is_expected BOOLEAN)
- experience(user_id, company TEXT, title TEXT, start_date DATE NULL, 
             end_date DATE NULL, is_current BOOLEAN)
"""

from datetime import datetime, date
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


def _get_graduation_date(
    graduation_date: Optional[date], 
    graduation_year: Optional[int],
    graduation_month: Optional[int]
) -> Optional[date]:
    """
    Determine the graduation date using the priority rules.
    
    Args:
        graduation_date: Direct DATE field if available
        graduation_year: Year of graduation
        graduation_month: Month of graduation (1-12)
    
    Returns:
        Calculated/used graduation date, or None if unknown
    """
    # Rule 1: If graduation_date exists, use it
    if graduation_date is not None:
        return graduation_date
    
    # Rule 2: If only graduation_year exists, use May 15 as fallback
    if graduation_year is not None:
        try:
            # Use May 15 (mid-May) as the assumed graduation date for year-only data
            return date(graduation_year, 5, 15)
        except (ValueError, TypeError):
            logger.warning(f"Invalid graduation_year: {graduation_year}")
            return None
    
    # No graduation info available
    return None


def _get_graduated_status(
    graduation_year: Optional[int],
    graduation_date: Optional[date],
    is_expected: Optional[bool],
    current_year: int = None
) -> str:
    """
    Determine if user has graduated or is still studying.
    
    Args:
        graduation_year: Year of graduation
        graduation_date: Calculated/actual graduation date
        is_expected: Whether graduation is expected/future
        current_year: Current year for comparison (defaults to today's year)
    
    Returns:
        One of: "graduated", "not_yet_graduated", "unknown"
    """
    if current_year is None:
        current_year = datetime.now().year
    
    # Rule 1: If is_expected=true, definitely not yet graduated
    if is_expected is True:
        return "not_yet_graduated"
    
    # Rule 2: If graduation_year is in the future, not yet graduated
    if graduation_year is not None and graduation_year > current_year:
        return "not_yet_graduated"
    
    # Rule 3: If we have any graduation info and it's not future, they graduated
    if graduation_year is not None or graduation_date is not None:
        return "graduated"
    
    # No graduation info available
    return "unknown"


def _classify_job_experience(
    job_start_date: Optional[date],
    graduation_date_used: Optional[date]
) -> Optional[str]:
    """
    Classify a single job as worked_while_studying or worked_after_graduation.
    
    Args:
        job_start_date: Start date of the job
        graduation_date_used: The effective graduation date (date or calculated)
    
    Returns:
        "worked_while_studying", "worked_after_graduation", or None if can't classify
    """
    # If start_date is missing, can't classify this job
    if job_start_date is None:
        return None
    
    # If no graduation date to compare against, can't classify
    if graduation_date_used is None:
        return None
    
    # Compare: if job started before graduation, they were working while studying
    if job_start_date < graduation_date_used:
        return "worked_while_studying"
    else:
        return "worked_after_graduation"


def computeWorkWhileStudying(
    user_id: int,
    get_connection_func
) -> Optional[Dict[str, Any]]:
    """
    Determine if a user was working while studying.
    
    Args:
        user_id: The user ID to analyze
        get_connection_func: Function that returns a database connection
        
    Returns:
        Dict with structure:
        {
            user_id: int,
            graduation_year: int or None,
            graduation_date_used: date or None,
            graduated_status: "graduated" | "not_yet_graduated" | "unknown",
            is_working_while_studying: bool,
            evidence_jobs: [
                {
                    company: str,
                    title: str,
                    start_date: date or None,
                    end_date: date or None,
                    status: "worked_while_studying" | "worked_after_graduation"
                }
            ]
        }
        
        Returns None if user_id not found or database error occurs.
    """
    conn = None
    try:
        conn = get_connection_func()
        
        # Fetch education record for this user
        with conn.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT 
                    graduation_year,
                    graduation_month,
                    graduation_date,
                    is_expected
                FROM education
                WHERE user_id = %s
                LIMIT 1
            """, (user_id,))
            edu_record = cur.fetchone()
        
        # If no education record, return unknown status
        if not edu_record:
            logger.warning(f"No education record found for user_id={user_id}")
            return {
                "user_id": user_id,
                "graduation_year": None,
                "graduation_date_used": None,
                "graduated_status": "unknown",
                "is_working_while_studying": False,
                "evidence_jobs": []
            }
        
        # Extract education fields
        graduation_year = edu_record.get("graduation_year")
        graduation_month = edu_record.get("graduation_month")
        graduation_date = edu_record.get("graduation_date")
        is_expected = edu_record.get("is_expected", False)
        
        # Determine effective graduation date using rules
        graduation_date_used = _get_graduation_date(
            graduation_date, 
            graduation_year,
            graduation_month
        )
        
        # Determine graduation status
        graduated_status = _get_graduated_status(
            graduation_year,
            graduation_date,
            is_expected
        )
        
        # Fetch all experience records for this user
        with conn.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT 
                    company,
                    title,
                    start_date,
                    end_date,
                    is_current
                FROM experience
                WHERE user_id = %s
                ORDER BY start_date DESC NULLS LAST
            """, (user_id,))
            exp_records = cur.fetchall() or []
        
        # Classify each job and build evidence_jobs list
        evidence_jobs = []
        is_working_while_studying = False
        
        for exp in exp_records:
            company = exp.get("company")
            title = exp.get("title")
            start_date = exp.get("start_date")
            end_date = exp.get("end_date")
            is_current = exp.get("is_current", False)
            
            # Skip jobs with null start_date (can't classify)
            if start_date is None:
                logger.debug(
                    f"Skipping job for user {user_id}: "
                    f"company={company}, title={title} (null start_date)"
                )
                continue
            
            # Classify this job
            status = _classify_job_experience(start_date, graduation_date_used)
            
            # Only include jobs that could be classified (status is not None)
            if status is not None:
                evidence_jobs.append({
                    "company": company,
                    "title": title,
                    "start_date": start_date,
                    "end_date": end_date,
                    "status": status
                })
                
                # Check if any job was worked_while_studying
                if status == "worked_while_studying":
                    is_working_while_studying = True
        
        return {
            "user_id": user_id,
            "graduation_year": graduation_year,
            "graduation_date_used": graduation_date_used,
            "graduated_status": graduated_status,
            "is_working_while_studying": is_working_while_studying,
            "evidence_jobs": evidence_jobs
        }
    
    except Exception as e:
        logger.error(f"Error computing work while studying for user {user_id}: {e}")
        return None
    
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.error(f"Error closing connection: {e}")


# ============================================================
# ENSURE TABLE SCHEMA EXISTS
# ============================================================

def ensure_work_while_studying_schema(get_connection_func) -> bool:
    """
    Create education and experience tables if they don't exist.
    
    Args:
        get_connection_func: Function that returns a database connection
        
    Returns:
        True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_connection_func()
        
        with conn.cursor() as cur:
            # Create education table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS education (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL UNIQUE,
                    graduation_year INT NULL,
                    graduation_month INT NULL,
                    graduation_date DATE NULL,
                    is_expected BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_graduation_year (graduation_year)
                )
            """)
            logger.info("✅ Education table created/verified")
            
            # Create experience table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS experience (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL,
                    company TEXT NULL,
                    title TEXT NULL,
                    start_date DATE NULL,
                    end_date DATE NULL,
                    is_current BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_start_date (start_date),
                    INDEX idx_is_current (is_current)
                )
            """)
            logger.info("✅ Experience table created/verified")
            
            conn.commit()
            return True
    
    except Exception as e:
        logger.error(f"Error ensuring work_while_studying schema: {e}")
        return False
    
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
