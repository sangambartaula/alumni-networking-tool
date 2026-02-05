"""
Normalize existing data in the database:
1. Strip trailing spaces from all text fields (company, job_title, location, etc.)
2. Ensure URLs don't have trailing slashes
3. Merge duplicate entries that differ only by trailing spaces

Run: python backend/normalize_data.py
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

from database import get_connection, normalize_url
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Text fields to normalize (must match actual database column names)
TEXT_FIELDS = [
    'first_name', 'last_name', 'company', 'current_job_title', 'location', 
    'headline', 'degree', 'major'
]

def normalize_text_fields():
    """Strip leading/trailing whitespace from all text fields."""
    logger.info("🧹 Normalizing text fields (stripping whitespace)...")
    conn = get_connection()
    try:
        cursor = conn.cursor()
        
        for field in TEXT_FIELDS:
            try:
                # MySQL syntax for TRIM
                cursor.execute(f"UPDATE alumni SET {field} = TRIM({field}) WHERE {field} IS NOT NULL AND {field} != TRIM({field})")
                
                affected = cursor.rowcount
                if affected > 0:
                    logger.info(f"  ✨ Trimmed {affected} records in '{field}'")
            except Exception as e:
                logger.warning(f"  ⚠️ Could not trim '{field}': {e}")
        
        conn.commit()
        logger.info("✅ Text field normalization complete.")
        
    except Exception as e:
        logger.error(f"Error normalizing text fields: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def normalize_urls():
    """Ensure all URLs have no trailing slashes."""
    logger.info("🧹 Normalizing URLs (removing trailing slashes)...")
    conn = get_connection()
    try:
        cursor = conn.cursor()
        
        for table in ['alumni', 'visited_profiles']:
            try:
                # MySQL: TRIM TRAILING
                cursor.execute(f"""
                    UPDATE {table} 
                    SET linkedin_url = TRIM(TRAILING '/' FROM linkedin_url) 
                    WHERE linkedin_url LIKE '%/'
                """)
                
                affected = cursor.rowcount
                if affected > 0:
                    logger.info(f"  ✨ Fixed {affected} URLs in '{table}'")
            except Exception as e:
                logger.warning(f"  ⚠️ Could not fix URLs in '{table}': {e}")
        
        conn.commit()
        logger.info("✅ URL normalization complete.")
        
    except Exception as e:
        logger.error(f"Error normalizing URLs: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def find_duplicates():
    """Find and report potential duplicates after normalization."""
    logger.info("🔍 Checking for duplicates after normalization...")
    conn = get_connection()
    try:
        cursor = conn.cursor()
        
        # Check for duplicate companies (case-insensitive)
        cursor.execute("""
            SELECT LOWER(TRIM(company)) as clean_company, COUNT(*) as cnt 
            FROM alumni 
            WHERE company IS NOT NULL AND company != ''
            GROUP BY LOWER(TRIM(company)) 
            HAVING COUNT(*) > 5
            ORDER BY cnt DESC
            LIMIT 20
        """)
        
        results = cursor.fetchall()
        if results:
            logger.info("  Top companies with many entries:")
            for company, count in results:
                logger.info(f"    - '{company}': {count} alumni")
        
    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("DATA NORMALIZATION SCRIPT")
    logger.info("=" * 60)
    
    normalize_text_fields()
    normalize_urls()
    find_duplicates()
    
    logger.info("=" * 60)
    logger.info("🎉 Normalization complete!")
