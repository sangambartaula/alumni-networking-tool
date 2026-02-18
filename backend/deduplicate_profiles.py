import os
import sys
from pathlib import Path
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add current directory to path so we can import database.py
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from database import get_connection, normalize_url
except ImportError:
    # If running from project root
    sys.path.insert(0, str(Path(__file__).resolve().parent / 'backend'))
    from database import get_connection, normalize_url

def deduplicate_table(table_name, url_col, update_col, timestamp_col, id_col='id'):
    """
    Deduplicate a table by normalized URL.
    - Keeps the record with the LATEST update_col.
    - Preserves the EARLIEST timestamp_col (merges it into the kept record).
    - Deletes all other duplicates.
    """
    logger.info(f"--- Deduplicating table '{table_name}' ---")
    
    conn = get_connection()
    try:
        # Fetch all records
        with conn.cursor(dictionary=True) as cur:
            # We need to fetch all columns to inspect them, but mainly ID, URL, Dates
            query = f"SELECT {id_col}, {url_col}, {update_col}, {timestamp_col} FROM {table_name}"
            cur.execute(query)
            rows = cur.fetchall()
        
        logger.info(f"Fetched {len(rows)} rows. Analyzing duplicates...")

        # Group by normalized URL
        grouped = {}
        for row in rows:
            raw_url = row[url_col]
            norm_url = normalize_url(raw_url)
            
            if not norm_url:
                continue
                
            if norm_url not in grouped:
                grouped[norm_url] = []
            grouped[norm_url].append(row)
        
        duplicates_found = 0
        records_deleted = 0
        
        for norm_url, group in grouped.items():
            if len(group) < 2:
                continue
            
            duplicates_found += 1
            
            # Sort by last_updated/last_checked DESCending (Latest is first)
            # Handle None values by treating them as very old
            def sort_key(r):
                val = r[update_col]
                if not val:
                    return "" # sort of works for strings/datetimes comparison usually, or 0
                return str(val)

            # Sort: Newest update first (Target to Keep)
            group.sort(key=sort_key, reverse=True)
            
            target = group[0]
            others = group[1:]
            
            # Find earliest creation/scraped timestamp
            earliest_ts = target[timestamp_col]
            
            for other in others:
                other_ts = other[timestamp_col]
                # If target has no TS, take other's. If other has TS and it's older, take it.
                if not earliest_ts and other_ts:
                    earliest_ts = other_ts
                elif earliest_ts and other_ts:
                    # String comparison works for ISO datetimes. 
                    # For python objects, standard comparison works.
                    if str(other_ts) < str(earliest_ts):
                        earliest_ts = other_ts
            
            # Update the target with the earliest timestamp AND normalized URL (to strip slash if present)
            with conn.cursor() as cur:
                # 1. Update Target
                update_query = f"""
                    UPDATE {table_name} 
                    SET {timestamp_col} = %s, {url_col} = %s 
                    WHERE {id_col} = %s
                """
                cur.execute(update_query, (earliest_ts, norm_url, target[id_col]))
                
                # 2. Delete Others
                ids_to_delete = [o[id_col] for o in others]
                if ids_to_delete:
                    # Construct comma-separated string for IN clause safely
                    placeholders = ','.join(['%s'] * len(ids_to_delete))
                    delete_query = f"DELETE FROM {table_name} WHERE {id_col} IN ({placeholders})"
                    cur.execute(delete_query, ids_to_delete)
                    records_deleted += len(ids_to_delete)
            
            conn.commit()
            
        logger.info(f"Analyzed {len(grouped)} unique URLs.")
        logger.info(f"Found {duplicates_found} groups with duplicates.")
        logger.info(f"Deleted {records_deleted} duplicate records.")
        logger.info(f"Merged earliest timestamps into surviving records.")

    except Exception as e:
        logger.error(f"Error deduplicating {table_name}: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()

def deduplicate_flagged_file():
    """Deduplicate the flagged_for_review.txt file."""
    logger.info("--- Deduplicating flagged_for_review.txt ---")
    
    # Path is relative to project root usually
    project_root = Path(__file__).resolve().parent.parent
    flagged_file = project_root / 'scraper' / 'output' / 'flagged_for_review.txt'
    
    if not flagged_file.exists():
        logger.info("No flagged_for_review.txt found.")
        return

    try:
        lines = flagged_file.read_text(encoding='utf-8').splitlines()
        unique_lines = {} # norm_url -> line
        
        for line in lines:
            if not line.strip(): continue
            
            # Format: URL # Issue1; Issue2
            parts = line.split('#')
            raw_url = parts[0].strip()
            if not raw_url: continue
            
            norm_url = normalize_url(raw_url)
            
            # If we already have this URL, maybe keep the one with longer text (more issues?)
            # or just the first/last one. Let's keep the last one to be consistent 
            # or just simple set behavior.
            unique_lines[norm_url] = line
            
        # Write back
        with open(flagged_file, 'w', encoding='utf-8') as f:
            for line in unique_lines.values():
                f.write(line + '\n')
                
        logger.info(f"Reduced {len(lines)} lines to {len(unique_lines)} in flagged file.")
        
    except Exception as e:
        logger.error(f"Error processing flagged file: {e}")

if __name__ == "__main__":
    logger.info("Starting Deduplication Process...")
    
    # 1. Deduplicate 'alumni' table
    # url_col='linkedin_url', update_col='last_updated', timestamp_col='scraped_at'
    deduplicate_table('alumni', 'linkedin_url', 'last_updated', 'scraped_at')
    
    # 2. Deduplicate 'visited_profiles' table
    # url_col='linkedin_url', update_col='last_checked', timestamp_col='visited_at'
    deduplicate_table('visited_profiles', 'linkedin_url', 'last_checked', 'visited_at')
    
    # 3. Deduplicate text file
    deduplicate_flagged_file()
    
    logger.info("Deduplication Complete.")
