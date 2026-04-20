import os
import sys
import pandas as pd
from datetime import datetime

# Adjust path to import backend
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from backend.database import get_connection

def sync_csv_to_db():
    print("Initiating database connection...")
    conn = get_connection()
    if not conn:
        print("Failed to connect to database.")
        return
        
    try:
        cur = conn.cursor()
        
        # 1. Create backup table
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_table = f"alumni_backup_{timestamp}"
        print(f"Creating backup table '{backup_table}' from current 'alumni' table...")
        
        if hasattr(conn, 'cmd_query'):
            # It's MySQL
            cur.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM alumni")
        else:
            # It's SQLite fallback behavior
            cur.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM alumni")
            
        print(f"Backup table '{backup_table}' created successfully.")
        
        # 2. Get columns of alumni table dynamically
        if hasattr(conn, 'cmd_query'):
            cur.execute("SHOW COLUMNS FROM alumni")
            db_columns = [row[0] for row in cur.fetchall()]
        else:
            cur.execute("PRAGMA table_info(alumni)")
            db_columns = [row[1] for row in cur.fetchall()]

        # 3. Process CSV matching
        csv_path = 'scraper/output/UNT_Alumni_Data.csv'
        if not os.path.exists(csv_path):
            print(f"Could not find CSV data at {csv_path}")
            return
            
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows from CSV. Starting DB updates...")
        
        cols_to_update = [
            'normalized_job_title', 
            'normalized_exp2_title', 
            'normalized_exp3_title',
            'standardized_degree', 
            'standardized_degree2', 
            'standardized_degree3'
        ]
        
        # Filter strictly those columns that exist in DB
        valid_cols = [c for c in cols_to_update if c in db_columns and c in df.columns]
        
        if not valid_cols:
            print("No matching normalized columns found in database schema!")
            return
            
        print(f"Updating DB columns: {', '.join(valid_cols)}")
        
        updates_attempted = 0
        updates_matched = 0
        
        for idx, row in df.iterrows():
            url = row.get('linkedin_url')
            if pd.isna(url) or not str(url).strip():
                continue
            
            clean_url = str(url).strip().rstrip('/')
            
            updates = {}
            for col in valid_cols:
                val = row.get(col)
                updates[col] = "" if pd.isna(val) else str(val).strip()
            
            if updates:
                if hasattr(conn, 'cmd_query'):
                    # MySQL formatted query with %s
                    set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                    query = f"UPDATE alumni SET {set_clause} WHERE linkedin_url = %s OR linkedin_url = %s"
                    values = list(updates.values())
                    values.extend([clean_url, clean_url + '/'])
                else:
                    # SQLite formatted query with ?
                    set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
                    query = f"UPDATE alumni SET {set_clause} WHERE linkedin_url = ? OR linkedin_url = ?"
                    values = list(updates.values())
                    values.extend([clean_url, clean_url + '/'])
                
                cur.execute(query, tuple(values))
                updates_attempted += 1
                updates_matched += cur.rowcount
                
        conn.commit()
        print(f"\nFinished processing.")
        print(f"Searched for {updates_attempted} URLs from CSV.")
        print(f"Successfully matched and updated {updates_matched} rows in the database.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    sync_csv_to_db()
