
import pandas as pd
import os
import sys
import sqlite3

# Setup paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.append(os.path.join(ROOT_DIR, "backend"))

from database import get_connection

CSV_PATH = os.path.join(BASE_DIR, "output", "UNT_Alumni_Data.csv")
FLAGGED_PATH = os.path.join(BASE_DIR, "output", "flagged_for_review.txt")

def clean_data():
    print("🚀 Starting data cleanup...")
    
    # 1. Load CSV
    if not os.path.exists(CSV_PATH):
        print("❌ CSV not found!")
        return
    
    df = pd.read_csv(CSV_PATH)
    initial_count = len(df)
    print(f"📊 Loaded {initial_count} rows from CSV.")
    
    # Create temp full name for matching
    df['full_name'] = (df['first'].fillna('') + ' ' + df['last'].fillna('')).str.strip()

    # 2. Fix Majors
    corrections = {
        "Aaditi Bhandari": "Data Science",
        "Alexis Aguilar": "Data Science",
        "Ajay Kumar": "Data Science",
        "Amruth Jaligama": "Data Science"
    }
    
    db_updates = [] # List of (url, new_major) tuples for DB sync
    
    print("🛠 Fixing majors...")
    for name, new_major in corrections.items():
        # Match by name
        mask = df['full_name'].str.contains(name, case=False, na=False)
        if mask.any():
            df.loc[mask, 'standardized_major'] = new_major
            df.loc[mask, 'discipline'] = "Software, Data & AI Engineering"
            
            # Store URLs for DB update
            urls = df.loc[mask, 'linkedin_url'].tolist()
            for url in urls:
                if pd.notna(url):
                    db_updates.append((url, new_major))
            
            print(f"   ✓ Updated {name} -> {new_major}")
        else:
            print(f"   ⚠️ Could not find {name}")

    # 3. Deduplicate Aparna
    print("🗑 Deduplicating Aparna Chalumuri...")
    # Matches: Aparna Chalumuri
    # Keep: https://www.linkedin.com/in/aparna-chalumuri (scraped 12:34:05)
    # Remove: https://www.linkedin.com/in/aparna-chalumuri-36a060216 (scraped 12:35:05)
    
    target_duplicate_url = "https://www.linkedin.com/in/aparna-chalumuri-36a060216"
    
    dup_mask = df['linkedin_url'] == target_duplicate_url
    if dup_mask.any():
        df = df[~dup_mask]
        print(f"   ✓ Removed duplicate row with URL: {target_duplicate_url}")
    else:
        print("   ℹ️ Duplicate Aparna URL ({target_duplicate_url}) not found in CSV.")

    # Drop temp column
    df = df.drop(columns=['full_name'])

    # 4. Save CSV
    df.to_csv(CSV_PATH, index=False)
    print(f"💾 Saved CSV. Rows: {len(df)}")

    # 5. Sync DB
    print("🗄 Syncing Database...")
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Majors updates using precise URLs
        updated_count = 0
        for url, new_major in db_updates:
            cursor.execute("""
                UPDATE alumni 
                SET standardized_major = ?, discipline = 'Software, Data & AI Engineering'
                WHERE profile_url = ?
            """, (new_major, url))
            updated_count += cursor.rowcount
            
        if updated_count > 0:
            print(f"   ✓ DB Updated {updated_count} records with corrected majors.")

        # Remove duplicate Aparna
        cursor.execute("DELETE FROM alumni WHERE profile_url = ?", (target_duplicate_url,))
        if cursor.rowcount > 0:
            print("   ✓ DB Deleted duplicate Aparna row.")
            
        conn.commit()
    except Exception as e:
        print(f"❌ DB Sync Error: {e}")
    finally:
        conn.close()

    # 6. Clean Flagged List
    print("🧹 Cleaning flagged_for_review.txt...")
    if os.path.exists(FLAGGED_PATH):
        # normalize CSV URLs for comparison (strip slash)
        scraped_urls = set(df['linkedin_url'].apply(lambda x: x.rstrip('/') if isinstance(x, str) else ""))
        
        with open(FLAGGED_PATH, 'r') as f:
            lines = f.readlines()
        
        new_lines = []
        removed_count = 0
        for line in lines:
            url = line.split('#')[0].strip().rstrip('/')
            if url in scraped_urls:
                removed_count += 1
            else:
                new_lines.append(line)
        
        with open(FLAGGED_PATH, 'w') as f:
            f.writelines(new_lines)
            
        print(f"   ✓ Removed {removed_count} URLs from flagged list used in CSV.")
    else:
        print("   ⚠️ flagged_for_review.txt not found.")

    print("✅ Cleanup Complete!")

if __name__ == "__main__":
    clean_data()
