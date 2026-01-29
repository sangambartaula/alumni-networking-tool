import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import mysql.connector

# Add backend to path to reuse database connection logic
sys.path.insert(0, str(Path(__file__).parent / "backend"))
import database

def export_urls_to_flagged():
    """Fetch all unique LinkedIn URLs from MySQL and save to flagged_for_review.txt"""
    load_dotenv()
    
    output_file = Path("scraper") / "output" / "flagged_for_review.txt"
    
    print("🔌 Connecting to Railway Database...")
    try:
        # Use direct connection (bypass SQLite fallback)
        conn = database.get_direct_mysql_connection()
        cursor = conn.cursor()
        
        print("🔍 Fetching unique LinkedIn URLs...")
        cursor.execute("SELECT DISTINCT linkedin_url FROM alumni WHERE linkedin_url IS NOT NULL AND linkedin_url != ''")
        urls = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        if not urls:
            print("❌ No URLs found in database.")
            return

        print(f"✅ Found {len(urls)} URLs.")
        
        # Ensure directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to file
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("# URLs exported from Railway DB for rescrape\n")
            for url in urls:
                f.write(f"{url}\n")
                
        print(f"💾 Successfully wrote {len(urls)} URLs to {output_file}")
        print("🚀 Ready! Now run: python scraper/main.py")
        print("   (It will detect the file and ask if you want to run REVIEW mode)")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    export_urls_to_flagged()
