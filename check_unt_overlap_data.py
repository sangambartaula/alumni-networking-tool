"""
Check 10 sample records from the database for UNT overlap analysis.
"""

import sys
sys.path.insert(0, 'backend')

from backend.database import get_connection
from backend.work_while_studying import computeWorkWhileStudying
from datetime import date

def check_sample_data():
    """Check 10 sample users for UNT overlap analysis."""
    conn = None
    try:
        conn = get_connection()
        
        # First, check what tables exist
        print("🔍 Checking available tables...")
        with conn.cursor(dictionary=True) as cur:
            try:
                # Try MySQL syntax first
                cur.execute("SHOW TABLES")
                tables = cur.fetchall()
                print(f"Available tables: {[list(t.values())[0] for t in tables]}")
            except Exception:
                # Try SQLite syntax
                cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cur.fetchall()
                table_names = [t['name'] if isinstance(t, dict) else t[0] for t in tables]
                print(f"Available tables: {table_names}")
        
        # Check if education table exists
        with conn.cursor(dictionary=True) as cur:
            # Get 10 users who have UNT education records
            cur.execute("""
                SELECT DISTINCT alumni_id
                FROM education
                WHERE school_name LIKE '%University of North Texas%'
                   OR school_name LIKE '%UNT%'
                LIMIT 10
            """)
            users = cur.fetchall()
        
        if not users:
            print("❌ No UNT education records found in database")
            print("\nChecking what's in the education table...")
            with conn.cursor(dictionary=True) as cur:
                cur.execute("SELECT COUNT(*) as count FROM education")
                count = cur.fetchone()
                print(f"Total education records: {count['count']}")
                
                cur.execute("SELECT DISTINCT school_name FROM education LIMIT 10")
                schools = cur.fetchall()
                print("\nSample school names:")
                for school in schools:
                    print(f"  - {school['school_name']}")
            return
        
        print(f"✅ Found {len(users)} UNT alumni records\n")
        print("=" * 80)
        
        # Analyze each user
        for i, user_row in enumerate(users, 1):
            alumni_id = user_row['alumni_id']
            
            print(f"\n{'=' * 80}")
            print(f"RECORD #{i} - Alumni ID: {alumni_id}")
            print('=' * 80)
            
            # Get education details
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT school_name, school_start_date, school_start_year,
                           graduation_date, graduation_year, is_expected
                    FROM education
                    WHERE alumni_id = %s
                """, (alumni_id,))
                edu = cur.fetchone()
            
            if edu:
                print("\n📚 EDUCATION:")
                print(f"  School: {edu['school_name']}")
                print(f"  Start: {edu['school_start_date']} (year: {edu['school_start_year']})")
                print(f"  Grad: {edu['graduation_date']} (year: {edu['graduation_year']})")
                print(f"  Expected: {edu['is_expected']}")
            
            # Get experience details
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT company, title, start_date, end_date, is_current
                    FROM experience
                    WHERE alumni_id = %s
                    ORDER BY start_date DESC NULLS LAST
                    LIMIT 5
                """, (alumni_id,))
                jobs = cur.fetchall()
            
            if jobs:
                print(f"\n💼 EXPERIENCE ({len(jobs)} jobs shown):")
                for j, job in enumerate(jobs, 1):
                    print(f"  {j}. {job['company']} - {job['title']}")
                    print(f"     {job['start_date']} → {job['end_date']} (current: {job['is_current']})")
            else:
                print("\n💼 EXPERIENCE: No jobs found")
            
            # Run the UNT overlap analysis
            result = computeWorkWhileStudying(alumni_id, get_connection)
            
            if result:
                print("\n🔍 UNT OVERLAP ANALYSIS:")
                print(f"  UNT Attendance: {result['unt_start']} → {result['unt_end']}")
                print(f"  Worked while at UNT: {'✅ YES' if result['worked_while_at_unt'] else '❌ NO'}")
                
                if result['evidence_jobs']:
                    print(f"\n  Overlapping Jobs ({len(result['evidence_jobs'])}):")
                    for job in result['evidence_jobs']:
                        print(f"    • {job['company']} - {job['title']}")
                        print(f"      {job['start_date']} → {job['end_date']}")
                else:
                    print("  No overlapping jobs found")
            else:
                print("\n❌ Failed to analyze user")
        
        print("\n" + "=" * 80)
        print("✅ Sample data check complete")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


if __name__ == '__main__':
    check_sample_data()
