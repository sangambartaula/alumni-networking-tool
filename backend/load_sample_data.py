#!/usr/bin/env python
"""
Load or Delete sample alumni data for testing purposes.
Usage:
    python load_sample_data.py          (Loads data)
    python load_sample_data.py --delete (Deletes data)
"""

import sys
import argparse
import os

# Ensure backend path is added so we can import database
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))) # if running from backend/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')) # if running from backend/ (to get root?)
# Actually simpler: assume run as `python backend/load_sample_data.py` from root or `python load_sample_data.py` from backend.
# Just try importing.
try:
    from database import get_connection
except ImportError:
    # If we are in backend/, try adding parent
    sys.path.append('..')
    try:
        from database import get_connection
    except ImportError:
        # If we are in root, try adding backend
        sys.path.append('backend')
        from database import get_connection

# Sample alumni data
sample_alumni = [
    {
        'first_name': 'John',
        'last_name': 'Smith',
        'grad_year': 2020,
        'degree': 'Bachelor of Science',
        'major': 'Computer Engineering',
        'current_job_title': 'Software Engineer',
        'company': 'Google',
        'location': 'San Francisco, CA',
        'headline': 'Software Engineer at Google | UNT Graduate',
        'linkedin_url': 'https://linkedin.com/in/johnsmith'
    },
    {
        'first_name': 'Sarah',
        'last_name': 'Johnson',
        'grad_year': 2019,
        'degree': 'Master of Business Administration',
        'major': 'Business Administration',
        'current_job_title': 'Product Manager',
        'company': 'Microsoft',
        'location': 'Seattle, WA',
        'headline': 'Product Manager at Microsoft',
        'linkedin_url': 'https://linkedin.com/in/sarahjohnson'
    },
    {
        'first_name': 'Michael',
        'last_name': 'Chen',
        'grad_year': 2021,
        'degree': 'Bachelor of Science',
        'major': 'Mechanical Engineering',
        'current_job_title': 'Data Scientist',
        'company': 'Amazon',
        'location': 'Austin, TX',
        'headline': 'Data Scientist at Amazon',
        'linkedin_url': 'https://linkedin.com/in/michaelchen'
    },
    {
        'first_name': 'Jessica',
        'last_name': 'Williams',
        'grad_year': 2018,
        'degree': 'Master of Science',
        'major': 'Electrical Engineering',
        'current_job_title': 'Engineering Manager',
        'company': 'Apple',
        'location': 'Cupertino, CA',
        'headline': 'Engineering Manager at Apple',
        'linkedin_url': 'https://linkedin.com/in/jessicawilliams'
    },
    {
        'first_name': 'David',
        'last_name': 'Martinez',
        'grad_year': 2020,
        'degree': 'Bachelor of Science',
        'major': 'Computer Engineering',
        'current_job_title': 'Senior Software Engineer',
        'company': 'Tesla',
        'location': 'Palo Alto, CA',
        'headline': 'Senior Software Engineer at Tesla',
        'linkedin_url': 'https://linkedin.com/in/davidmartinez'
    },
    {
        'first_name': 'Emily',
        'last_name': 'Brown',
        'grad_year': 2019,
        'degree': 'Bachelor of Science',
        'major': 'Civil Engineering',
        'current_job_title': 'UX Designer',
        'company': 'Meta',
        'location': 'Menlo Park, CA',
        'headline': 'UX Designer at Meta',
        'linkedin_url': 'https://linkedin.com/in/emilybrown'
    },
    {
        'first_name': 'Robert',
        'last_name': 'Taylor',
        'grad_year': 2021,
        'degree': 'Master of Business Administration',
        'major': 'Business Administration',
        'current_job_title': 'Business Analyst',
        'company': 'IBM',
        'location': 'New York, NY',
        'headline': 'Business Analyst at IBM',
        'linkedin_url': 'https://linkedin.com/in/roberttaylor'
    },
    {
        'first_name': 'Lisa',
        'last_name': 'Anderson',
        'grad_year': 2017,
        'degree': 'Master of Science',
        'major': 'Aerospace Engineering',
        'current_job_title': 'VP of Engineering',
        'company': 'Adobe',
        'location': 'San Jose, CA',
        'headline': 'VP of Engineering at Adobe',
        'linkedin_url': 'https://linkedin.com/in/lisaanderson'
    }
]

def load_data(conn):
    print(f"📥 Loading {len(sample_alumni)} sample records...")
    with conn.cursor() as cur:
        for alumni in sample_alumni:
            # Use REPLACE INTO or INSERT IGNORE to avoid duplicates
            cur.execute("""
                INSERT INTO alumni (first_name, last_name, grad_year, degree, major, current_job_title, company, location, headline, linkedin_url, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    first_name=VALUES(first_name),
                    last_name=VALUES(last_name),
                    current_job_title=VALUES(current_job_title),
                    company=VALUES(company)
            """, (
                alumni['first_name'],
                alumni['last_name'],
                alumni['grad_year'],
                alumni['degree'],
                alumni['major'],
                alumni['current_job_title'],
                alumni['company'],
                alumni['location'],
                alumni['headline'],
                alumni['linkedin_url']
            ))
        conn.commit()
    print("✅ Sample data loaded.")

def delete_data(conn):
    print(f"🗑️ Deleting {len(sample_alumni)} sample records...")
    with conn.cursor() as cur:
        for alumni in sample_alumni:
            cur.execute("DELETE FROM alumni WHERE linkedin_url = %s", (alumni['linkedin_url'],))
        conn.commit()
    print("✅ Sample data deleted.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage sample alumni data")
    parser.add_argument('--delete', action='store_true', help='Delete sample data instead of loading')
    args = parser.parse_args()

    try:
        conn = get_connection()
        if args.delete:
            delete_data(conn)
        else:
            load_data(conn)
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
