#!/usr/bin/env python
"""Load sample alumni data into the database"""

import sys
sys.path.insert(0, 'backend')
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

conn = get_connection()
try:
    with conn.cursor() as cur:
        for alumni in sample_alumni:
            cur.execute("""
                INSERT INTO alumni (first_name, last_name, grad_year, degree, major, current_job_title, company, location, headline, linkedin_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        print(f"✅ Inserted {len(sample_alumni)} sample alumni records")
finally:
    conn.close()
