#!/usr/bin/env python
"""Add major column to alumni table and related migration"""

import sys
sys.path.insert(0, 'backend')
from database import get_connection

def add_major_column():
    """Add major column to alumni table if it doesn't exist"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Try to add the major column
            try:
                cur.execute("""
                    ALTER TABLE alumni 
                    ADD COLUMN major VARCHAR(255) DEFAULT NULL
                """)
                conn.commit()
                print("✅ Added major column to alumni table")
            except Exception as e:
                if "Duplicate column name" in str(e):
                    print("✅ Major column already exists")
                else:
                    raise
    finally:
        conn.close()

if __name__ == '__main__':
    add_major_column()
