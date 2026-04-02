#!/usr/bin/env python3
"""
Seed Admins Script
==================
This script ensures the 7 default designated admins (faculty)
are added to the whitelist and graded the 'admin' role in the database.

It is safe to re-run (idempotent).
Note: This is automatically run on API startup as part of the auth migration.
"""

import sys
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)

# Make backend and migrations modules importable
root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir / "backend"))
sys.path.insert(0, str(root_dir))
os.chdir(root_dir)

from migrations.migrate_auth_system import migrate

if __name__ == "__main__":
    logging.info("Starting admin seeding process...")
    # The migration script contains the list of default admins and idempotent seeding logic
    migrate()
    logging.info("Admin seeding complete.")
