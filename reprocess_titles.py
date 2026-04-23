#!/usr/bin/env python3
"""Thin CLI wrapper: forwards to scripts/reprocess_titles.py (same argv)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main() -> int:
    script = ROOT / "scripts" / "reprocess_titles.py"
    return subprocess.call([sys.executable, str(script), *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
