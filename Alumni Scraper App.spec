# -*- mode: python ; coding: utf-8 -*-

import os


import sys

PROJECT_ROOT = SPECPATH

# On Windows PyInstaller needs .ico (or Pillow installed for auto-conversion).
# Use the icon only when the file exists; fall back to no icon to avoid build failures.
_icon_candidate = os.path.join(PROJECT_ROOT, "frontend", "public", "assets", "unt-logo-square.png")
ICON_PATH = [_icon_candidate] if os.path.isfile(_icon_candidate) else []

a = Analysis(
    ['scraper_gui.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Alumni Scraper App',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=ICON_PATH,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Alumni Scraper App',
)
