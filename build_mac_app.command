#!/bin/bash
cd "$(dirname "$0")"

echo "Building standalone Mac Application for UNT Alumni Scraper..."

# Ensure we have a virtual environment loaded
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Force clear old cached builds
echo "Clearing old build cache..."
rm -rf "build"
rm -rf "build/Alumni Scraper App"
rm -rf "dist/UNT Alumni Scraper.app"
rm -rf "dist/Alumni Scraper App.app"
rm -rf "dist/Alumni Scraper App"
rm -f *.spec

# Install requirements
echo "Installing PyQt6 and PyInstaller..."
pip install PyQt6 pyinstaller python-dotenv pillow
pip install -r requirements.txt

# Detect Python command
if command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
    PYTHON_CMD="python"
else
    echo "Error: Python not found. Please install Python 3."
    exit 1
fi

# Build using PyInstaller
echo "Squarifying icon to prevent stretching..."
$PYTHON_CMD scripts/pad_icon.py

# Remove extended attributes (detritus) that can cause codesign to fail
echo "Cleaning extended attributes from source..."
xattr -cr .

echo "Bundling App..."
pyinstaller --clean \
            --name "Alumni Scraper App" \
            --windowed \
            --icon="frontend/public/assets/unt-logo-square.png" \
            --noconfirm \
            scraper_gui.py

# After bundle, clean attributes from the app bundle itself if it was created
if [ -d "dist/Alumni Scraper App.app" ]; then
    echo "Cleaning detritus from built app bundle..."
    xattr -cr "dist/Alumni Scraper App.app"
    echo "Attempting to re-sign the app bundle ad-hoc..."
    codesign --force --deep --sign - "dist/Alumni Scraper App.app"
fi

echo "Build complete! The application has been created inside the 'dist' folder."
echo "You can move 'UNT Alumni Scraper.app' anywhere, but for it to function correctly, place it in the same parent folder as your 'venv' and 'scraper' directories."
