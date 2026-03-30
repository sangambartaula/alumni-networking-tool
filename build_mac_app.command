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
rm -rf "dist/UNT Alumni Scraper.app"
rm -rf "dist/Alumni Scraper App.app"
rm -f *.spec

# Install requirements
echo "Installing PyQt6 and PyInstaller..."
pip install PyQt6 pyinstaller python-dotenv

# Build using PyInstaller
echo "Bundling App..."
pyinstaller --clean \
            --name "Alumni Scraper App" \
            --windowed \
            --noconfirm \
            scraper_gui.py

echo "Build complete! The application has been created inside the 'dist' folder."
echo "You can move 'UNT Alumni Scraper.app' anywhere, but for it to function correctly, place it in the same parent folder as your 'venv' and 'scraper' directories."
