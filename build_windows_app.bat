@echo off
echo Building standalone Windows Application for UNT Alumni Scraper...

:: Ensure we have a virtual environment loaded
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

:: Clear old build caches
echo Clearing old build cache...
if exist "build" rmdir /s /q "build"
if exist "build\Alumni Scraper App" rmdir /s /q "build\Alumni Scraper App"
if exist "dist\UNT Alumni Scraper.exe" del /f /q "dist\UNT Alumni Scraper.exe"
if exist "dist\Alumni Scraper App.exe" del /f /q "dist\Alumni Scraper App.exe"
if exist "dist\Alumni Scraper App" rmdir /s /q "dist\Alumni Scraper App"
del /f /q *.spec

:: Install requirements
echo Installing PyQt6 and PyInstaller...
pip install PyQt6 pyinstaller python-dotenv

:: Increment desktop app version and update remote manifest metadata.
echo Bumping app version...
python scripts\bump_app_version.py

:: Build using PyInstaller
echo Squarifying icon to prevent stretching...
python scripts\pad_icon.py

echo Bundling App...
pyinstaller --clean --name "Alumni Scraper App" --windowed --icon="frontend/public/assets/unt-logo-square.png" --noconfirm scraper_gui.py

echo Build complete! The application has been created inside the 'dist' folder.
echo You can move 'UNT Alumni Scraper.exe' anywhere, but for it to function correctly, place it in the same parent folder as your 'venv' and 'scraper' directories.
pause
