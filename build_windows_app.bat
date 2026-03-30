@echo off
echo Building standalone Windows Application for UNT Alumni Scraper...

:: Ensure we have a virtual environment loaded
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

:: Clear old build caches
echo Clearing old build cache...
if exist "build\UNT Alumni Scraper" rmdir /s /q "build\UNT Alumni Scraper"
if exist "dist\UNT Alumni Scraper.exe" del /f /q "dist\UNT Alumni Scraper.exe"
if exist "UNT Alumni Scraper.spec" del /f /q "UNT Alumni Scraper.spec"

:: Install requirements
echo Installing PyQt6 and PyInstaller...
pip install PyQt6 pyinstaller python-dotenv

:: Build using PyInstaller
echo Bundling App...
pyinstaller --clean --name "UNT Alumni Scraper" --windowed --noconfirm scraper_gui.py

echo Build complete! The application has been created inside the 'dist' folder.
echo You can move 'UNT Alumni Scraper.exe' anywhere, but for it to function correctly, place it in the same parent folder as your 'venv' and 'scraper' directories.
pause
