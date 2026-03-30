@echo off
echo Building standalone Windows Application for UNT Alumni Scraper...

:: Ensure we have a virtual environment loaded
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

:: Clear old build caches
echo Clearing old build cache...
if exist "build" rmdir /s /q "build"
if exist "dist\UNT Alumni Scraper.exe" del /f /q "dist\UNT Alumni Scraper.exe"
if exist "dist\Alumni Scraper App.exe" del /f /q "dist\Alumni Scraper App.exe"
del /f /q *.spec

:: Install requirements
echo Installing PyQt6 and PyInstaller...
pip install PyQt6 pyinstaller python-dotenv

:: Build using PyInstaller
echo Bundling App...
pyinstaller --clean --name "Alumni Scraper App" --windowed --noconfirm scraper_gui.py

echo Build complete! The application has been created inside the 'dist' folder.
echo You can move 'UNT Alumni Scraper.exe' anywhere, but for it to function correctly, place it in the same parent folder as your 'venv' and 'scraper' directories.
pause
