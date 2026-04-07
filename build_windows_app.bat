@echo off
echo Building standalone Windows Application for UNT Alumni Scraper...

:: Detect Python command
where python3 >nul 2>nul
if %ERRORLEVEL% equ 0 (
    set PYTHON_CMD=python3
) else (
    where python >nul 2>nul
    if %ERRORLEVEL% equ 0 (
        set PYTHON_CMD=python
    ) else (
        echo Error: Python not found. Please install Python 3.
        pause
        exit /b 1
    )
)

:: Ensure we have a virtual environment loaded
if not exist venv (
    echo Creating virtual environment...
    %PYTHON_CMD% -m venv venv
)
call venv\Scripts\activate.bat

:: Clear old build caches
echo Clearing old build cache...
if exist "build" rmdir /s /q "build"
if exist "dist\Alumni Scraper App.exe" del /f /q "dist\Alumni Scraper App.exe"
del /f /q *.spec

:: Install requirements with optimization
echo Installing build dependencies (using --prefer-binary for speed)...
pip install --prefer-binary PyQt6 pyinstaller python-dotenv pillow
pip install --prefer-binary -r requirements.txt
if errorlevel 1 (
    echo Dependency installation failed. Aborting build.
    pause
    exit /b 1
)

:: Build using PyInstaller
echo Squarifying icon to prevent stretching...
%PYTHON_CMD% scripts\pad_icon.py
if errorlevel 1 (
    echo Icon preparation failed. Aborting build.
    pause
    exit /b 1
)

echo Bundling App...
pyinstaller --clean --name "Alumni Scraper App" --windowed --icon="frontend/public/assets/unt-logo-square.png" --noconfirm scraper_gui.py
if errorlevel 1 (
    echo PyInstaller build failed. Ensure Pillow is installed and icon path is valid.
    pause
    exit /b 1
)

echo Build complete! The application has been created inside the 'dist' folder.
echo You can move 'UNT Alumni Scraper.exe' anywhere, but for it to function correctly, place it in the same parent folder as your 'venv' and 'scraper' directories.
pause
