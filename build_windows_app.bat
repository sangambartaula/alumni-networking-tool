@echo off
echo ============================================================
echo  Building standalone Windows App for UNT Alumni Scraper
echo ============================================================

:: --- Detect base Python ---
where python3 >nul 2>nul
if %ERRORLEVEL% equ 0 (
    set PYTHON_CMD=python3
    goto :python_found
)
where python >nul 2>nul
if %ERRORLEVEL% equ 0 (
    set PYTHON_CMD=python
    goto :python_found
)
echo ERROR: Python not found. Please install Python 3 and add it to PATH.
exit /b 1

:python_found

:: --- Ensure virtual environment exists ---
if not exist "venv\Scripts\python.exe" (
    echo Creating virtual environment...
    %PYTHON_CMD% -m venv venv
    if %ERRORLEVEL% neq 0 (
        echo ERROR: Failed to create virtual environment.
        exit /b 1
    )
)

echo Using Python from venv:
venv\Scripts\python.exe --version

:: --- Stop stale processes that lock dist files ---
echo.
echo Stopping running app/processes that may lock build artifacts...
taskkill /f /im "Alumni Scraper App.exe" >nul 2>nul
taskkill /f /im "pyinstaller.exe" >nul 2>nul

:: --- Clear old build caches ---
echo.
echo Clearing old build cache...
if exist "build" rmdir /s /q "build"
if exist "dist\Alumni Scraper App" (
    rmdir /s /q "dist\Alumni Scraper App"
    if exist "dist\Alumni Scraper App" (
        echo Waiting for file handles to release...
        timeout /t 2 /nobreak >nul
        rmdir /s /q "dist\Alumni Scraper App"
    )
)
if exist "dist\Alumni Scraper App" (
    echo ERROR: Could not remove dist\Alumni Scraper App. Close Explorer windows or AV locks and retry.
    exit /b 1
)
if exist "Alumni Scraper App.spec" del /f /q "Alumni Scraper App.spec"

:: --- Install build dependencies ---
echo.
echo Installing build dependencies...
venv\Scripts\pip.exe install --prefer-binary --quiet --disable-pip-version-check PyQt6 pyinstaller python-dotenv pillow
if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to install core build dependencies.
    exit /b 1
)

venv\Scripts\pip.exe install --prefer-binary --quiet --disable-pip-version-check -r requirements.txt
if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to install requirements.txt dependencies.
    exit /b 1
)
echo Dependencies installed.

:: --- Prepare icon ---
echo.
echo Preparing icon...
if not exist "scripts\pad_icon.py" goto :skip_icon
venv\Scripts\python.exe scripts\pad_icon.py
if %ERRORLEVEL% neq 0 (
    echo WARNING: Icon script failed. Building without custom icon.
    goto :build_no_icon
)
goto :build_with_icon

:skip_icon
echo WARNING: scripts\pad_icon.py not found. Skipping icon step.
goto :build_no_icon

:build_with_icon
echo.
echo Bundling application with PyInstaller (with icon)...
venv\Scripts\python.exe -m PyInstaller --clean --name "Alumni Scraper App" --windowed --icon="frontend\public\assets\unt-logo-square.png" --noconfirm scraper_gui.py
goto :check_build

:build_no_icon
echo.
echo Bundling application with PyInstaller (no icon)...
venv\Scripts\python.exe -m PyInstaller --clean --name "Alumni Scraper App" --windowed --noconfirm scraper_gui.py

:check_build
:: PyInstaller may return exit code 1 for warnings even on success.
:: Check if the exe was actually created instead.
if not exist "dist\Alumni Scraper App\Alumni Scraper App.exe" (
    echo ERROR: PyInstaller build failed - exe not found in dist folder.
    exit /b 1
)

:build_done
echo.
echo ============================================================
echo  BUILD COMPLETE!
echo  Find your app at:
echo    dist\Alumni Scraper App\Alumni Scraper App.exe
echo ============================================================
exit /b 0
