import sys
import os
import subprocess
import signal
import threading
import sqlite3
import shutil
import csv
import json
import re
import webbrowser
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QGridLayout, QGroupBox, QLabel, QLineEdit, QComboBox, 
    QCheckBox, QPushButton, QTextEdit, QMessageBox, QDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QFileDialog, QSizePolicy, QTabWidget, QFormLayout, QScrollArea, QFrame, QSplitter
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QTimer
from PyQt6.QtGui import QFont, QIcon


def _format_runtime_short(total_seconds):
    total_seconds = max(0, int(total_seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _safe_load_dotenv_values(env_path):
    """Load .env defensively across mixed encodings on different OSes."""
    if not os.path.exists(env_path):
        return {}
    try:
        return dotenv.dotenv_values(env_path, encoding="utf-8")
    except Exception:
        try:
            return dotenv.dotenv_values(env_path, encoding="latin-1")
        except Exception:
            return {}

def get_base_dir():
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
        if sys.platform == 'darwin':
            base_dir = os.path.abspath(os.path.join(base_dir, '../../..'))
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
    # If the app is sitting inside a 'dist' folder, find the actual project root
    temp_dir = base_dir
    for _ in range(3):
        if os.path.exists(os.path.join(temp_dir, 'scraper', 'main.py')):
            return temp_dir
        temp_dir = os.path.dirname(temp_dir)
        
    return base_dir

# Update .env utility
def update_env(key, value):
    # Determine base directory depending on if frozen by PyInstaller
    base_dir = get_base_dir()
        
    env_path = os.path.join(base_dir, '.env')
    lines = []
    found = False
    if os.path.exists(env_path):
        try:
            with open(env_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
        except Exception:
            lines = []
            
    for i, line in enumerate(lines):
        if line.strip().startswith(f"{key}="):
            lines[i] = f"{key}={value}\n"
            found = True
            break
            
    if not found:
        if lines and not lines[-1].endswith('\n'):
            lines.append('\n')
        lines.append(f"{key}={value}\n")
        
    with open(env_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)


def update_env_many(updates):
    """Atomically write multiple .env keys while preserving existing order/comments."""
    base_dir = get_base_dir()
    env_path = os.path.join(base_dir, '.env')

    lines = []
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()

    keys_to_set = set(updates.keys())
    seen = set()
    rewritten = []
    for line in lines:
        stripped = line.strip()
        if '=' in stripped and not stripped.startswith('#'):
            key = stripped.split('=', 1)[0].strip()
            if key in updates:
                rewritten.append(f"{key}={updates[key]}\n")
                seen.add(key)
                continue
        rewritten.append(line)

    missing_keys = [k for k in updates.keys() if k not in seen]
    if missing_keys and rewritten and not rewritten[-1].endswith('\n'):
        rewritten.append('\n')
    for key in missing_keys:
        rewritten.append(f"{key}={updates[key]}\n")

    tmp_path = env_path + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        f.writelines(rewritten)
    os.replace(tmp_path, env_path)


def _resolve_python_exec(base_dir):
    """Prefer project venv; only fall back to system Python when needed."""
    if sys.platform == "win32":
        venv_python = os.path.join(base_dir, "venv", "Scripts", "python.exe")
        if os.path.exists(venv_python):
            return venv_python
        if shutil.which("python"):
            return "python"
        if shutil.which("py"):
            return "py"
        return sys.executable
    venv_python = os.path.join(base_dir, "venv", "bin", "python")
    if os.path.exists(venv_python):
        return venv_python
    if shutil.which("python3"):
        return "python3"
    if shutil.which("python"):
        return "python"
    return "python3"


def _check_missing_modules(python_exec, modules):
    """Return a list of missing import modules for the selected interpreter."""
    check_script = (
        "import sys\n"
        f"mods = {modules!r}\n"
        "missing = []\n"
        "for m in mods:\n"
        "    try:\n"
        "        __import__(m)\n"
        "    except Exception:\n"
        "        missing.append(m)\n"
        "print('|'.join(missing))\n"
        "raise SystemExit(1 if missing else 0)\n"
    )
    try:
        result = subprocess.run(
            [python_exec, "-c", check_script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
        )
    except Exception:
        return list(modules)

    if result.returncode == 0:
        return []
    payload = (result.stdout or "").strip()
    if not payload:
        return list(modules)
    return [m for m in payload.split("|") if m]


def _bootstrap_requirements(python_exec, base_dir, emit):
    """Install project requirements into the selected interpreter."""
    req_path = os.path.join(base_dir, "requirements.txt")
    if not os.path.exists(req_path):
        emit("\nERROR: requirements.txt not found. Cannot auto-install dependencies.\n")
        return False

    emit(f"\nMissing dependencies detected. Installing from requirements.txt using: {python_exec}\n")
    try:
        proc = subprocess.Popen(
            [python_exec, "-m", "pip", "install", "-r", req_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            cwd=base_dir,
        )
        for line in iter(proc.stdout.readline, ""):
            emit(line)
        proc.stdout.close()
        proc.wait()
        if proc.returncode != 0:
            emit(f"\nERROR: Dependency installation failed with exit code {proc.returncode}.\n")
            return False
        emit("\nDependency installation completed successfully.\n")
        return True
    except Exception as e:
        emit(f"\nERROR: Failed to install dependencies: {e}\n")
        return False


def _ensure_runtime_ready(python_exec, base_dir, required_modules, emit):
    missing = _check_missing_modules(python_exec, required_modules)
    if not missing:
        return True

    emit(f"\nMissing Python modules for {python_exec}: {', '.join(missing)}\n")
    if not _bootstrap_requirements(python_exec, base_dir, emit):
        return False

    missing_after = _check_missing_modules(python_exec, required_modules)
    if missing_after:
        emit(f"\nERROR: Modules still missing after install: {', '.join(missing_after)}\n")
        return False
    return True


def _run_json_probe(python_exec, base_dir, script_source):
    """Run a small Python probe in the target interpreter and parse JSON output."""
    try:
        result = subprocess.run(
            [python_exec, "-c", script_source],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=base_dir,
            timeout=20,
        )
    except Exception as e:
        return {
            "state": "yellow",
            "text": "Probe setup needed",
            "tip": f"Could not execute probe with '{python_exec}': {e}. Install dependencies or configure Python path.",
        }

    output = (result.stdout or "").strip().splitlines()
    if not output:
        err = (result.stderr or "").strip()
        return {
            "state": "yellow",
            "text": "Probe setup needed",
            "tip": err or f"Probe returned no output (exit {result.returncode}).",
        }

    last_line = output[-1].strip()
    try:
        payload = json.loads(last_line)
    except Exception:
        err = (result.stderr or "").strip()
        return {
            "state": "yellow",
            "text": "Probe setup needed",
            "tip": err or last_line,
        }

    if not isinstance(payload, dict):
        return {
            "state": "yellow",
            "text": "Probe setup needed",
            "tip": "Probe returned invalid payload.",
        }
    return payload

class ScraperWorker(QThread):
    output_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)
    
    def __init__(
        self,
        min_delay_seconds=None,
        max_delay_seconds=None,
        scraper_mode=None,
        selected_disciplines=None,
        connections_csv_path=None,
        max_profiles=None,
        max_runtime_minutes=None,
    ):
        super().__init__()
        self.process = None
        self._is_stopped = False
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.scraper_mode = (scraper_mode or "search").strip().lower()
        self.selected_disciplines = list(selected_disciplines or [])
        self.connections_csv_path = (connections_csv_path or "").strip()
        self.max_profiles = max_profiles
        self.max_runtime_minutes = max_runtime_minutes

    def run(self):
        base_dir = get_base_dir()
        python_exec = _resolve_python_exec(base_dir)
                
        scraper_script = os.path.join(base_dir, 'scraper', 'main.py')
        exit_code = 1
        
        try:
            self.output_signal.emit(f"Launching using: {python_exec}\n")
            required_modules = ["selenium", "bs4", "pandas", "requests", "dotenv"]
            if not _ensure_runtime_ready(python_exec, base_dir, required_modules, self.output_signal.emit):
                self.output_signal.emit(
                    "\nSetup failed. Please run build/setup again or install dependencies manually with:\n"
                    f"{python_exec} -m pip install -r requirements.txt\n"
                )
                exit_code = 2
                return

            popen_kwargs = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "stdin": subprocess.PIPE,
                "text": True,
                "encoding": "utf-8",
                "errors": "replace",
                "bufsize": 1,
                "cwd": base_dir,
                "env": os.environ.copy(),
            }

            if self.min_delay_seconds is not None:
                popen_kwargs["env"]["GUI_MIN_DELAY_SECONDS"] = str(int(self.min_delay_seconds))
            if self.max_delay_seconds is not None:
                popen_kwargs["env"]["GUI_MAX_DELAY_SECONDS"] = str(int(self.max_delay_seconds))
            if self.scraper_mode:
                popen_kwargs["env"]["GUI_SCRAPER_MODE"] = self.scraper_mode
            popen_kwargs["env"]["GUI_SEARCH_DISCIPLINES"] = ",".join(self.selected_disciplines)
            if self.connections_csv_path:
                popen_kwargs["env"]["INPUT_CSV"] = self.connections_csv_path
            if self.max_profiles is not None:
                popen_kwargs["env"]["GUI_MAX_PROFILES"] = str(int(self.max_profiles))
            if self.max_runtime_minutes is not None:
                popen_kwargs["env"]["GUI_MAX_RUNTIME_MINUTES"] = str(int(self.max_runtime_minutes))
            popen_kwargs["env"]["PYTHONUTF8"] = "1"
            popen_kwargs["env"]["PYTHONIOENCODING"] = "utf-8"

            if sys.platform == "win32":
                popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                popen_kwargs["start_new_session"] = True

            self.process = subprocess.Popen(
                [python_exec, scraper_script],
                **popen_kwargs
            )
            
            for line in iter(self.process.stdout.readline, ''):
                if self._is_stopped:
                    break
                self.output_signal.emit(line)
                
            self.process.stdout.close()
            self.process.wait()
            exit_code = self.process.returncode
            
            if not self._is_stopped:
                self.output_signal.emit(f"\nProcess finished with exit code {self.process.returncode}\n")
        except Exception as e:
            self.output_signal.emit(f"\nError starting scraper: {e}\n")
            exit_code = 1
        finally:
            self.finished_signal.emit(exit_code)
            
    def stop(self, immediate=False):
        proc = self.process
        if not proc or proc.poll() is not None:
            return

        if not immediate:
            try:
                if proc.stdin:
                    self.output_signal.emit("\nGraceful stop requested (after current profile).\n")
                    proc.stdin.write("exit\n")
                    proc.stdin.flush()
                    try:
                        proc.wait(timeout=2)
                        return
                    except subprocess.TimeoutExpired:
                        self.output_signal.emit(
                            "\nNo graceful listener response detected. Escalating to termination...\n"
                        )
            except Exception as e:
                self.output_signal.emit(f"\nCould not request graceful stop, falling back to immediate stop: {e}\n")

        self._is_stopped = True

        self.output_signal.emit("\nSent termination signal...\n")
        try:
            if sys.platform == "win32":
                proc.terminate()
            else:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except Exception:
                    proc.terminate()

            proc.wait(timeout=8)
            return
        except subprocess.TimeoutExpired:
            self.output_signal.emit("\nProcess did not exit in time. Forcing kill...\n")
        except Exception as e:
            self.output_signal.emit(f"\nError while terminating scraper: {e}\n")

        try:
            if sys.platform == "win32":
                proc.kill()
            else:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    proc.kill()
            proc.wait(timeout=5)
        except Exception as e:
            self.output_signal.emit(f"\nUnable to force kill scraper process: {e}\n")

class DatabaseWorker(QThread):
    output_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)
    
    def __init__(self):
        super().__init__()
        self.process = None

    def run(self):
        base_dir = get_base_dir()
        python_exec = _resolve_python_exec(base_dir)
                
        db_script = os.path.join(base_dir, 'backend', 'database.py')
        
        try:
            self.output_signal.emit(f"Launching Database Upload: {python_exec} backend/database.py\n")
            required_modules = ["mysql.connector", "pandas", "dotenv"]
            if not _ensure_runtime_ready(python_exec, base_dir, required_modules, self.output_signal.emit):
                self.output_signal.emit(
                    "\nSetup failed. Please run build/setup again or install dependencies manually with:\n"
                    f"{python_exec} -m pip install -r requirements.txt\n"
                )
                self.finished_signal.emit(2)
                return

            env = os.environ.copy()
            env.setdefault("DB_RUN_SEED", "1")
            env.setdefault("DB_RUN_MAINTENANCE", "0")
            self.process = subprocess.Popen(
                [python_exec, db_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                cwd=base_dir,
                env=env,
            )
            
            for line in iter(self.process.stdout.readline, ''):
                self.output_signal.emit(line)
                
            self.process.stdout.close()
            self.process.wait()
            self.output_signal.emit(f"\nDatabase Upload finished with exit code {self.process.returncode}\n")
        except Exception as e:
            self.output_signal.emit(f"\nError starting database upload: {e}\n")
        finally:
            code = self.process.returncode if self.process else 1
            self.finished_signal.emit(code)


class GeocodeWorker(QThread):
    output_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)

    def __init__(self):
        super().__init__()
        self.process = None

    def run(self):
        base_dir = get_base_dir()
        python_exec = _resolve_python_exec(base_dir)

        geocode_script = os.path.join(base_dir, 'backend', 'geocoding.py')

        try:
            self.output_signal.emit(f"Launching Geocoding: {python_exec} backend/geocoding.py --mode missing\n")
            required_modules = ["requests", "mysql.connector", "dotenv"]
            if not _ensure_runtime_ready(python_exec, base_dir, required_modules, self.output_signal.emit):
                self.output_signal.emit(
                    "\nSetup failed. Please run build/setup again or install dependencies manually with:\n"
                    f"{python_exec} -m pip install -r requirements.txt\n"
                )
                self.finished_signal.emit(2)
                return

            self.process = subprocess.Popen(
                [python_exec, geocode_script, '--mode', 'missing'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                cwd=base_dir,
            )

            for line in iter(self.process.stdout.readline, ''):
                self.output_signal.emit(line)

            self.process.stdout.close()
            self.process.wait()
            self.output_signal.emit(f"\nGeocoding finished with exit code {self.process.returncode}\n")
        except Exception as e:
            self.output_signal.emit(f"\nError starting geocoding: {e}\n")
        finally:
            code = self.process.returncode if self.process else 1
            self.finished_signal.emit(code)


class InstallDepsWorker(QThread):
    output_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)

    def __init__(self):
        super().__init__()

    def run(self):
        base_dir = get_base_dir()
        python_exec = _resolve_python_exec(base_dir)
        self.output_signal.emit(f"\nInstalling dependencies using: {python_exec}\n")

        ok = _bootstrap_requirements(python_exec, base_dir, self.output_signal.emit)
        if not ok:
            self.finished_signal.emit(1)
            return

        # Verify the core modules needed by workers.
        required = [
            "selenium",
            "bs4",
            "pandas",
            "requests",
            "dotenv",
            "mysql.connector",
            "groq",
            "bcrypt",
        ]
        missing = _check_missing_modules(python_exec, required)
        if missing:
            self.output_signal.emit(f"\nERROR: Still missing after install: {', '.join(missing)}\n")
            self.finished_signal.emit(1)
            return

        self.output_signal.emit("\nAll required dependencies are installed.\n")
        self.finished_signal.emit(0)


class UpdateNowWorker(QThread):
    output_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)

    def __init__(self):
        super().__init__()

    def run(self):
        base_dir = get_base_dir()
        if sys.platform == "win32":
            script_path = os.path.join(base_dir, "build_windows_app.bat")
            cmd = ["cmd", "/c", script_path]
        else:
            script_path = os.path.join(base_dir, "build_mac_app.command")
            cmd = ["bash", script_path]

        if not os.path.exists(script_path):
            self.output_signal.emit(
                f"\nUpdate build script not found at {script_path}. Use download-page update instead.\n"
            )
            self.finished_signal.emit(2)
            return

        self.output_signal.emit(f"\nRunning local update script: {' '.join(cmd)}\n")
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                cwd=base_dir,
            )
            for line in iter(proc.stdout.readline, ""):
                self.output_signal.emit(line)
            proc.stdout.close()
            proc.wait()
            self.finished_signal.emit(proc.returncode)
        except Exception as e:
            self.output_signal.emit(f"\nError running update script: {e}\n")
            self.finished_signal.emit(1)

class FlagManagerDialog(QDialog):
    COLUMN_ORDER = [
        ("Review", None),
        ("Run", None),
        ("Date Scraped", None),
        ("Name", "name"),
        ("Reason Flagged", "reason"),
        ("Profile URL", "linkedin_url"),
        ("Grad Year", "grad_year"),
        ("Degree", "degree"),
        ("Major", "major"),
        ("Discipline", "discipline"),
        ("Job Title", "current_job_title"),
        ("Company", "company"),
        ("Location", "location"),
        ("Exp2 Title", "exp2_title"),
        ("Exp2 Company", "exp2_company"),
        ("Exp3 Title", "exp3_title"),
        ("Exp3 Company", "exp3_company"),
    ]

    FIELD_TO_COLUMN = {field: idx for idx, (_, field) in enumerate(COLUMN_ORDER) if field}

    def __init__(self, base_dir, parent=None):
        super().__init__(parent)
        self.base_dir = base_dir
        self.setWindowTitle("Manage Review Flags")
        self.resize(1300, 620)
        
        self.csv_path = os.path.join(self.base_dir, 'scraper', 'output', 'UNT_Alumni_Data.csv')
        self.txt_path = os.path.join(self.base_dir, 'scraper', 'output', 'flagged_for_review.txt')
        self.runs = []
        self._loading_table = False
        
        self.init_ui()
        self.load_runs()
        self.load_data()
        

    def init_ui(self):
        layout = QVBoxLayout(self)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Scope:"))
        self.run_combo = QComboBox()
        self.run_combo.currentIndexChanged.connect(self.load_data)
        controls.addWidget(self.run_combo, stretch=1)

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.load_runs)
        controls.addWidget(self.refresh_btn)

        self.bypass_edit_cb = QCheckBox("Bypass mode (edit any field)")
        self.bypass_edit_cb.setToolTip("When disabled, editable fields are restricted by flag reason.")
        self.bypass_edit_cb.toggled.connect(self._apply_edit_rules)
        controls.addWidget(self.bypass_edit_cb)

        self.open_profile_btn = QPushButton("Open Selected Profile")
        self.open_profile_btn.clicked.connect(self.open_selected_profile)
        controls.addWidget(self.open_profile_btn)

        layout.addLayout(controls)
        
        self.table = QTableWidget(0, len(self.COLUMN_ORDER))
        self.table.setHorizontalHeaderLabels([label for label, _ in self.COLUMN_ORDER])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        self.table.itemChanged.connect(self._handle_item_changed)
        
        layout.addWidget(self.table)
        
        btn_layout = QHBoxLayout()
        self.sel_all_btn = QPushButton("Select All")
        self.clear_all_btn = QPushButton("Accept All (Clear Flags)")
        self.save_btn = QPushButton("Save Flags + Edits")
        
        self.sel_all_btn.clicked.connect(self.select_all)
        self.clear_all_btn.clicked.connect(self.clear_all)
        self.save_btn.clicked.connect(self.save_flags)
        
        btn_layout.addWidget(self.sel_all_btn)
        btn_layout.addWidget(self.clear_all_btn)
        btn_layout.addStretch()
        self.unsaved_label = QLabel("Unsaved edits: 0")
        btn_layout.addWidget(self.unsaved_label)
        btn_layout.addWidget(self.save_btn)
        
        layout.addLayout(btn_layout)

    def _normalize_url(self, value):
        return (value or "").strip().rstrip('/').lower()

    def _get_connection(self):
        backend_dir = os.path.join(self.base_dir, "backend")
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)
        from database import get_connection
        return get_connection()

    def load_runs(self):
        self.run_combo.blockSignals(True)
        self.run_combo.clear()
        self.runs = []

        self.run_combo.addItem("All flagged (legacy file)", None)
        try:
            conn = self._get_connection()
            try:
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(
                        """
                        SELECT id, run_uuid, scraper_email, scraper_mode, status,
                               profiles_scraped, started_at
                        FROM scrape_runs
                        ORDER BY started_at DESC
                        LIMIT 50
                        """
                    )
                    rows = cur.fetchall() or []
            except Exception:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, run_uuid, scraper_email, scraper_mode, status,
                               profiles_scraped, started_at
                        FROM scrape_runs
                        ORDER BY started_at DESC
                        LIMIT 50
                        """
                    )
                    fetched = cur.fetchall() or []
                    cols = [d[0] for d in (cur.description or [])]
                    rows = []
                    for r in fetched:
                        if isinstance(r, dict):
                            rows.append(r)
                        else:
                            rows.append(dict(zip(cols, r)))
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

            self.runs = rows
            for row in rows:
                started_at = str(row.get("started_at") or "")[:16].replace("T", " ")
                label = (
                    f"Run #{row.get('id')} | {started_at} | {row.get('scraper_mode') or 'unknown'} | "
                    f"{row.get('scraper_email') or 'unknown'} | scraped {row.get('profiles_scraped') or 0}"
                )
                self.run_combo.addItem(label, row.get("id"))
        except Exception:
            pass

        self.run_combo.blockSignals(False)
        self.load_data()

    def _fetch_alumni_row(self, conn, linkedin_url):
        query = (
            "SELECT first_name, last_name, scraped_at, grad_year, degree, major, discipline, "
            "current_job_title, company, location, exp2_title, exp2_company, exp3_title, exp3_company, "
            "linkedin_url FROM alumni WHERE linkedin_url = %s OR linkedin_url = %s LIMIT 1"
        )
        params = (linkedin_url, f"{linkedin_url}/")
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return row or {}
        except Exception:
            sqlite_query = query.replace("%s", "?")
            with conn.cursor() as cur:
                cur.execute(sqlite_query, params)
                row = cur.fetchone()
                if not row:
                    return {}
                if isinstance(row, dict):
                    return row
                cols = [d[0] for d in (cur.description or [])]
                return dict(zip(cols, row))

    def _load_rows_from_db(self, run_id):
        rows = []
        conn = self._get_connection()
        try:
            if run_id:
                flags_sql = """
                    SELECT rf.scrape_run_id, rf.linkedin_url, rf.reason, sr.started_at
                    FROM scrape_run_flags rf
                    LEFT JOIN scrape_runs sr ON sr.id = rf.scrape_run_id
                    WHERE rf.scrape_run_id = %s
                    ORDER BY rf.created_at DESC
                """
                params = (int(run_id),)
            else:
                flags_sql = """
                    SELECT rf.scrape_run_id, rf.linkedin_url, rf.reason, sr.started_at
                    FROM scrape_run_flags rf
                    LEFT JOIN scrape_runs sr ON sr.id = rf.scrape_run_id
                    ORDER BY rf.created_at DESC
                    LIMIT 500
                """
                params = tuple()

            try:
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(flags_sql, params)
                    flags = cur.fetchall() or []
            except Exception:
                with conn.cursor() as cur:
                    cur.execute(flags_sql.replace("%s", "?"), params)
                    fetched = cur.fetchall() or []
                    cols = [d[0] for d in (cur.description or [])]
                    flags = []
                    for r in fetched:
                        if isinstance(r, dict):
                            flags.append(r)
                        else:
                            flags.append(dict(zip(cols, r)))

            for flag in flags:
                url = (flag.get("linkedin_url") or "").strip().rstrip('/')
                if not url:
                    continue
                alumni = self._fetch_alumni_row(conn, url)
                first = (alumni.get("first_name") or "").strip()
                last = (alumni.get("last_name") or "").strip()
                name = f"{first} {last}".strip()
                started_at = str(flag.get("started_at") or "")[:16].replace("T", " ")
                rows.append({
                    "run": f"#{flag.get('scrape_run_id') or '-'} {started_at}".strip(),
                    "date": str(alumni.get("scraped_at") or "")[:10] if alumni.get("scraped_at") else "Unknown",
                    "name": name,
                    "reason": (flag.get("reason") or "Needs Manual Review").strip() or "Needs Manual Review",
                    "linkedin_url": url,
                    "grad_year": "" if alumni.get("grad_year") is None else str(alumni.get("grad_year")),
                    "degree": alumni.get("degree") or "",
                    "major": alumni.get("major") or "",
                    "discipline": alumni.get("discipline") or "",
                    "current_job_title": alumni.get("current_job_title") or "",
                    "company": alumni.get("company") or "",
                    "location": alumni.get("location") or "",
                    "exp2_title": alumni.get("exp2_title") or "",
                    "exp2_company": alumni.get("exp2_company") or "",
                    "exp3_title": alumni.get("exp3_title") or "",
                    "exp3_company": alumni.get("exp3_company") or "",
                })
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return rows

    def _load_rows_from_legacy_file(self):
        flagged_urls = {}
        if os.path.exists(self.txt_path):
            with open(self.txt_path, 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.split('#')
                    url = parts[0].strip().rstrip('/')
                    if not url:
                        continue
                    reason = "Needs Manual Review"
                    if len(parts) > 1:
                        reason = parts[-1].strip() or reason
                    flagged_urls[self._normalize_url(url)] = reason

        rows = []
        if os.path.exists(self.csv_path):
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = (row.get('linkedin_url', '') or row.get('profile_url', '')).strip().rstrip('/')
                    if not url:
                        continue
                    url_key = self._normalize_url(url)
                    if url_key not in flagged_urls:
                        continue
                    rows.append({
                        "run": "legacy",
                        "date": (row.get('scraped_at', '') or '')[:10] or "Unknown",
                        "name": f"{row.get('first', '')} {row.get('last', '')}".strip(),
                        "reason": flagged_urls.get(url_key, "Needs Manual Review"),
                        "linkedin_url": url,
                        "grad_year": str(row.get('grad_year', '') or row.get('graduation_year', '') or ''),
                        "degree": row.get('degree', '') or '',
                        "major": row.get('major', '') or '',
                        "discipline": row.get('discipline', '') or '',
                        "current_job_title": row.get('title', '') or row.get('job_title', '') or '',
                        "company": row.get('company', '') or '',
                        "location": row.get('location', '') or '',
                        "exp2_title": row.get('exp_2_title', '') or row.get('exp2_title', '') or '',
                        "exp2_company": row.get('exp_2_company', '') or row.get('exp2_company', '') or '',
                        "exp3_title": row.get('exp_3_title', '') or row.get('exp3_title', '') or '',
                        "exp3_company": row.get('exp_3_company', '') or row.get('exp3_company', '') or '',
                    })

        def get_date(entry):
            try:
                return datetime.fromisoformat((entry.get("date") or "").replace('Z', '+00:00'))
            except Exception:
                return datetime.min

        rows.sort(key=get_date, reverse=True)
        return rows

    def _set_item(self, row_idx, col_idx, text, editable=False):
        item_text = "" if text is None else str(text)
        item = QTableWidgetItem(item_text)
        item.setData(Qt.ItemDataRole.UserRole + 1, item_text)
        item.setToolTip(item_text)
        if not editable:
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.table.setItem(row_idx, col_idx, item)
        return item

    def _refresh_unsaved_count(self):
        changed = 0
        editable_cols = set(self.FIELD_TO_COLUMN.values())
        for row in range(self.table.rowCount()):
            for col in editable_cols:
                item = self.table.item(row, col)
                if not item:
                    continue
                original = str(item.data(Qt.ItemDataRole.UserRole + 1) or "")
                current = item.text() or ""
                if current != original:
                    changed += 1
        if hasattr(self, "unsaved_label"):
            self.unsaved_label.setText(f"Unsaved edits: {changed}")

    def _handle_item_changed(self, item):
        if self._loading_table or not item:
            return
        col = item.column()
        if col not in set(self.FIELD_TO_COLUMN.values()):
            return

        original = str(item.data(Qt.ItemDataRole.UserRole + 1) or "")
        current = item.text() or ""
        if current != original:
            item.setBackground(Qt.GlobalColor.yellow)
        else:
            item.setBackground(Qt.GlobalColor.transparent)
        self._refresh_unsaved_count()

    def load_data(self):
        self._loading_table = True
        run_id = self.run_combo.currentData() if hasattr(self, "run_combo") else None
        rows = []
        if run_id:
            try:
                rows = self._load_rows_from_db(run_id)
            except Exception:
                rows = self._load_rows_from_legacy_file()
        else:
            rows = self._load_rows_from_legacy_file()

        self.table.setRowCount(len(rows))
        for i, row in enumerate(rows):
            cb = QCheckBox()
            cb.setChecked(True)
            cb_widget = QWidget()
            cb_layout = QHBoxLayout(cb_widget)
            cb_layout.addWidget(cb)
            cb_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cb_layout.setContentsMargins(0, 0, 0, 0)
            self.table.setCellWidget(i, 0, cb_widget)

            self._set_item(i, 1, row.get("run", "-"), editable=False)
            self._set_item(i, 2, row.get("date", "Unknown"), editable=False)
            self._set_item(i, 3, row.get("name", ""), editable=False)
            self._set_item(i, 4, row.get("reason", "Needs Manual Review"), editable=False)
            url_item = self._set_item(i, 5, row.get("linkedin_url", ""), editable=False)
            url_item.setData(Qt.ItemDataRole.UserRole, row)

            for field in [
                "grad_year", "degree", "major", "discipline", "current_job_title", "company", "location",
                "exp2_title", "exp2_company", "exp3_title", "exp3_company"
            ]:
                col = self.FIELD_TO_COLUMN[field]
                self._set_item(i, col, row.get(field, ""), editable=True)

        self._apply_edit_rules()
        self._refresh_unsaved_count()
        self._loading_table = False

    def _reason_target_fields(self, reason):
        text = (reason or "").strip().lower()
        if "missing grad year" in text:
            return {"grad_year"}
        if "missing degree/major" in text:
            return {"degree", "major", "discipline"}
        if "missing company but job title present for experience 2" in text:
            return {"exp2_company"}
        if "missing job title but company present for experience 2" in text:
            return {"exp2_title"}
        if "missing company but job title present for experience 3" in text:
            return {"exp3_company"}
        if "missing job title but company present for experience 3" in text:
            return {"exp3_title"}
        if "missing company but job title present" in text:
            return {"company"}
        if "missing job title but company present" in text:
            return {"current_job_title"}
        return set()

    def _apply_edit_rules(self):
        bypass = self.bypass_edit_cb.isChecked() if hasattr(self, "bypass_edit_cb") else False
        editable_fields = [
            "grad_year", "degree", "major", "discipline", "current_job_title", "company", "location",
            "exp2_title", "exp2_company", "exp3_title", "exp3_company"
        ]
        for row in range(self.table.rowCount()):
            reason_item = self.table.item(row, 4)
            reason = reason_item.text() if reason_item else ""
            allowed = set(editable_fields) if bypass else self._reason_target_fields(reason)
            for field in editable_fields:
                col = self.FIELD_TO_COLUMN[field]
                item = self.table.item(row, col)
                if not item:
                    continue
                base_flags = item.flags() | Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled
                if field in allowed:
                    item.setFlags(base_flags | Qt.ItemFlag.ItemIsEditable)
                else:
                    item.setFlags(base_flags & ~Qt.ItemFlag.ItemIsEditable)

    def open_selected_profile(self):
        row = self.table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Open Profile", "Select a row first.")
            return
        url_item = self.table.item(row, 5)
        url = (url_item.text() if url_item else "").strip()
        if not url:
            QMessageBox.warning(self, "Open Profile", "No profile URL found for selected row.")
            return
        webbrowser.open(url)
            
    def select_all(self):
        for i in range(self.table.rowCount()):
            cb_widget = self.table.cellWidget(i, 0)
            if cb_widget: cb_widget.layout().itemAt(0).widget().setChecked(True)
            
    def clear_all(self):
        for i in range(self.table.rowCount()):
            cb_widget = self.table.cellWidget(i, 0)
            if cb_widget: cb_widget.layout().itemAt(0).widget().setChecked(False)
            
    def save_flags(self):
        flagged = []
        updates_by_url = {}
        editable_fields = [
            "grad_year", "degree", "major", "discipline", "current_job_title", "company", "location",
            "exp2_title", "exp2_company", "exp3_title", "exp3_company"
        ]

        for i in range(self.table.rowCount()):
            cb_widget = self.table.cellWidget(i, 0)
            if cb_widget and cb_widget.layout().itemAt(0).widget().isChecked():
                url = self.table.item(i, 5).text()
                reason = (self.table.item(i, 4).text() if self.table.item(i, 4) else "").strip() or "Needs Manual Review"
                
                line = f"{url} # {reason}"
                flagged.append(line)

            url_item = self.table.item(i, 5)
            if not url_item:
                continue
            row_data = url_item.data(Qt.ItemDataRole.UserRole) or {}
            url = (url_item.text() or "").strip()
            if not url:
                continue

            field_updates = {}
            for field in editable_fields:
                col = self.FIELD_TO_COLUMN[field]
                item = self.table.item(i, col)
                if not item:
                    continue
                new_val = (item.text() or "").strip()
                old_val = str(row_data.get(field, "") or "").strip()
                if field == "grad_year":
                    if not new_val:
                        parsed = None
                    else:
                        try:
                            parsed = int(float(new_val))
                        except ValueError:
                            QMessageBox.critical(self, "Invalid Grad Year", f"Row {i+1}: grad year must be an integer.")
                            return
                    old_parsed = None
                    if old_val:
                        try:
                            old_parsed = int(old_val)
                        except Exception:
                            old_parsed = None
                    if parsed != old_parsed:
                        field_updates[field] = parsed
                else:
                    if new_val != old_val:
                        field_updates[field] = new_val or None

            if field_updates:
                updates_by_url[url] = field_updates
                
        os.makedirs(os.path.dirname(self.txt_path), exist_ok=True)
        with open(self.txt_path, 'w', encoding='utf-8') as f:
            for item in flagged:
                f.write(f"{item}\n")

        updated_rows = 0
        if updates_by_url:
            try:
                conn = self._get_connection()
                try:
                    for url, updates in updates_by_url.items():
                        columns = list(updates.keys())
                        assignments = ", ".join([f"{col} = %s" for col in columns])
                        values = [updates[col] for col in columns]
                        sql = f"UPDATE alumni SET {assignments} WHERE linkedin_url = %s OR linkedin_url = %s"
                        params = tuple(values + [url, f"{url}/"])
                        try:
                            with conn.cursor() as cur:
                                cur.execute(sql, params)
                                updated_rows += cur.rowcount or 0
                        except Exception:
                            sqlite_sql = sql.replace("%s", "?")
                            with conn.cursor() as cur:
                                cur.execute(sqlite_sql, params)
                                updated_rows += cur.rowcount or 0
                    conn.commit()
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception as e:
                QMessageBox.warning(self, "Partial Save", f"Flags were saved, but DB updates failed: {e}")
                
        QMessageBox.information(
            self,
            "Saved",
            f"Saved {len(flagged)} profiles for review.\nApplied {len(updates_by_url)} edited profile update(s) to DB.",
        )
        self.accept()


import dotenv

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Scraper Settings")
        self.setMinimumSize(620, 440)
        self.resize(780, 560)
        self.setStyleSheet(
            "QCheckBox::indicator { width: 18px; height: 18px; }"
            "QTabBar::tab { padding: 8px 14px; color: #1b2430; background: #e9eef6; border: 1px solid #cfd8e6; border-bottom: none; }"
            "QTabBar::tab:selected { background: #1659a6; color: #ffffff; font-weight: 600; }"
            "QTabWidget::pane { border: 1px solid #cfd8e6; top: -1px; }"
        )
        
        self.base_dir = get_base_dir()
        self.env_path = os.path.join(self.base_dir, '.env')
        self.env_values = _safe_load_dotenv_values(self.env_path)
        
        self._fields = {}
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        self.tabs = QTabWidget()
        self.tabs.setTabBarAutoHide(False)
        layout.addWidget(self.tabs)
        
        self._create_settings_tabs()
        
        action_bar = QFrame()
        action_bar.setStyleSheet("QFrame { background: #f3f6fb; border: 1px solid #d3deed; border-radius: 8px; }")
        btn_layout = QHBoxLayout(action_bar)
        btn_layout.setContentsMargins(10, 8, 10, 8)
        btn_layout.setSpacing(8)
        reset_btn = QPushButton("Reset to Defaults")
        reset_btn.clicked.connect(self.reset_to_defaults)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)

        save_only_btn = QPushButton("Save")
        save_only_btn.clicked.connect(self.save_only)
        save_only_btn.setStyleSheet("background-color: #1659a6; color: white; font-weight: bold; padding: 6px;")
        
        save_test_btn = QPushButton("Save && Test Connection")
        save_test_btn.clicked.connect(self.save_and_test)
        save_test_btn.setStyleSheet("background-color: #2e6f40; color: white; font-weight: bold; padding: 6px;")
        
        btn_layout.addWidget(reset_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(cancel_btn)
        btn_layout.addWidget(save_only_btn)
        btn_layout.addWidget(save_test_btn)
        
        layout.addWidget(action_bar)
        self.setSizeGripEnabled(True)
        self._apply_screen_bounds()

    def _apply_screen_bounds(self):
        try:
            host = self.windowHandle().screen() if self.windowHandle() else None
            screen = host or QApplication.primaryScreen()
            if not screen:
                return
            available = screen.availableGeometry()
            max_w = max(640, available.width() - 40)
            max_h = max(460, available.height() - 40)
            self.setMaximumSize(max_w, max_h)

            target_w = min(max_w, max(680, int(available.width() * 0.62)))
            target_h = min(max_h, max(500, int(available.height() * 0.62)))
            self.resize(target_w, target_h)
        except Exception:
            pass

    def showEvent(self, event):
        super().showEvent(event)
        self._apply_screen_bounds()

    def _create_settings_tabs(self):
        builders = [
            ("Credentials", self.create_credentials_tab),
            ("Scraper", self.create_scraper_tab),
            ("Database", self.create_database_tab),
        ]
        for tab_name, builder in builders:
            try:
                builder()
            except Exception as e:
                f = self._create_tab(tab_name)
                err = QLabel(f"Could not load this tab: {e}")
                err.setWordWrap(True)
                err.setStyleSheet("color: #B00020; font-weight: 600;")
                f.addRow(QLabel("Status"), err)

    def _add_field(self, form_layout, key, label_text, ftype, default_val, tooltip, is_required=False, is_password=False):
        lbl = QLabel(f"{label_text} *" if is_required else label_text)
        lbl.setToolTip(tooltip)
        if is_required:
            lbl.setStyleSheet("color: #0056b3; font-weight: bold;")
        else:
            lbl.setStyleSheet("color: #2f3b4b;")
        
        lbl_widget = QWidget()
        lbl_h = QHBoxLayout(lbl_widget)
        lbl_h.setContentsMargins(0, 0, 5, 0)
        lbl_h.addWidget(lbl)
        lbl_h.addStretch()
        
        row_widget = QWidget()
        row_layout = QHBoxLayout(row_widget)
        row_layout.setContentsMargins(0, 0, 0, 0)
        
        current_val = self.env_values.get(key, "")
        inp_widget = None
        
        if ftype == bool:
            inp_widget = QCheckBox()
            inp_widget.setToolTip(tooltip)
            inp_widget.setMinimumWidth(28)
            inp_widget.setMinimumHeight(24)
            # Strict falsy checks for bools
            val_str = current_val.lower() if current_val else str(default_val).lower()
            inp_widget.setChecked(val_str in ("true", "1", "yes"))
            row_layout.addWidget(inp_widget)
            row_layout.addStretch()
        else:
            inp_widget = QLineEdit()
            inp_widget.setPlaceholderText(f"Default: {default_val}")
            inp_widget.setMinimumWidth(320)
            inp_widget.setToolTip(tooltip)
            if current_val:
                inp_widget.setText(current_val)
            
            if is_password:
                inp_widget.setEchoMode(QLineEdit.EchoMode.Password)
                row_layout.addWidget(inp_widget)
            else:
                row_layout.addWidget(inp_widget)
                
        form_layout.addRow(lbl_widget, row_widget)
        
        self._fields[key] = {
            'widget': inp_widget,
            'type': ftype,
            'default': default_val
        }

    def _create_tab(self, name):
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        form = QFormLayout(container)
        form.setSpacing(10)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        form.setHorizontalSpacing(14)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        form.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        scroll.setWidget(container)
        self.tabs.addTab(scroll, name)
        return form

    def create_credentials_tab(self):
        f = self._create_tab("Credentials")
        self._add_field(f, "LINKEDIN_EMAIL", "LinkedIn Email", str, "", "Email address used for LinkedIn login. Required for headless or expired sessions.", True)
        self._add_field(f, "LINKEDIN_PASSWORD", "LinkedIn Password", str, "", "Password for LinkedIn. Only needed if cookies expire.", True, True)

    def create_scraper_tab(self):
        f = self._create_tab("Scraper")
        self._add_field(
            f,
            "HEADLESS",
            "Headless Mode",
            bool,
            False,
            "Runs Chrome in the background so you can keep working, but it is often more detectable and harder to troubleshoot login/challenge flows. Non-headless may occasionally bring browser focus while scraping.",
            False,
        )
        self._add_field(f, "USE_COOKIES", "Use Cookies", bool, True, "Attempt to inject previous session cookies to bypass manual login.", False)
        self._add_field(
            f,
            "SCRAPER_DEBUG",
            "Debug",
            bool,
            False,
            "Enable verbose scraper logs (including defense navigator details).",
            False,
        )
        self._add_field(
            f,
            "SCRAPER_DEBUG_HTML",
            "Debug HTML Dumps",
            bool,
            False,
            "Save scraped HTML dumps on extraction failures for inspection.",
            False,
        )
        self._add_field(
            f,
            "SCRAPE_RESUME_MAX_AGE_DAYS",
            "Resume Max Age",
            int,
            7,
            "In search mode, this keeps track of recent page/progress state so restarts avoid repeating pages you already scraped.",
            False,
        )
        self._add_field(
            f,
            "USE_GROQ",
            "Use Groq",
            bool,
            True,
            "Use Groq for extraction. Recommended ON because HTML-only extraction is less reliable for some profile formats.",
            False,
        )
        self._add_field(
            f,
            "GROQ_API_KEY",
            "Groq API Key",
            str,
            "",
            "Optional but strongly recommended for stable extraction quality.",
            False,
            True,
        )
        self._add_field(
            f,
            "GEOCODE_USE_GROQ_FALLBACK",
            "Groq Geocode Fallback",
            bool,
            True,
            "When geocoding returns unknown, ask Groq once to normalize location text before retrying geocode.",
            False,
        )
        hint = QLabel(
            "If USE_GROQ is enabled and key is empty, scraping can still run but may be less reliable on some profiles."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #2f3b4b;")
        f.addRow(QLabel("Notes"), hint)

    def create_database_tab(self):
        f = self._create_tab("Database")
        self._add_field(f, "MYSQLHOST", "MySQL Host", str, "localhost", "Database server address (e.g., your.domain.com or localhost).", True)
        self._add_field(f, "MYSQLUSER", "MySQL User", str, "root", "Database username.", True)
        self._add_field(f, "MYSQLPASSWORD", "MySQL Password", str, "", "Database password.", True, True)
        self._add_field(f, "MYSQL_DATABASE", "Database Name", str, "linkedinhelper", "Primary catalog/schema name.", True)
        self._add_field(f, "MYSQLPORT", "MySQL Port", int, 3306, "Database connection port (defaults to 3306).", True)

    def reset_to_defaults(self):
        for key, field in self._fields.items():
            w = field['widget']
            d = field['default']
            t = field['type']
            if t == bool:
                w.setChecked(bool(d))
            else:
                w.setText("")
        QMessageBox.information(self, "Reset", "Fields reset. Click Save to apply changes to .env.")

    def _collect_updates(self):
        updates = {}
        for key, field in self._fields.items():
            w = field['widget']
            t = field['type']
            
            if t == bool:
                updates[key] = "true" if w.isChecked() else "false"
            else:
                val = w.text().strip()
                if val:
                    try:
                        t(val)
                    except ValueError:
                        QMessageBox.warning(self, "Validation Error", f"Invalid value for {key}: must be valid {t.__name__}")
                        w.setFocus()
                        return None
                    updates[key] = val
                else:
                    # User wants it empty (or fallback to default dynamically later)
                    # We can remove the key or leave it empty
                    updates[key] = ""

        return updates

    def save_only(self):
        updates = self._collect_updates()
        if updates is None:
            return

        update_env_many(updates)
        QMessageBox.information(self, "Saved", "Settings saved to .env successfully.")
        self.accept()

    def save_and_test(self):
        updates = self._collect_updates()
        if updates is None:
            return

        update_env_many(updates)

        # Safely try MySQL
        try:
            import mysql.connector
            port_val = updates.get("MYSQLPORT", "")
            port_val = int(port_val) if port_val else 3306
            conn = mysql.connector.connect(
                host=updates.get("MYSQLHOST", ""),
                user=updates.get("MYSQLUSER", ""),
                password=updates.get("MYSQLPASSWORD", ""),
                database=updates.get("MYSQL_DATABASE", ""),
                port=port_val,
                connection_timeout=5
            )
            with conn.cursor(buffered=True) as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            conn.close()
            QMessageBox.information(self, "Success", "Settings saved and database test succeeded.")
            self.accept()
        except Exception as e:
            QMessageBox.warning(
                self,
                "Database Test Failed",
                f"Settings were saved, but database test failed:\n{e}\n\n"
                "You can continue using local/offline modes and retry connection test later.",
            )


class ScraperApp(QMainWindow):
    MODE_INFO_TEXT = {
        "search": (
            "Search Mode: Finds new alumni from LinkedIn UNT search results. "
            "Uses selected discipline filters when chosen."
        ),
        "review": (
            "Review Mode: Re-scrapes URLs listed in scraper/output/flagged_for_review.txt "
            "to repair incomplete records."
        ),
        "update": (
            "Update Mode: Re-scrapes alumni already in the database who are due for refresh. "
            "Queue order is oldest last_updated first, newest last."
        ),
        "connections": (
            "Connections Mode: Scrapes profiles from a LinkedIn Connections.csv export file."
        ),
    }

    def _mode_info_for(self, mode):
        return self.MODE_INFO_TEXT.get(mode, "Select a mode to see details.")

    def __init__(self):
        super().__init__()
        self.setWindowTitle("UNT Alumni Scraper")
        self.resize(850, 650)
        
        base_dir = get_base_dir()
        icon_path = os.path.join(base_dir, 'frontend', 'public', 'assets', 'unt-logo.png')
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
            QApplication.setWindowIcon(QIcon(icon_path))
            
        self.worker = None
        self.db_worker = None
        self.geocode_worker = None
        self.install_worker = None
        self.update_worker = None
        self._pending_upload_after_geocode = False
        self._manual_intervention_needed = False
        self._manual_intervention_reason = ""
        self._suggest_restart_headed = False
        self._run_started_at = None
        self._run_metrics = {}
        self._history_rows = []
        self._gui_file_path = os.path.abspath(__file__)
        self._gui_file_mtime = self._safe_get_mtime(self._gui_file_path)
        self._pending_gui_reload = False
        self._about_version_text = "local"
        self._about_last_updated_text = "unknown"
        self._update_status_text = "Local build mode"
        self._update_available = False
        self._latest_version = "local"
        self._update_url = ""
        self._cloud_status_cache = None
        self._geo_status_cache = None
        self._missing_module_prompt_shown = False
        self._live_tracker_timer = None
        self._tracker_refresh_pending = False
        self.init_ui()

    def _apply_modern_style(self):
        self.setStyleSheet("""
            QWidget {
                background: #f4f7fb;
                color: #1b2430;
                font-family: 'SF Pro Text', 'Segoe UI', sans-serif;
                font-size: 12px;
            }
            QGroupBox {
                border: 1px solid #d7e2f0;
                border-radius: 10px;
                margin-top: 8px;
                padding-top: 10px;
                background: #ffffff;
                font-weight: 600;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 6px;
            }
            QLineEdit, QComboBox, QTableWidget, QTextEdit {
                border: 1px solid #c9d7ea;
                border-radius: 8px;
                padding: 6px;
                background: #ffffff;
            }
            QPushButton {
                background: #1659a6;
                color: #ffffff;
                border: none;
                border-radius: 8px;
                padding: 8px 12px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: #104a8a;
            }
            QPushButton:disabled {
                background: #a7b8ce;
                color: #ecf1f8;
            }
        """)

    def _safe_get_mtime(self, path):
        try:
            return os.path.getmtime(path)
        except Exception:
            return None

    def _read_git_version_info(self):
        base_dir = get_base_dir()

        if getattr(sys, "frozen", False):
            return "packaged build", "packaged build"

        try:
            short_hash = subprocess.check_output(
                ["git", "-C", base_dir, "rev-parse", "--short", "HEAD"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            commit_time = subprocess.check_output(
                ["git", "-C", base_dir, "show", "-s", "--format=%ci", "HEAD"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            if short_hash:
                return f"git-{short_hash}", commit_time or "unknown"
        except Exception:
            pass

        mtime = self._safe_get_mtime(self._gui_file_path)
        if mtime:
            return "local", datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        return "local", "unknown"

    def _start_remote_update_check(self):
        self._update_status_text = "Local build mode"

    def _refresh_about_metadata(self):
        version_text, last_updated_text = self._read_git_version_info()
        self._about_version_text = version_text
        self._about_last_updated_text = last_updated_text

    def _start_gui_autoreload_watcher(self):
        self._gui_reload_timer = QTimer(self)
        self._gui_reload_timer.setInterval(3000)
        self._gui_reload_timer.timeout.connect(self._check_for_gui_file_update)
        self._gui_reload_timer.start()

    def _start_live_tracker_updates(self):
        if self._live_tracker_timer is None:
            self._live_tracker_timer = QTimer(self)
            self._live_tracker_timer.setInterval(8000)
            self._live_tracker_timer.timeout.connect(self._refresh_live_trackers)
        self._live_tracker_timer.start()

    def _stop_live_tracker_updates(self):
        if self._live_tracker_timer and self._live_tracker_timer.isActive():
            self._live_tracker_timer.stop()

    def _refresh_live_trackers(self):
        if not (self.worker and self.worker.isRunning()):
            self._stop_live_tracker_updates()
            return
        self.refresh_run_history()
        self.refresh_scrape_count()

    def _schedule_tracker_refresh(self, delay_ms=350):
        if self._tracker_refresh_pending:
            return
        self._tracker_refresh_pending = True

        def _run_refresh():
            self._tracker_refresh_pending = False
            self.refresh_run_history()
            self.refresh_scrape_count()

        QTimer.singleShot(delay_ms, _run_refresh)

    def _check_for_gui_file_update(self):
        current_mtime = self._safe_get_mtime(self._gui_file_path)
        if current_mtime is None or self._gui_file_mtime is None:
            self._gui_file_mtime = current_mtime
            return
        if current_mtime <= self._gui_file_mtime:
            return

        self._gui_file_mtime = current_mtime
        self._refresh_about_metadata()

        if self.worker and self.worker.isRunning():
            if not self._pending_gui_reload:
                self._pending_gui_reload = True
                self.append_console("\nAUTO-RELOAD: GUI update detected. Restart will happen after the current scraper run completes.\n")
            return

        self._restart_gui_process()

    def _restart_gui_process(self):
        self._pending_gui_reload = False
        QApplication.instance().quit()
        os.execv(sys.executable, [sys.executable] + sys.argv)

    def _show_about_dialog(self):
        self._refresh_about_metadata()
        QMessageBox.information(
            self,
            "About Scraper GUI",
            (
                f"Version: {self._about_version_text}\n"
                f"Last Updated: {self._about_last_updated_text}\n\n"
                f"Update Status: {self._update_status_text}\n"
                "Update Channel: local build flow\n\n"
                "Auto-reload: enabled (checks every 3 seconds)."
            ),
        )

    def _get_db_connection(self):
        backend_dir = os.path.join(get_base_dir(), "backend")
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)
        from database import get_connection
        return get_connection()

    def refresh_run_history(self):
        if not hasattr(self, "run_history_table"):
            return

        rows = []
        current_email = (self.email_input.text() or "").strip().lower()
        only_current = bool(getattr(self, "run_history_mine_only", None) and self.run_history_mine_only.isChecked())
        if only_current and not current_email:
            self._history_rows = []
            self.run_history_table.setRowCount(0)
            return
        try:
            conn = self._get_db_connection()
            try:
                if only_current and current_email and not (self.worker and self.worker.isRunning()):
                    self._cleanup_stale_runs_from_connection(
                        conn,
                        threshold_hours=2,
                        current_email=current_email,
                    )
                rows = self._fetch_recent_runs_from_connection(conn, current_email, only_current)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            rows = []

        # If cloud query yields nothing, fall back to local SQLite cache to avoid an empty panel.
        if not rows:
            if only_current and current_email and not (self.worker and self.worker.isRunning()):
                self._cleanup_stale_runs_from_local_sqlite(
                    threshold_hours=2,
                    current_email=current_email,
                )
            rows = self._fetch_recent_runs_from_local_sqlite(current_email, only_current)

        self._history_rows = rows
        self.run_history_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            started_raw = str(row.get("started_at") or "")
            started = started_raw[:16].replace("T", " ")
            completed_raw = str(row.get("completed_at") or "")
            status_raw = str(row.get("status") or "").strip().lower()
            status_display = (row.get("status") or "unknown")
            duration = "-"
            try:
                scraped_count = int(row.get("profiles_scraped") or 0)
            except Exception:
                scraped_count = 0
            try:
                if started_raw and completed_raw:
                    dt_start = datetime.fromisoformat(started_raw.replace("Z", "").replace(" ", "T"))
                    dt_end = datetime.fromisoformat(completed_raw.replace("Z", "").replace(" ", "T"))
                    duration = _format_runtime_short((dt_end - dt_start).total_seconds())
                elif started_raw and status_raw == "running":
                    dt_start = datetime.fromisoformat(started_raw.replace("Z", "").replace(" ", "T"))
                    elapsed_seconds = (datetime.now() - dt_start).total_seconds()
                    # Ignore stale copied runs that were never finalized (common in shared backup DBs).
                    if elapsed_seconds > (4 * 3600) and scraped_count == 0:
                        duration = "-"
                        status_display = "stale"
                    else:
                        duration = _format_runtime_short(elapsed_seconds)
            except Exception:
                duration = "-"

            values = [
                started,
                (row.get("scraper_email") or "unknown"),
                (row.get("scraper_mode") or "unknown"),
                str(row.get("profiles_scraped") or 0),
                duration,
                status_display,
                str(row.get("id") or ""),
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.run_history_table.setItem(row_idx, col, item)

    def _parse_history_datetime(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        for candidate in (text.replace("Z", ""), text.replace(" ", "T"), text.replace("Z", "").replace(" ", "T")):
            try:
                return datetime.fromisoformat(candidate)
            except Exception:
                continue
        return None

    def _cleanup_stale_runs_from_connection(self, conn, threshold_hours=2, current_email=None):
        now = datetime.now()
        stale_ids = []
        select_attempts = [
            {"dictionary": True, "placeholder": "%s"},
            {"dictionary": False, "placeholder": "%s"},
            {"dictionary": False, "placeholder": "?"},
        ]

        for attempt in select_attempts:
            try:
                cursor_ctx = conn.cursor(dictionary=True) if attempt["dictionary"] else conn.cursor()
                with cursor_ctx as cur:
                    q = (
                        "SELECT id, started_at, completed_at, status, profiles_scraped "
                        "FROM scrape_runs WHERE LOWER(COALESCE(status, '')) = LOWER({p}) "
                        "AND COALESCE(profiles_scraped, 0) = 0"
                    ).format(p=attempt["placeholder"])
                    params = ["running"]
                    if current_email:
                        q += f" AND LOWER(COALESCE(scraper_email, '')) = LOWER({attempt['placeholder']})"
                        params.append(current_email)
                    cur.execute(q, tuple(params))
                    fetched = cur.fetchall() or []
                    if not attempt["dictionary"]:
                        cols = [d[0] for d in (cur.description or [])]
                        fetched = [dict(zip(cols, row)) if not isinstance(row, dict) else row for row in fetched]

                    for row in fetched:
                        if row.get("completed_at"):
                            continue
                        started = self._parse_history_datetime(row.get("started_at"))
                        if not started:
                            continue
                        age_seconds = (now - started).total_seconds()
                        if age_seconds > (threshold_hours * 3600):
                            try:
                                stale_ids.append(int(row.get("id")))
                            except Exception:
                                pass
                break
            except Exception:
                continue

        stale_ids = sorted(set([sid for sid in stale_ids if sid]))
        if not stale_ids:
            return

        delete_attempts = [
            {"placeholder": "%s"},
            {"placeholder": "?"},
        ]
        for attempt in delete_attempts:
            try:
                ph = attempt["placeholder"]
                with conn.cursor() as cur:
                    if ph == "%s":
                        marks = ",".join(["%s"] * len(stale_ids))
                        cur.execute(f"DELETE FROM scrape_runs WHERE id IN ({marks})", tuple(stale_ids))
                    else:
                        marks = ",".join(["?"] * len(stale_ids))
                        cur.execute(f"DELETE FROM scrape_runs WHERE id IN ({marks})", tuple(stale_ids))
                conn.commit()
                return
            except Exception:
                continue

    def _cleanup_stale_runs_from_local_sqlite(self, threshold_hours=2, current_email=None):
        sqlite_path = os.path.join(get_base_dir(), "backend", "alumni_backup.db")
        if not os.path.exists(sqlite_path):
            return
        conn = None
        try:
            conn = sqlite3.connect(sqlite_path)
            query = (
                "DELETE FROM scrape_runs "
                "WHERE LOWER(COALESCE(status, '')) = 'running' "
                "AND COALESCE(profiles_scraped, 0) = 0 "
                "AND completed_at IS NULL "
                "AND datetime(started_at) <= datetime('now', ?)"
            )
            params = [f"-{int(threshold_hours)} hours"]
            if current_email:
                query += " AND LOWER(COALESCE(scraper_email, '')) = LOWER(?)"
                params.append(current_email)
            conn.execute(query, tuple(params))
            conn.commit()
        except Exception:
            pass
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _fetch_recent_runs_from_connection(self, conn, current_email, only_current):
        base_query = (
            "SELECT id, scraper_email, scraper_mode, status, profiles_scraped, "
            "started_at, completed_at FROM scrape_runs"
        )

        attempts = [
            {"dictionary": True, "placeholder": "%s"},
            {"dictionary": False, "placeholder": "%s"},
            {"dictionary": False, "placeholder": "?"},
        ]

        for attempt in attempts:
            try:
                if attempt["dictionary"]:
                    cursor_ctx = conn.cursor(dictionary=True)
                else:
                    cursor_ctx = conn.cursor()

                with cursor_ctx as cur:
                    query = base_query
                    params = []
                    if only_current and current_email:
                        query += f" WHERE LOWER(scraper_email) = {attempt['placeholder']}"
                        params.append(current_email)
                    query += " ORDER BY started_at DESC LIMIT 25"
                    cur.execute(query, tuple(params))
                    fetched = cur.fetchall() or []
                    if attempt["dictionary"]:
                        return fetched

                    cols = [d[0] for d in (cur.description or [])]
                    normalized = []
                    for row in fetched:
                        if isinstance(row, dict):
                            normalized.append(row)
                        else:
                            normalized.append(dict(zip(cols, row)))
                    return normalized
            except Exception:
                continue

        return []

    def _fetch_recent_runs_from_local_sqlite(self, current_email, only_current):
        sqlite_path = os.path.join(get_base_dir(), "backend", "alumni_backup.db")
        if not os.path.exists(sqlite_path):
            return []

        conn = None
        try:
            conn = sqlite3.connect(sqlite_path)
            conn.row_factory = sqlite3.Row
            query = (
                "SELECT id, scraper_email, scraper_mode, status, profiles_scraped, "
                "started_at, completed_at FROM scrape_runs"
            )
            params = []
            if only_current and current_email:
                query += " WHERE LOWER(scraper_email) = ?"
                params.append(current_email)
            query += " ORDER BY started_at DESC LIMIT 25"
            rows = conn.execute(query, tuple(params)).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _on_history_filter_email_changed(self, _text):
        if getattr(self, "run_history_mine_only", None) and self.run_history_mine_only.isChecked():
            self.refresh_run_history()

    def _fetch_scrape_count_by_person_from_connection(self, conn, only_current=True, current_email=None):
        """Fetch scrape counts grouped by scraper email from scrape run history."""
        attempts = [
            {"dictionary": True, "placeholder": "%s"},
            {"dictionary": False, "placeholder": "%s"},
            {"dictionary": False, "placeholder": "?"},
        ]

        for attempt in attempts:
            try:
                if attempt["dictionary"]:
                    cursor_ctx = conn.cursor(dictionary=True)
                else:
                    cursor_ctx = conn.cursor()

                with cursor_ctx as cur:
                    query = (
                        "SELECT COALESCE(scraper_email, 'unknown') AS email, "
                        "COALESCE(SUM(COALESCE(profiles_scraped, 0)), 0) AS count "
                        "FROM scrape_runs "
                    )
                    params = []
                    if only_current and current_email:
                        query += f"WHERE LOWER(COALESCE(scraper_email, '')) = {attempt['placeholder']} "
                        params.append(current_email)
                    query += "GROUP BY COALESCE(scraper_email, 'unknown') ORDER BY count DESC, email ASC LIMIT 100"
                    
                    cur.execute(query, tuple(params))
                    fetched = cur.fetchall() or []
                    if attempt["dictionary"]:
                        return fetched

                    cols = [d[0] for d in (cur.description or [])]
                    normalized = []
                    for row in fetched:
                        if isinstance(row, dict):
                            normalized.append(row)
                        else:
                            normalized.append(dict(zip(cols, row)))
                    return normalized
            except Exception:
                continue

        return []

    def _fetch_scrape_count_by_person_from_local_sqlite(self, only_current=True, current_email=None):
        """Fetch scrape counts grouped by scraper email from local scrape run history."""
        sqlite_path = os.path.join(get_base_dir(), "backend", "alumni_backup.db")
        if not os.path.exists(sqlite_path):
            return []

        conn = None
        try:
            conn = sqlite3.connect(sqlite_path)
            conn.row_factory = sqlite3.Row
            query = (
                "SELECT COALESCE(scraper_email, 'unknown') AS email, "
                "COALESCE(SUM(COALESCE(profiles_scraped, 0)), 0) AS count FROM scrape_runs "
            )
            params = []
            if only_current and current_email:
                query += "WHERE LOWER(COALESCE(scraper_email, '')) = ? "
                params.append(current_email)
            query += "GROUP BY COALESCE(scraper_email, 'unknown') ORDER BY count DESC, email ASC LIMIT 100"
            rows = conn.execute(query, tuple(params)).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def refresh_scrape_count(self):
        """Refresh the scrape count by scraper email table."""
        if not hasattr(self, "scrape_count_table"):
            return

        rows = []
        current_email = (self.email_input.text() or "").strip().lower()

        try:
            conn = self._get_db_connection()
            try:
                rows = self._fetch_scrape_count_by_person_from_connection(conn, only_current=False)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            rows = []

        # If cloud query yields nothing, fall back to local SQLite
        if not rows:
            rows = self._fetch_scrape_count_by_person_from_local_sqlite(only_current=False)

        self.scrape_count_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            email = str(row.get("email") or "Unknown").strip()
            try:
                count = int(row.get("count") or 0)
            except Exception:
                count = 0

            email_item = QTableWidgetItem(email)
            email_item.setFlags(email_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.scrape_count_table.setItem(row_idx, 0, email_item)

            count_item = QTableWidgetItem(str(count))
            count_item.setFlags(count_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.scrape_count_table.setItem(row_idx, 1, count_item)

    def _reset_run_metrics(self):
        self._run_metrics = {
            "user": "",
            "mode": "",
            "scraped_count": 0,
            "run_duration": "",
            "flagged_count": 0,
            "review_path": "scraper/output/flagged_for_review.txt",
            "cloud_success": 0,
            "cloud_fail": 0,
            "geocode_success": 0,
            "geocode_fail": 0,
            "geocode_unknown": 0,
            "unknown_locations": "",
        }

    def _set_status_badge(self, label_widget, state, text, tooltip):
        color_map = {
            "green": "#1B5E20",
            "yellow": "#B26A00",
            "red": "#B00020",
            "gray": "#5F6368",
        }
        color = color_map.get(state, color_map["gray"])
        label_widget.setText(f"● {text}")
        label_widget.setStyleSheet(f"color: {color}; font-weight: 600;")
        label_widget.setToolTip(tooltip)

    def _probe_cloud_status(self):
        base_dir = get_base_dir()
        python_exec = _resolve_python_exec(base_dir)
        script = (
            "import json, os, sys\n"
            "base = os.getcwd()\n"
            "backend = os.path.join(base, 'backend')\n"
            "if backend not in sys.path:\n"
            "    sys.path.insert(0, backend)\n"
            "if os.getenv('DISABLE_DB', '0') == '1':\n"
            "    print(json.dumps({'state':'gray','text':'Cloud DB disabled','tip':'DISABLE_DB=1 in .env, so cloud DB checks are intentionally bypassed.'}))\n"
            "    raise SystemExit(0)\n"
            "try:\n"
            "    from database import get_connection, get_direct_mysql_connection\n"
            "except ModuleNotFoundError as e:\n"
            "    print(json.dumps({'state':'yellow','text':'Cloud DB setup needed','tip':f'Missing Python dependency: {e}. Click Install Dependencies in the main window.'}))\n"
            "    raise SystemExit(0)\n"
            "except Exception as e:\n"
            "    print(json.dumps({'state':'red','text':'Cloud DB unavailable','tip':f'Database module failed to load: {type(e).__name__}: {e}'}))\n"
            "    raise SystemExit(0)\n"
            "try:\n"
            "    conn = get_direct_mysql_connection()\n"
            "    try:\n"
            "        cur = conn.cursor()\n"
            "        cur.execute('SELECT COUNT(*) FROM alumni')\n"
            "        row = cur.fetchone()\n"
            "        total = int(row[0]) if row else 0\n"
            "    finally:\n"
            "        try:\n"
            "            conn.close()\n"
            "        except Exception:\n"
            "            pass\n"
            "    print(json.dumps({'state':'green','text':'Cloud DB connected','tip':f'Cloud probe succeeded (alumni rows: {total}).'}))\n"
            "except ModuleNotFoundError as e:\n"
            "    print(json.dumps({'state':'yellow','text':'Cloud DB setup needed','tip':f'Missing Python dependency: {e}. Click Install Dependencies in the main window.'}))\n"
            "except Exception as e:\n"
            "    try:\n"
            "        conn = get_connection()\n"
            "        try:\n"
            "            if conn.__class__.__name__ == 'SQLiteConnectionWrapper':\n"
            "                print(json.dumps({'state':'yellow','text':'Cloud DB offline (SQLite fallback active)','tip':'Cloud probe failed; app is writing locally via SQLite fallback.'}))\n"
            "                raise SystemExit(0)\n"
            "        finally:\n"
            "            try:\n"
            "                conn.close()\n"
            "            except Exception:\n"
            "                pass\n"
            "    except Exception:\n"
            "        pass\n"
            "    print(json.dumps({'state':'red','text':'Cloud DB unavailable','tip':f'Cloud probe failed: {type(e).__name__}: {e}'}))\n"
        )
        payload = _run_json_probe(python_exec, base_dir, script)
        return payload.get("state", "red"), payload.get("text", "Cloud DB unavailable"), payload.get("tip", "Cloud probe failed")

    def _check_cloud_status(self, force_probe=False):
        if force_probe or self._cloud_status_cache is None:
            self._cloud_status_cache = self._probe_cloud_status()
        return self._cloud_status_cache

    def _probe_geocode_status(self):
        base_dir = get_base_dir()
        python_exec = _resolve_python_exec(base_dir)
        script = (
            "import json, os, sys\n"
            "base = os.getcwd()\n"
            "backend = os.path.join(base, 'backend')\n"
            "if backend not in sys.path:\n"
            "    sys.path.insert(0, backend)\n"
            "try:\n"
            "    from geocoding import geocode_location_with_status\n"
            "except ModuleNotFoundError as e:\n"
            "    print(json.dumps({'state':'yellow','text':'Geocoding setup needed','tip':f'Missing Python dependency: {e}. Click Install Dependencies in the main window.'}))\n"
            "    raise SystemExit(0)\n"
            "except Exception as e:\n"
            "    print(json.dumps({'state':'red','text':'Geocoding unavailable','tip':f'Geocoding module failed to load: {type(e).__name__}: {e}'}))\n"
            "    raise SystemExit(0)\n"
            "try:\n"
            "    coords, status = geocode_location_with_status('Fort Worth, Texas, United States')\n"
            "    if coords:\n"
            "        lat = float(coords[0])\n"
            "        lon = float(coords[1])\n"
            "        ref_lat, ref_lon = 32.7555, -97.3308\n"
            "        lat_delta = abs(lat - ref_lat)\n"
            "        lon_delta = abs(lon - ref_lon)\n"
            "        if lat_delta <= 0.8 and lon_delta <= 0.8:\n"
            "            print(json.dumps({'state':'green','text':'Geocoding reachable','tip':'Fort Worth probe resolved to expected metro coordinates.'}))\n"
            "        else:\n"
            "            print(json.dumps({'state':'yellow','text':'Geocoding unstable','tip':f'Fort Worth probe resolved but out of expected range (delta lat/lon: {lat_delta:.3f}/{lon_delta:.3f}).'}))\n"
            "    elif status in {'network_error', 'parse_error'}:\n"
            "        print(json.dumps({'state':'yellow','text':'Geocoding unstable','tip':'Network/API issue while validating Fort Worth geocode probe.'}))\n"
            "    else:\n"
            "        print(json.dumps({'state':'red','text':'Geocoding unavailable','tip':'Fort Worth geocode probe failed to resolve.'}))\n"
            "except ModuleNotFoundError as e:\n"
            "    print(json.dumps({'state':'yellow','text':'Geocoding setup needed','tip':f'Missing Python dependency: {e}. Click Install Dependencies in the main window.'}))\n"
            "except Exception as e:\n"
            "    print(json.dumps({'state':'red','text':'Geocoding unavailable','tip':f'Geocode probe failed: {type(e).__name__}: {e}'}))\n"
        )
        payload = _run_json_probe(python_exec, base_dir, script)
        return payload.get("state", "red"), payload.get("text", "Geocoding unavailable"), payload.get("tip", "Geocode probe failed")

    def _check_geocode_status(self, force_probe=False):
        if force_probe or self._geo_status_cache is None:
            self._geo_status_cache = self._probe_geocode_status()
        return self._geo_status_cache

    def refresh_preflight_status(self, force_cloud_probe=False, force_geo_probe=False):
        cloud_state, cloud_text, cloud_tip = self._check_cloud_status(force_probe=force_cloud_probe)
        geo_state, geo_text, geo_tip = self._check_geocode_status(force_probe=force_geo_probe)
        self._set_status_badge(self.cloud_status_label, cloud_state, cloud_text, cloud_tip)
        self._set_status_badge(self.geo_status_label, geo_state, geo_text, geo_tip)


    def open_settings(self):
        dialog = SettingsDialog(self)
        if dialog.exec():
            # Reload environment to sync any changes made in the dialog
            self.load_settings_from_env()
            self.refresh_preflight_status(force_cloud_probe=True, force_geo_probe=True)
            
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        self._refresh_about_metadata()
        
        # Left Panel (Settings)
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        left_layout.setSpacing(8)
        
        
        # Settings Button
        self.settings_btn = QPushButton("⚙️ Settings")
        self.settings_btn.setStyleSheet("padding: 8px; font-weight: bold; font-size: 14px; margin-bottom: 5px; background-color: #1659a6; color: white; border: none; border-radius: 8px;")
        self.settings_btn.clicked.connect(self.open_settings)
        left_layout.addWidget(self.settings_btn)

        # 1. Credentials Group
        cred_group = QGroupBox("LinkedIn Credentials")
        cred_layout = QGridLayout()
        
        cred_layout.addWidget(QLabel("Email:"), 0, 0)
        self.email_input = QLineEdit()
        self.email_input.textChanged.connect(self._on_history_filter_email_changed)
        cred_layout.addWidget(self.email_input, 0, 1)
        
        cred_layout.addWidget(QLabel("Password:"), 1, 0)
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        cred_layout.addWidget(self.password_input, 1, 1)
        
        cred_group.setLayout(cred_layout)
        left_layout.addWidget(cred_group)

        status_group = QGroupBox("Preflight Status")
        status_layout = QVBoxLayout()
        self.cloud_status_label = QLabel("● Checking cloud DB...")
        self.geo_status_label = QLabel("● Checking geocoding...")
        status_layout.addWidget(self.cloud_status_label)
        status_layout.addWidget(self.geo_status_label)
        self.refresh_status_btn = QPushButton("Refresh Status")
        self.refresh_status_btn.clicked.connect(lambda: self.refresh_preflight_status(force_cloud_probe=True, force_geo_probe=True))
        status_layout.addWidget(self.refresh_status_btn)
        status_group.setLayout(status_layout)
        left_layout.addWidget(status_group)
        
        # 2. Target Profile Options
        target_group = QGroupBox("Scraping Mode & Targets")
        target_layout = QGridLayout()
        
        target_layout.addWidget(QLabel("Mode:"), 0, 0)
        mode_row = QHBoxLayout()
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["search", "review", "update", "connections"])
        mode_row.addWidget(self.mode_combo)
        
        self.help_btn = QPushButton("?")
        self.help_btn.setFixedWidth(30)
        self.help_btn.clicked.connect(self.show_help)
        mode_row.addWidget(self.help_btn)
        
        self.manage_flags_btn = QPushButton("Manage Flags")
        self.manage_flags_btn.clicked.connect(self.open_flag_manager)
        mode_row.addWidget(self.manage_flags_btn)
        
        target_layout.addLayout(mode_row, 0, 1)

        self.mode_info_label = QLabel(self._mode_info_for("search"))
        self.mode_info_label.setWordWrap(True)
        self.mode_info_label.setStyleSheet("color: #2e4a62; font-size: 12px;")
        target_layout.addWidget(self.mode_info_label, 1, 0, 1, 2)
        
        # Connections file picker
        self.csv_path_label = QLabel("Connections CSV:")
        target_layout.addWidget(self.csv_path_label, 2, 0)
        
        csv_row = QHBoxLayout()
        self.csv_path_input = QLineEdit()
        self.csv_path_input.setPlaceholderText("Browse for Connections.csv...")
        csv_row.addWidget(self.csv_path_input)
        
        self.browse_csv_btn = QPushButton("Browse")
        self.browse_csv_btn.clicked.connect(self.browse_csv)
        csv_row.addWidget(self.browse_csv_btn)
        
        target_layout.addLayout(csv_row, 2, 1)
        
        self.disciplines_label = QLabel("Disciplines:")
        target_layout.addWidget(self.disciplines_label, 3, 0, Qt.AlignmentFlag.AlignTop)
        
        self.disc_widget = QWidget()
        disc_layout = QGridLayout(self.disc_widget)
        disc_layout.setContentsMargins(0,0,0,0)
        self.discs = {}
        discipline_options = [
            ("software", "Software/Data/AI/Cyber", "Software, Data, AI & Cybersecurity"),
            ("embedded", "Embedded/Electrical/Hardware", "Embedded, Electrical & Hardware Engineering"),
            ("mechanical", "Mechanical/Manufacturing", "Mechanical Engineering & Manufacturing"),
            ("construction", "Construction/Eng Mgmt", "Construction & Engineering Management"),
            ("biomedical", "Biomedical", "Biomedical Engineering"),
        ]
        disc_layout.setHorizontalSpacing(10)
        disc_layout.setVerticalSpacing(4)
        for idx, (discipline_key, compact_label, full_label) in enumerate(discipline_options):
            cb = QCheckBox(compact_label)
            cb.setToolTip(full_label)
            cb.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            self.discs[discipline_key] = cb
            row = idx // 2
            col = idx % 2
            disc_layout.addWidget(cb, row, col)
        
        target_layout.addWidget(self.disc_widget, 3, 1)
        target_group.setLayout(target_layout)
        left_layout.addWidget(target_group)
        
        # 3. Delays
        delay_group = QGroupBox("Scrape Frequency (Anti-Ban)")
        delay_layout = QGridLayout()
        
        delay_layout.addWidget(QLabel("Preset:"), 0, 0)
        self.delay_combo = QComboBox()
        self.delay_combo.addItems(["Slow (2m - 10m)", "Medium (1m - 3m)", "Fast (15s - 60s)", "Custom"])
        self.delay_combo.setCurrentText("Fast (15s - 60s)")
        self.delay_combo.currentTextChanged.connect(self.on_delay_change)
        delay_layout.addWidget(self.delay_combo, 0, 1, 1, 3)
        
        self.custom_min_label = QLabel("Custom Min (s):")
        delay_layout.addWidget(self.custom_min_label, 1, 0)
        self.min_delay = QLineEdit("15")
        self.min_delay.setEnabled(False)
        delay_layout.addWidget(self.min_delay, 1, 1)
        
        self.custom_max_label = QLabel("Max (s):")
        delay_layout.addWidget(self.custom_max_label, 1, 2)
        self.max_delay = QLineEdit("60")
        self.max_delay.setEnabled(False)
        delay_layout.addWidget(self.max_delay, 1, 3)
        
        delay_group.setLayout(delay_layout)
        left_layout.addWidget(delay_group)
        
        # 4. Limits
        limit_group = QGroupBox("Auto-Stop Limits")
        limit_layout = QGridLayout()
        
        limit_layout.addWidget(QLabel("Stop after (Profiles):"), 0, 0)
        self.max_profiles = QLineEdit("0")
        self.max_profiles.setToolTip("0 for infinite")
        limit_layout.addWidget(self.max_profiles, 0, 1)
        
        limit_layout.addWidget(QLabel("Stop after Time:"), 1, 0)
        time_widget = QWidget()
        time_layout = QHBoxLayout(time_widget)
        time_layout.setContentsMargins(0,0,0,0)
        self.hours_input = QLineEdit("2")
        self.hours_input.setFixedWidth(40)
        self.mins_input = QLineEdit("0")
        self.mins_input.setFixedWidth(40)
        time_layout.addWidget(self.hours_input)
        time_layout.addWidget(QLabel("hrs"))
        time_layout.addWidget(self.mins_input)
        time_layout.addWidget(QLabel("mins"))
        limit_layout.addWidget(time_widget, 1, 1)
        
        limit_group.setLayout(limit_layout)
        left_layout.addWidget(limit_group)
        
        # Connect Buttons
        btn_layout = QHBoxLayout()
        self.start_btn = QPushButton("Start Scraper")
        self.start_btn.setMinimumHeight(40)
        self.start_btn.clicked.connect(self.start_scraper)
        
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setMinimumHeight(40)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.stop_scraper)
        
        btn_layout.addWidget(self.start_btn)
        btn_layout.addWidget(self.stop_btn)
        left_layout.addLayout(btn_layout)

        stop_options_layout = QHBoxLayout()
        self.stop_after_profile_btn = QPushButton("Stop After Profile")
        self.stop_after_profile_btn.setMinimumHeight(36)
        self.stop_after_profile_btn.setVisible(False)
        self.stop_after_profile_btn.clicked.connect(self.request_stop_after_profile)
        stop_options_layout.addWidget(self.stop_after_profile_btn)

        self.stop_immediately_btn = QPushButton("Stop Now")
        self.stop_immediately_btn.setMinimumHeight(36)
        self.stop_immediately_btn.setStyleSheet("background-color: #b3261e; color: white; font-weight: 700;")
        self.stop_immediately_btn.setVisible(False)
        self.stop_immediately_btn.clicked.connect(self.request_stop_immediately)
        stop_options_layout.addWidget(self.stop_immediately_btn)
        left_layout.addLayout(stop_options_layout)
        
        sync_btn_layout = QHBoxLayout()

        self.geocode_btn = QPushButton("Backfill Geocode (Optional)")
        self.geocode_btn.setMinimumHeight(40)
        self.geocode_btn.setStyleSheet("background-color: #2E7D32; color: white; font-weight: bold;")
        self.geocode_btn.setToolTip("Optional: re-geocode existing records in the database.")
        self.geocode_btn.clicked.connect(self.run_geocode)

        self.upload_db_btn = QPushButton("Upload to Database")
        self.upload_db_btn.setMinimumHeight(40)
        self.upload_db_btn.setStyleSheet("background-color: #005A9C; color: white; font-weight: bold;")
        self.upload_db_btn.clicked.connect(self.upload_to_db)

        self.install_deps_btn = QPushButton("Install Dependencies")
        self.install_deps_btn.setMinimumHeight(40)
        self.install_deps_btn.setStyleSheet("background-color: #6C757D; color: white; font-weight: bold;")
        self.install_deps_btn.setToolTip("Install Python dependencies from requirements.txt")
        self.install_deps_btn.clicked.connect(self.install_dependencies)

        sync_btn_layout.addWidget(self.geocode_btn)
        sync_btn_layout.addWidget(self.upload_db_btn)
        sync_btn_layout.addWidget(self.install_deps_btn)
        left_layout.addLayout(sync_btn_layout)
        left_layout.addStretch()
        
        left_panel.setMinimumWidth(440)
        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        left_scroll.setWidget(left_panel)
        
        # Right Panel (Console)
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        header_row = QHBoxLayout()
        console_label = QLabel("Console Output")
        header_row.addWidget(console_label)
        header_row.addStretch()
        self.about_btn = QPushButton("i")
        self.about_btn.setFixedSize(26, 26)
        self.about_btn.setToolTip("Version and update info")
        self.about_btn.clicked.connect(self._show_about_dialog)
        header_row.addWidget(self.about_btn)
        right_layout.addLayout(header_row)
        
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4; font-family: Consolas, monospace;")
        right_layout.addWidget(self.console)

        stdin_row = QHBoxLayout()
        stdin_label = QLabel("Terminal Input")
        stdin_row.addWidget(stdin_label)
        self.stdin_input = QLineEdit()
        self.stdin_input.setPlaceholderText("Type input for scraper prompts and press Enter...")
        self.stdin_input.setEnabled(True)
        self.stdin_input.returnPressed.connect(self.send_terminal_input)
        stdin_row.addWidget(self.stdin_input)
        self.stdin_send_btn = QPushButton("Send")
        self.stdin_send_btn.setEnabled(True)
        self.stdin_send_btn.clicked.connect(self.send_terminal_input)
        stdin_row.addWidget(self.stdin_send_btn)
        right_layout.addLayout(stdin_row)

        # ══════════════════════════════════════════════════════════════
        # Tab widget for "Scrape Count" and "Session History"
        # ══════════════════════════════════════════════════════════════
        history_tabs = QTabWidget()
        
        # ───────────────────────────────────────────────────────────────
        # TAB 1: Scrape Count (DEFAULT)
        # ───────────────────────────────────────────────────────────────
        scrape_count_tab = QWidget()
        scrape_count_layout = QVBoxLayout()
        
        scrape_count_controls = QHBoxLayout()
        scrape_count_controls.addStretch()
        self.refresh_scrape_count_btn = QPushButton("Refresh Counts")
        self.refresh_scrape_count_btn.clicked.connect(self.refresh_scrape_count)
        scrape_count_controls.addWidget(self.refresh_scrape_count_btn)
        scrape_count_layout.addLayout(scrape_count_controls)
        
        self.scrape_count_table = QTableWidget(0, 2)
        self.scrape_count_table.setHorizontalHeaderLabels(["Scraper Email", "People Scraped"])
        self.scrape_count_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.scrape_count_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.scrape_count_table.setMinimumHeight(200)
        scrape_count_layout.addWidget(self.scrape_count_table)
        scrape_count_tab.setLayout(scrape_count_layout)
        
        # ───────────────────────────────────────────────────────────────
        # TAB 2: Session History
        # ───────────────────────────────────────────────────────────────
        session_history_tab = QWidget()
        history_layout = QVBoxLayout()
        history_controls = QHBoxLayout()
        self.run_history_mine_only = QCheckBox("Only current email")
        self.run_history_mine_only.stateChanged.connect(self.refresh_run_history)
        self.run_history_mine_only.setChecked(True)
        history_controls.addWidget(self.run_history_mine_only)
        history_controls.addStretch()
        self.refresh_history_btn = QPushButton("Refresh History")
        self.refresh_history_btn.clicked.connect(self.refresh_run_history)
        history_controls.addWidget(self.refresh_history_btn)
        history_layout.addLayout(history_controls)

        self.run_history_table = QTableWidget(0, 7)
        self.run_history_table.setHorizontalHeaderLabels([
            "Started",
            "User",
            "Mode",
            "Scraped",
            "Duration",
            "Status",
            "Run ID",
        ])
        self.run_history_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.run_history_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.run_history_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.run_history_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.run_history_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self.run_history_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        self.run_history_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        self.run_history_table.setMinimumHeight(200)
        history_layout.addWidget(self.run_history_table)
        session_history_tab.setLayout(history_layout)
        
        # Add both tabs to the tab widget
        history_tabs.addTab(scrape_count_tab, "Scrape Count")
        history_tabs.addTab(session_history_tab, "Session History")
        history_tabs.setCurrentIndex(0)  # Default to "Scrape Count"
        
        right_layout.addWidget(history_tabs)
        
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(left_scroll)
        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([520, 860])
        main_layout.addWidget(splitter)
        
        # Hook mode change to layout visibility
        self.mode_combo.currentTextChanged.connect(self.on_mode_change)
        self.on_mode_change(self.mode_combo.currentText())
        self.on_delay_change(self.delay_combo.currentText())
        
        # Auto-load existing config
        self.load_settings_from_env()
        self._reset_run_metrics()
        self.refresh_preflight_status(force_cloud_probe=True, force_geo_probe=True)
        self.refresh_run_history()
        self.refresh_scrape_count()
        self._apply_modern_style()
        self._start_gui_autoreload_watcher()

    def on_mode_change(self, mode):
        self.mode_info_label.setText(self._mode_info_for(mode))
        is_conn = (mode == "connections")
        is_search = (mode == "search")
        is_review = (mode == "review")
        self.csv_path_label.setVisible(is_conn)
        self.csv_path_input.setVisible(is_conn)
        self.browse_csv_btn.setVisible(is_conn)
        self.disciplines_label.setVisible(is_search)
        self.disc_widget.setVisible(is_search)
        self.manage_flags_btn.setVisible(is_review)

    def browse_csv(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Connections.csv", "", "CSV Files (*.csv);;All Files (*)")
        if file_path:
            self.csv_path_input.setText(file_path)

    def load_settings_from_env(self):
        base_dir = get_base_dir()
            
        env_path = os.path.join(base_dir, '.env')
        if not os.path.exists(env_path):
            return

        env_values = _safe_load_dotenv_values(env_path)
        email = str(env_values.get("LINKEDIN_EMAIL", "") or "").strip()
        pwd = str(env_values.get("LINKEDIN_PASSWORD", "") or "").strip()
        if email:
            self.email_input.setText(email)
        if pwd:
            self.password_input.setText(pwd)

        mode = str(env_values.get("GUI_SCRAPER_MODE", "") or "").strip().lower()
        if mode and mode in ["search", "review", "update", "connections"]:
            self.mode_combo.setCurrentText(mode)

        saved_preset = str(env_values.get("GUI_DELAY_PRESET", "") or "").strip()
        valid_presets = {self.delay_combo.itemText(i) for i in range(self.delay_combo.count())}
        if saved_preset and saved_preset in valid_presets:
            self.delay_combo.setCurrentText(saved_preset)

        saved_min = str(env_values.get("GUI_MIN_DELAY_SECONDS", "") or "").strip()
        saved_max = str(env_values.get("GUI_MAX_DELAY_SECONDS", "") or "").strip()
        if saved_min and saved_min.isdigit():
            self.min_delay.setText(saved_min)
        if saved_max and saved_max.isdigit():
            self.max_delay.setText(saved_max)

        saved_max_profiles = str(env_values.get("GUI_MAX_PROFILES", "") or "").strip()
        if saved_max_profiles and saved_max_profiles.isdigit():
            self.max_profiles.setText(saved_max_profiles)

        saved_hours = str(env_values.get("GUI_MAX_RUNTIME_HOURS", "") or "").strip()
        saved_minutes = str(env_values.get("GUI_MAX_RUNTIME_MINUTES", "") or "").strip()
        if saved_hours and saved_hours.isdigit():
            self.hours_input.setText(saved_hours)
        if saved_minutes and saved_minutes.isdigit():
            self.mins_input.setText(saved_minutes)

        for checkbox in self.discs.values():
            checkbox.setChecked(False)
        disciplines_raw = str(env_values.get("GUI_SEARCH_DISCIPLINES", "") or "").strip().lower()
        for token in [d.strip() for d in disciplines_raw.split(",") if d.strip()]:
            if token in self.discs:
                self.discs[token].setChecked(True)

        input_csv = str(env_values.get("INPUT_CSV", "") or "").strip()
        if input_csv:
            self.csv_path_input.setText(input_csv)

        self.on_delay_change(self.delay_combo.currentText())

    def on_delay_change(self, text):
        is_custom = (text == "Custom")
        self.custom_min_label.setVisible(is_custom)
        self.min_delay.setVisible(is_custom)
        self.custom_max_label.setVisible(is_custom)
        self.max_delay.setVisible(is_custom)

        if is_custom:
            self.min_delay.setEnabled(True)
            self.max_delay.setEnabled(True)
        else:
            self.min_delay.setEnabled(False)
            self.max_delay.setEnabled(False)
            if "Slow" in text:
                self.min_delay.setText("120")
                self.max_delay.setText("600")
            elif "Medium" in text:
                self.min_delay.setText("60")
                self.max_delay.setText("180")
            elif "Fast" in text:
                self.min_delay.setText("15")
                self.max_delay.setText("60")

    def _get_effective_delay_range(self):
        preset = (self.delay_combo.currentText() or "").strip()
        if preset.startswith("Slow"):
            return 120, 600
        if preset.startswith("Medium"):
            return 60, 180
        if preset.startswith("Fast"):
            return 15, 60

        try:
            min_d = int(self.min_delay.text())
            max_d = int(self.max_delay.text())
        except ValueError:
            return 60, 180
        return min_d, max_d

    def _get_selected_discipline_aliases(self):
        selected = []
        for alias, checkbox in getattr(self, "discs", {}).items():
            if checkbox.isChecked():
                selected.append(alias)
        return selected

    def append_console(self, text):
        stripped = (text or "").strip()
        summary_idx = stripped.find("SUMMARY|")
        if summary_idx >= 0:
            payload = stripped[summary_idx + len("SUMMARY|"):]
            if "=" in payload:
                key, value = payload.split("=", 1)
                self._run_metrics[key.strip()] = value.strip()
            return

        action_idx = stripped.find("ACTION|")
        if action_idx >= 0:
            payload = stripped[action_idx + len("ACTION|"):]
            if "=" in payload:
                key, value = payload.split("=", 1)
                key = key.strip().lower()
                value = value.strip()
                if key == "manual_intervention_needed" and value == "1":
                    self._manual_intervention_needed = True
                elif key == "reason":
                    self._manual_intervention_reason = value
                elif key == "suggest_restart_headed" and value == "1":
                    self._suggest_restart_headed = True
            return

        line_lower = (text or "").lower()
        if (
            "completed — saved" in line_lower
            or "completed - saved" in line_lower
            or "info persistence: csv updated | cloud db updated | sqlite mirror updated" in line_lower
        ):
            self._schedule_tracker_refresh()

        color = "#D4D4D4"
        if "error" in line_lower or "failed" in line_lower or "critical" in line_lower:
            color = "#FF6B6B"
        elif "warning" in line_lower or "unavailable" in line_lower:
            color = "#FFD166"
        elif "summary|" in line_lower or "run summary" in line_lower:
            color = "#7FDBFF"
        elif "progress |" in line_lower or "success" in line_lower or "finished" in line_lower:
            color = "#8CE99A"

        safe_text = (
            (text or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\n", "<br>")
        )
        self.console.insertHtml(f"<span style='color:{color};'>{safe_text}</span>")
        scrollbar = self.console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

        prompt_hint = self._detect_prompt_hint(stripped)
        if prompt_hint:
            self.console.insertHtml(
                "<span style='color:#FFD166;'>[input] Prompt detected. "
                "Use Terminal Input and press Send.</span><br>"
            )

        if ("modulenotfounderror: no module named" in line_lower) and (not self._missing_module_prompt_shown):
            self._missing_module_prompt_shown = True
            missing_name = "unknown"
            m = re.search(r"No module named ['\"]([^'\"]+)['\"]", text or "")
            if m:
                missing_name = m.group(1)

            choice = QMessageBox.question(
                self,
                "Missing Dependency Detected",
                (
                    f"The scraper failed because module '{missing_name}' is missing.\n\n"
                    "Install all required dependencies now using requirements.txt?"
                ),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.Yes,
            )
            if choice == QMessageBox.StandardButton.Yes:
                self.install_dependencies()

    def _detect_prompt_hint(self, line):
        if not line:
            return ""

        lower = line.lower()
        prompt_tokens = [
            "remove these",
            "[y/n]",
            "[y/n]:",
            "[y/n]",
            "[y/n]:",
            "awaiting input",
            "input required",
        ]
        if any(token.lower() in lower for token in prompt_tokens):
            return "Scraper is waiting for terminal input."

        if lower.endswith(":") and any(k in lower for k in ("choice", "answer", "remove these", "[y/n")):
            return "Scraper is waiting for terminal input."
        return ""

    def _append_gui_summary_block(self):
        if self._run_started_at:
            elapsed_seconds = int((datetime.now() - self._run_started_at).total_seconds())
            runtime = self._run_metrics.get("run_duration") or _format_runtime_short(elapsed_seconds)
        else:
            runtime = self._run_metrics.get("run_duration") or "0m"

        user = self._run_metrics.get("user") or self.email_input.text().strip().lower() or "unknown"
        mode = self._run_metrics.get("mode") or self.mode_combo.currentText()
        scraped = self._run_metrics.get("scraped_count", 0)
        flagged = self._run_metrics.get("flagged_count", 0)
        review_path = self._run_metrics.get("review_path") or "scraper/output/flagged_for_review.txt"
        cloud_success = self._run_metrics.get("cloud_success", 0)
        cloud_fail = self._run_metrics.get("cloud_fail", 0)
        geo_success = self._run_metrics.get("geocode_success", 0)
        geo_fail = self._run_metrics.get("geocode_fail", 0)
        geo_unknown = self._run_metrics.get("geocode_unknown", 0)
        unknown_locations = self._run_metrics.get("unknown_locations") or ""

        lines = [
            "\n================ GUI RUN SUMMARY ================\n",
            f"User: {user}\n",
            f"Mode: {mode}\n",
            f"Scraped Count: {scraped}\n",
            f"Run Duration: {runtime}\n",
            f"Flagged Count: {flagged}\n",
            f"Review Path: {review_path}\n",
            f"Cloud Upload Success/Fail: {cloud_success}/{cloud_fail}\n",
            f"Geocode Success/Fail/Unknown: {geo_success}/{geo_fail}/{geo_unknown}\n",
        ]
        if unknown_locations:
            lines.append(f"Unknown Locations: {unknown_locations}\n")
        lines.append("=================================================\n")
        for line in lines:
            self.append_console(line)

    def show_help(self):
        mode = self.mode_combo.currentText()
        details = [self._mode_info_for(mode)]
        if mode == "connections":
            details.append(
                "\n\nConnections.csv setup:\n"
                "1. Visit: https://www.linkedin.com/mypreferences/d/download-my-data\n"
                "2. Select 'Download larger data archive'.\n"
                "3. Request archive and wait for email.\n"
                "4. Extract the archive and locate Connections.csv.\n"
                "5. Use Browse to select that file in this GUI."
            )
        elif mode == "update":
            details.append(
                "\n\nUpdate mode notes:\n"
                "- Pulls already-scraped alumni from DB using UPDATE_FREQUENCY.\n"
                "- Processes queue in ascending last_updated order.\n"
                "- Best for periodic refresh of existing records."
            )
        elif mode == "review":
            details.append(
                "\n\nReview mode notes:\n"
                "- Uses scraper/output/flagged_for_review.txt.\n"
                "- Best for data-quality repair runs."
            )
        else:
            details.append(
                "\n\nSearch mode notes:\n"
                "- Finds new people from LinkedIn UNT search results.\n"
                "- Discipline filters are optional and only apply in search mode."
            )

        QMessageBox.information(self, f"Mode Info: {mode}", "".join(details))

    def open_flag_manager(self):
        dialog = FlagManagerDialog(get_base_dir(), self)
        dialog.exec()

    def validate_inputs(self):
        base_dir = get_base_dir()
        mode = self.mode_combo.currentText()
        if not self.email_input.text().strip():
            QMessageBox.critical(self, "Missing Email", "LinkedIn email is required so scraper activity can be tracked.")
            return False

        if mode == "review":
            txt_path = os.path.join(base_dir, 'scraper', 'output', 'flagged_for_review.txt')
            if not os.path.exists(txt_path) or os.path.getsize(txt_path) == 0:
                QMessageBox.critical(self, "Missing Data", "Review Mode selected, but 'flagged_for_review.txt' is empty or missing!\n\nPlease use the 'Manage Flags' button to flag profiles first.")
                return False
                
        if mode == "connections":
            csv_path = self.csv_path_input.text() or os.path.join(base_dir, 'Connections.csv')
            if not os.path.exists(csv_path):
                QMessageBox.critical(self, "Missing File", f"Connections file not found at:\n{csv_path}\n\nClick 'Browse' to select your Connections.csv file or click the (?) icon for download instructions.")
                return False
            try:
                import csv
                with open(csv_path, 'r', encoding='utf-8') as f:
                    header = next(csv.reader(f))
                    if not any("url" in c.lower() for c in header):
                        QMessageBox.critical(self, "Invalid Format", "The selected file does not contain a recognizable 'URL' column!\nPlease ensure you downloaded the correct Connections archive from LinkedIn.")
                        return False
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to read CSV: {e}")
                return False

        try:
            min_d = int(self.min_delay.text())
            max_d = int(self.max_delay.text())
        except ValueError:
            QMessageBox.critical(self, "Error", "Delay values must be valid integers.")
            return False

        if min_d < 0 or max_d < 0:
            QMessageBox.critical(self, "Invalid Delay", "Delay values must be non-negative integers.")
            return False

        if max_d < min_d:
            QMessageBox.critical(
                self,
                "Invalid Delay Range",
                "Maximum delay must be greater than or equal to minimum delay."
            )
            return False
            
        if self.delay_combo.currentText() == "Custom":
            if min_d < 15:
                QMessageBox.warning(self, "Warning", "Minimum delay < 15s. This is extremely risky and may lead to a ban.")
            if (max_d - min_d) < 45:
                QMessageBox.warning(self, "Warning", "Delay gap is very tight (under 45s). It's recommended to widen the range to mimic human variance better.")

        # Limit Validations
        try:
            p_limit = int(self.max_profiles.text() or 0)
            h_limit = int(self.hours_input.text() or 0)
            m_limit = int(self.mins_input.text() or 0)
            if p_limit < 0 or h_limit < 0 or m_limit < 0:
                raise ValueError("Negative value")
        except ValueError:
            QMessageBox.critical(self, "Error", "Auto-stop limits (profiles/time) must be valid non-negative integers.")
            return False

        return True

    def toggle_password(self, checked):
        if checked:
            self.password_input.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            self.password_input.setEchoMode(QLineEdit.EchoMode.Password)

    def closeEvent(self, event):
        # Auto-save current selections when closing the app
        self.save_all_settings_to_env()
        event.accept()

    def save_all_settings_to_env(self):
        selected_disciplines = ",".join(self._get_selected_discipline_aliases())
        updates = {
            "LINKEDIN_EMAIL": self.email_input.text().strip().lower(),
            "GUI_SCRAPER_MODE": self.mode_combo.currentText().strip().lower(),
            "GUI_SEARCH_DISCIPLINES": selected_disciplines,
            "GUI_DELAY_PRESET": self.delay_combo.currentText().strip(),
            "GUI_MIN_DELAY_SECONDS": self.min_delay.text().strip() or "0",
            "GUI_MAX_DELAY_SECONDS": self.max_delay.text().strip() or "0",
            "GUI_MAX_PROFILES": self.max_profiles.text().strip() or "0",
            "GUI_MAX_RUNTIME_HOURS": self.hours_input.text().strip() or "0",
            "GUI_MAX_RUNTIME_MINUTES": self.mins_input.text().strip() or "0",
            "INPUT_CSV": self.csv_path_input.text().strip(),
        }
        if self.password_input.text():
            updates["LINKEDIN_PASSWORD"] = self.password_input.text()
        update_env_many(updates)

    def _warn_if_groq_not_ready(self):
        env_path = os.path.join(get_base_dir(), ".env")
        env_values = _safe_load_dotenv_values(env_path)
        use_groq = str(env_values.get("USE_GROQ", "true")).strip().lower() in {"1", "true", "yes", "on"}
        groq_key = str(env_values.get("GROQ_API_KEY", "")).strip()

        if use_groq and groq_key:
            return True

        msg = (
            "Groq is recommended for scraping reliability.\n\n"
            "Some profiles have layouts where HTML-only extraction is less consistent. "
            "Using Groq improves extraction quality for those cases.\n\n"
            "Recommendation:\n"
            "1. Enable 'Use Groq' in Settings > Scraper\n"
            "2. Add GROQ_API_KEY in your .env (Settings currently hides API keys for safety)\n"
            "3. Get a key at https://console.groq.com/keys\n"
        )

        if not use_groq:
            msg += "\nContinue anyway with HTML-only extraction?"
        else:
            msg += "\nContinue anyway without a Groq API key?"

        choice = QMessageBox.question(
            self,
            "Groq Recommended",
            msg,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        return choice == QMessageBox.StandardButton.Yes

    def start_scraper(self):
        if not self.validate_inputs():
            return

        if self.mode_combo.currentText() == "update" and not self._confirm_update_mode_queue():
            return
            
        self.save_all_settings_to_env()

        if not self._warn_if_groq_not_ready():
            return
        
        self.console.clear()
        self._manual_intervention_needed = False
        self._manual_intervention_reason = ""
        self._suggest_restart_headed = False
        self._missing_module_prompt_shown = False
        self._run_started_at = datetime.now()
        self._reset_run_metrics()
        self._run_metrics["user"] = self.email_input.text().strip().lower()
        self._run_metrics["mode"] = self.mode_combo.currentText()
        self.start_btn.setEnabled(False)
        self.geocode_btn.setEnabled(False)
        self.upload_db_btn.setEnabled(False)
        self.install_deps_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.stop_btn.setText("Stop")
        self.stop_after_profile_btn.setVisible(False)
        self.stop_immediately_btn.setVisible(False)

        min_delay_seconds, max_delay_seconds = self._get_effective_delay_range()
        self.append_console(
            f"Using delay range: {min_delay_seconds}s - {max_delay_seconds}s (preset: {self.delay_combo.currentText()})\n"
        )
        selected_disciplines = self._get_selected_discipline_aliases() if self.mode_combo.currentText() == "search" else []
        if selected_disciplines:
            self.append_console(f"Selected disciplines: {', '.join(selected_disciplines)}\n")
        max_profiles = int(self.max_profiles.text() or 0)
        max_runtime_minutes = int((int(self.hours_input.text() or 0) * 60) + int(self.mins_input.text() or 0))
        
        self.worker = ScraperWorker(
            min_delay_seconds=min_delay_seconds,
            max_delay_seconds=max_delay_seconds,
            scraper_mode=self.mode_combo.currentText(),
            selected_disciplines=selected_disciplines,
            connections_csv_path=self.csv_path_input.text().strip(),
            max_profiles=max_profiles,
            max_runtime_minutes=max_runtime_minutes,
        )
        self.worker.output_signal.connect(self.append_console)
        self.worker.finished_signal.connect(self.on_scraper_finished)
        self.worker.start()
        self._start_live_tracker_updates()
        QTimer.singleShot(1500, self.refresh_run_history)
        QTimer.singleShot(2000, self.refresh_scrape_count)

    def run_geocode(self):
        self.save_all_settings_to_env()
        self.console.clear()
        self.start_btn.setEnabled(False)
        self.geocode_btn.setEnabled(False)
        self.upload_db_btn.setEnabled(False)
        self.install_deps_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)

        self.geocode_worker = GeocodeWorker()
        self.geocode_worker.output_signal.connect(self.append_console)
        self.geocode_worker.finished_signal.connect(self.on_geocode_finished)
        self.geocode_worker.start()

    def upload_to_db(self):
        was_pending_after_geocode = self._pending_upload_after_geocode
        if not self._pending_upload_after_geocode:
            choice = QMessageBox.question(
                self,
                "Geocode Before Upload?",
                "Scraping now auto-geocodes each profile. Run optional geocode backfill before upload?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Yes,
            )

            if choice == QMessageBox.StandardButton.Cancel:
                return

            if choice == QMessageBox.StandardButton.Yes:
                self._pending_upload_after_geocode = True
                self.run_geocode()
                return

        self._pending_upload_after_geocode = False
        self.save_all_settings_to_env()
        if not was_pending_after_geocode:
            self.console.clear()
        self.start_btn.setEnabled(False)
        self.geocode_btn.setEnabled(False)
        self.upload_db_btn.setEnabled(False)
        self.install_deps_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        
        self.db_worker = DatabaseWorker()
        self.db_worker.output_signal.connect(self.append_console)
        self.db_worker.finished_signal.connect(self.on_db_finished)
        self.db_worker.start()

    def install_dependencies(self):
        if self.install_worker and self.install_worker.isRunning():
            return

        self.console.clear()
        self.start_btn.setEnabled(False)
        self.geocode_btn.setEnabled(False)
        self.upload_db_btn.setEnabled(False)
        self.install_deps_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)

        self.install_worker = InstallDepsWorker()
        self.install_worker.output_signal.connect(self.append_console)
        self.install_worker.finished_signal.connect(self.on_install_finished)
        self.install_worker.start()

    def run_update_now(self):
        if self.update_worker and self.update_worker.isRunning():
            return

        self.console.clear()
        self.append_console("\nStarting Update Now... this runs the local build script.\n")
        self.start_btn.setEnabled(False)
        self.geocode_btn.setEnabled(False)
        self.upload_db_btn.setEnabled(False)
        self.install_deps_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)

        self.update_worker = UpdateNowWorker()
        self.update_worker.output_signal.connect(self.append_console)
        self.update_worker.finished_signal.connect(self.on_update_now_finished)
        self.update_worker.start()

    def on_update_now_finished(self, exit_code):
        if exit_code == 0:
            choice = QMessageBox.question(
                self,
                "Update Build Completed",
                "Local build/update completed successfully. Restart app now to use latest build artifacts?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.Yes,
            )
            if choice == QMessageBox.StandardButton.Yes:
                self._restart_gui_process()
                return
        elif exit_code == 2:
            QMessageBox.warning(
                self,
                "Update Script Not Found",
                "Local build script was not found. Use 'Open Download Page' for updates.",
            )
        else:
            QMessageBox.warning(
                self,
                "Update Failed",
                "Local update build failed. Check console output for details.",
            )
        self.on_sync_finished()

    def on_install_finished(self, exit_code):
        if exit_code == 0:
            QMessageBox.information(
                self,
                "Dependencies Installed",
                "All required dependencies were installed successfully.",
            )
            self._cloud_status_cache = None
            self._geo_status_cache = None
            self.refresh_preflight_status(force_cloud_probe=True, force_geo_probe=True)
        else:
            QMessageBox.warning(
                self,
                "Install Failed",
                "Dependency installation failed. Check console output for details.",
            )
        self.on_sync_finished()

    def stop_scraper(self):
        if not self.worker:
            return
        self.stop_btn.setText("Choose Stop Mode")
        self.stop_after_profile_btn.setVisible(True)
        self.stop_immediately_btn.setVisible(True)
        self.stop_btn.setEnabled(False)

    def _reset_stop_controls(self):
        self.stop_btn.setEnabled(False)
        self.stop_btn.setText("Stop")
        self.stop_after_profile_btn.setVisible(False)
        self.stop_immediately_btn.setVisible(False)

    def request_stop_after_profile(self):
        if self.worker:
            self.worker.stop(immediate=False)
        self._reset_stop_controls()

    def request_stop_immediately(self):
        if self.worker:
            self.worker.stop(immediate=True)
        self._reset_stop_controls()

    def on_geocode_finished(self, exit_code):
        if exit_code != 0:
            self._pending_upload_after_geocode = False
            self.on_sync_finished()
            return
        if self._pending_upload_after_geocode:
            self.upload_to_db()
            return
        self.on_sync_finished()

    def on_db_finished(self, _exit_code):
        self.on_sync_finished()

    def on_sync_finished(self):
        self.start_btn.setEnabled(True)
        self.geocode_btn.setEnabled(True)
        self.upload_db_btn.setEnabled(True)
        self.install_deps_btn.setEnabled(True)
        self._reset_stop_controls()

    def on_scraper_finished(self):
        self._stop_live_tracker_updates()
        self.start_btn.setEnabled(True)
        self.geocode_btn.setEnabled(True)
        self.upload_db_btn.setEnabled(True)
        self.install_deps_btn.setEnabled(True)
        self._reset_stop_controls()
        self._append_gui_summary_block()
        try:
            cloud_fail = int(self._run_metrics.get("cloud_fail", 0) or 0)
            if cloud_fail > 0:
                self._cloud_status_cache = (
                    "red",
                    "Cloud DB unavailable",
                    f"Cloud upload failed {cloud_fail} time(s) during the latest scrape run.",
                )
        except Exception:
            pass
        self.refresh_preflight_status(force_cloud_probe=False)
        self.refresh_run_history()
        self.refresh_scrape_count()

        if self._manual_intervention_needed:
            reason = self._manual_intervention_reason or "login_or_challenge"
            message = (
                "LinkedIn requires manual intervention before scraping can continue.\n\n"
                f"Reason: {reason}\n\n"
                "You can restart now with headless disabled to complete verification in a visible browser window."
            )
            choice = QMessageBox.question(
                self,
                "Manual Intervention Needed",
                message,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.Yes,
            )
            if choice == QMessageBox.StandardButton.Yes:
                update_env("HEADLESS", "false")
                self.append_console("\nMANUAL ACTION: Restarting scraper with HEADLESS=false for verification flow.\n")
                self.start_scraper()

        if self._pending_gui_reload:
            self.append_console("\nAUTO-RELOAD: Restarting GUI to apply updated code.\n")
            self._restart_gui_process()

    def send_terminal_input(self):
        text = (self.stdin_input.text() or "").strip()
        if not text:
            return

        if not self.worker or not getattr(self.worker, "process", None):
            self.console.insertHtml(
                "<span style='color:#FFD166;'>[input] No active scraper process. "
                "Input was not sent.</span><br>"
            )
            return

        proc = self.worker.process
        if proc.poll() is not None or not proc.stdin:
            self.console.insertHtml(
                "<span style='color:#FFD166;'>[input] Scraper process is not running. "
                "Input was not sent.</span><br>"
            )
            return

        try:
            proc.stdin.write(text + "\n")
            proc.stdin.flush()
            self.append_console(f"\n[stdin] {text}\n")
            self.stdin_input.clear()
        except Exception as e:
            self.console.insertHtml(
                f"<span style='color:#FFD166;'>[input] Send failed: {e}</span><br>"
            )

    def _confirm_update_mode_queue(self):
        try:
            from scraper import database_handler

            profiles, cutoff_date = database_handler.get_outdated_profiles_from_db()
            queued_count = len(profiles or [])
            if queued_count <= 0:
                QMessageBox.information(
                    self,
                    "Update Mode",
                    "No profiles currently require refresh. Nothing to run.",
                )
                return False

            min_delay_seconds, max_delay_seconds = self._get_effective_delay_range()
            avg_seconds = ((min_delay_seconds + max_delay_seconds) / 2.0) + 25.0
            est_minutes = max(1, int((queued_count * avg_seconds) / 60.0))
            cutoff_text = cutoff_date.strftime("%Y-%m-%d %H:%M:%S") if cutoff_date else "unknown"

            choice = QMessageBox.question(
                self,
                "Confirm Update Queue",
                (
                    f"Update mode queued profiles: {queued_count}\n"
                    f"Cutoff (last_updated older than): {cutoff_text}\n"
                    f"Estimated minimum runtime: ~{est_minutes} minute(s)\n\n"
                    "Continue with update mode run?"
                ),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.Yes,
            )
            return choice == QMessageBox.StandardButton.Yes
        except Exception as e:
            choice = QMessageBox.question(
                self,
                "Update Queue Unavailable",
                f"Could not preview update queue count: {e}\n\nStart update mode anyway?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            return choice == QMessageBox.StandardButton.Yes

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ScraperApp()
    window.show()
    sys.exit(app.exec())
