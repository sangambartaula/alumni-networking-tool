import sys
import os
import subprocess
import signal
import threading
import csv
import json
import webbrowser
from datetime import datetime
from urllib import request, error
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QGridLayout, QGroupBox, QLabel, QLineEdit, QComboBox, 
    QCheckBox, QPushButton, QTextEdit, QMessageBox, QDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QFileDialog, QSizePolicy, QTabWidget, QFormLayout, QScrollArea, QFrame
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QTimer
from PyQt6.QtGui import QFont, QIcon

from app_version import APP_VERSION


UPDATE_MANIFEST_URL = "https://raw.githubusercontent.com/sangambartaula/alumni-networking-tool/main/app_update.json"
DEFAULT_RELEASES_URL = "https://github.com/sangambartaula/alumni-networking-tool/releases/latest"


def _format_runtime_short(total_seconds):
    total_seconds = max(0, int(total_seconds))
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"

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
        with open(env_path, 'r') as f:
            lines = f.readlines()
            
    for i, line in enumerate(lines):
        if line.strip().startswith(f"{key}="):
            lines[i] = f"{key}={value}\n"
            found = True
            break
            
    if not found:
        if lines and not lines[-1].endswith('\n'):
            lines.append('\n')
        lines.append(f"{key}={value}\n")
        
    with open(env_path, 'w') as f:
        f.writelines(lines)

class ScraperWorker(QThread):
    output_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)
    
    def __init__(self):
        super().__init__()
        self.process = None
        self._is_stopped = False

    def run(self):
        base_dir = get_base_dir()
            
        # Target the virtual environment python directly to maintain Selenium/Groq dependencies
        if sys.platform == "win32":
            python_exec = os.path.join(base_dir, "venv", "Scripts", "python.exe")
            if not os.path.exists(python_exec):
                python_exec = "python"
        else:
            python_exec = os.path.join(base_dir, "venv", "bin", "python")
            if not os.path.exists(python_exec):
                python_exec = "python3"
                
        scraper_script = os.path.join(base_dir, 'scraper', 'main.py')
        
        try:
            self.output_signal.emit(f"Launching using: {python_exec}\n")
            popen_kwargs = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "stdin": subprocess.PIPE,
                "text": True,
                "bufsize": 1,
                "cwd": base_dir,
            }

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
            
            if not self._is_stopped:
                self.output_signal.emit(f"\nProcess finished with exit code {self.process.returncode}\n")
        except Exception as e:
            self.output_signal.emit(f"\nError starting scraper: {e}\n")
        finally:
            self.finished_signal.emit(0)
            
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
                    return
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
            
        if sys.platform == "win32":
            python_exec = os.path.join(base_dir, "venv", "Scripts", "python.exe")
            if not os.path.exists(python_exec): python_exec = "python"
        else:
            python_exec = os.path.join(base_dir, "venv", "bin", "python")
            if not os.path.exists(python_exec): python_exec = "python3"
                
        db_script = os.path.join(base_dir, 'backend', 'database.py')
        
        try:
            self.output_signal.emit(f"Launching Database Upload: {python_exec} backend/database.py\n")
            env = os.environ.copy()
            env.setdefault("DB_RUN_SEED", "1")
            env.setdefault("DB_RUN_MAINTENANCE", "0")
            self.process = subprocess.Popen(
                [python_exec, db_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
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

        if sys.platform == "win32":
            python_exec = os.path.join(base_dir, "venv", "Scripts", "python.exe")
            if not os.path.exists(python_exec):
                python_exec = "python"
        else:
            python_exec = os.path.join(base_dir, "venv", "bin", "python")
            if not os.path.exists(python_exec):
                python_exec = "python3"

        geocode_script = os.path.join(base_dir, 'backend', 'geocoding.py')

        try:
            self.output_signal.emit(f"Launching Geocoding: {python_exec} backend/geocoding.py --mode missing\n")
            self.process = subprocess.Popen(
                [python_exec, geocode_script, '--mode', 'missing'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
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
        self.setMinimumSize(600, 700)
        
        self.base_dir = get_base_dir()
        self.env_path = os.path.join(self.base_dir, '.env')
        self.env_values = dotenv.dotenv_values(self.env_path)
        
        self._fields = {}
        
        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        
        self.create_credentials_tab()
        self.create_scraper_tab()
        self.create_database_tab()
        
        btn_layout = QHBoxLayout()
        reset_btn = QPushButton("Reset to Defaults")
        reset_btn.clicked.connect(self.reset_to_defaults)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        
        save_btn = QPushButton("Save && Test Connection")
        save_btn.clicked.connect(self.save_and_test)
        save_btn.setStyleSheet("background-color: #2e6f40; color: white; font-weight: bold; padding: 6px;")
        
        btn_layout.addWidget(reset_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(cancel_btn)
        btn_layout.addWidget(save_btn)
        
        layout.addLayout(btn_layout)

    def _add_field(self, form_layout, key, label_text, ftype, default_val, tooltip, is_required=False, is_password=False):
        lbl = QLabel(f"{label_text} *" if is_required else label_text)
        if is_required:
            lbl.setStyleSheet("color: #0056b3; font-weight: bold;")
        else:
            lbl.setStyleSheet("color: #666;")
        
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
            # Strict falsy checks for bools
            val_str = current_val.lower() if current_val else str(default_val).lower()
            inp_widget.setChecked(val_str in ("true", "1", "yes"))
            row_layout.addWidget(inp_widget)
            row_layout.addStretch()
        else:
            inp_widget = QLineEdit()
            inp_widget.setPlaceholderText(f"Default: {default_val}")
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
        form.setSpacing(15)
        scroll.setWidget(container)
        self.tabs.addTab(scroll, name)
        return form

    def create_credentials_tab(self):
        f = self._create_tab("Credentials")
        self._add_field(f, "LINKEDIN_EMAIL", "LinkedIn Email", str, "", "Email address used for LinkedIn login. Required for headless or expired sessions.", True)
        self._add_field(f, "LINKEDIN_PASSWORD", "LinkedIn Password", str, "", "Password for LinkedIn. Only needed if cookies expire.", True, True)

    def create_scraper_tab(self):
        f = self._create_tab("Scraper")
        self._add_field(f, "MIN_DELAY", "Min Delay", int, 60, "Minimum seconds to wait between profile scrapes (prevents ban).", True)
        self._add_field(f, "MAX_DELAY", "Max Delay", int, 240, "Maximum seconds to wait between profile scrapes.", True)
        self._add_field(f, "HEADLESS", "Headless Mode", bool, False, "Run browser invisibly in background. Often faster, but harder to solve catchas.", False)
        self._add_field(f, "USE_COOKIES", "Use Cookies", bool, True, "Attempt to inject previous session cookies to bypass manual login.", False)
        self._add_field(f, "SCRAPER_DEBUG_HTML", "Debug HTML", bool, False, "Save scraped HTML dumps on failure for inspection.", False)
        self._add_field(f, "SCRAPE_RESUME_MAX_AGE_DAYS", "Resume Max Age", int, 90, "Profiles successfully scraped within these days will be skipped.", False)
        self._add_field(f, "USE_GROQ", "Extrapolate with Groq", bool, True, "Pass scraped profiles to LLM for final structure normalization.", False)

    def create_database_tab(self):
        f = self._create_tab("Database")
        self._add_field(f, "MYSQLHOST", "MySQL Host", str, "localhost", "Database server address (e.g., your.domain.com or localhost).", True)
        self._add_field(f, "MYSQLUSER", "MySQL User", str, "root", "Database username.", True)
        self._add_field(f, "MYSQLPASSWORD", "MySQL Password", str, "", "Database password.", True, True)
        self._add_field(f, "MYSQL_DATABASE", "Database Name", str, "linkedinhelper", "Primary catalog/schema name.", True)
        self._add_field(f, "MYSQLPORT", "MySQL Port", int, 3306, "Database connection port (defaults to 3306).", True)
        self._add_field(f, "USE_SQLITE_FALLBACK", "SQLite Fallback", bool, True, "If MySQL fails or is unreachable, locally cache into SQLite.", False)
        self._add_field(f, "DISABLE_DB", "Disable DB", bool, False, "Completely disable MySQL/SQLite connections (CSV-only output).", False)

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

    def save_and_test(self):
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
                        return
                    updates[key] = val
                else:
                    # User wants it empty (or fallback to default dynamically later)
                    # We can remove the key or leave it empty
                    updates[key] = ""
                    
        # Write back all updates to .env
        for k, v in updates.items():
            update_env(k, v)
            
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
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.close()
            QMessageBox.information(self, "Success", "✅ Settings saved & database connected successfully!")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Database Connection Failed", f"❌ Database connection failed:\n{e}")


class ScraperApp(QMainWindow):
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
        self._update_status_text = "Checking for updates..."
        self._update_available = False
        self._latest_version = APP_VERSION
        self._update_url = DEFAULT_RELEASES_URL
        self._cloud_status_cache = None
        self.init_ui()
        self._start_remote_update_check()

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
            return f"v{APP_VERSION}", "packaged build"

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
                return f"v{APP_VERSION} ({short_hash})", commit_time or "unknown"
        except Exception:
            pass

        mtime = self._safe_get_mtime(self._gui_file_path)
        if mtime:
            return f"v{APP_VERSION} (local)", datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        return f"v{APP_VERSION}", "unknown"

    def _version_tuple(self, version_text):
        pieces = []
        for token in str(version_text).strip().split("."):
            try:
                pieces.append(int(token))
            except ValueError:
                pieces.append(0)
        return tuple(pieces)

    def _start_remote_update_check(self):
        def _worker():
            latest_version = APP_VERSION
            update_url = DEFAULT_RELEASES_URL
            status_text = "Update check unavailable"
            update_available = False

            try:
                req = request.Request(
                    UPDATE_MANIFEST_URL,
                    headers={"User-Agent": "alumni-scraper-app/1.0"},
                )
                with request.urlopen(req, timeout=5) as response:
                    payload = json.loads(response.read().decode("utf-8"))

                latest_version = str(payload.get("version", APP_VERSION)).strip() or APP_VERSION
                update_url = str(payload.get("download_url", DEFAULT_RELEASES_URL)).strip() or DEFAULT_RELEASES_URL
                update_available = self._version_tuple(latest_version) > self._version_tuple(APP_VERSION)
                if update_available:
                    status_text = f"Update available: v{latest_version}"
                else:
                    status_text = "Up to date"
            except error.URLError:
                status_text = "Update check unavailable (offline)"
            except Exception:
                status_text = "Update check unavailable"

            def _apply_result():
                self._latest_version = latest_version
                self._update_url = update_url
                self._update_available = update_available
                self._update_status_text = status_text
                if self._update_available and getattr(sys, "frozen", False):
                    choice = QMessageBox.question(
                        self,
                        "Update Available",
                        (
                            f"A newer Alumni Scraper App is available.\n\n"
                            f"Current: v{APP_VERSION}\n"
                            f"Latest: v{self._latest_version}\n\n"
                            "Open the download page now?"
                        ),
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                        QMessageBox.StandardButton.Yes,
                    )
                    if choice == QMessageBox.StandardButton.Yes:
                        webbrowser.open(self._update_url)

            QTimer.singleShot(0, _apply_result)

        threading.Thread(target=_worker, daemon=True, name="AppUpdateCheck").start()

    def _refresh_about_metadata(self):
        version_text, last_updated_text = self._read_git_version_info()
        self._about_version_text = version_text
        self._about_last_updated_text = last_updated_text

    def _start_gui_autoreload_watcher(self):
        self._gui_reload_timer = QTimer(self)
        self._gui_reload_timer.setInterval(3000)
        self._gui_reload_timer.timeout.connect(self._check_for_gui_file_update)
        self._gui_reload_timer.start()

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
                f"Update Channel: {DEFAULT_RELEASES_URL}\n\n"
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
        try:
            conn = self._get_db_connection()
            try:
                with conn.cursor(dictionary=True) as cur:
                    query = """
                        SELECT id, scraper_email, scraper_mode, status, profiles_scraped,
                               started_at, completed_at
                        FROM scrape_runs
                    """
                    params = []
                    if only_current and current_email:
                        query += " WHERE LOWER(scraper_email) = %s"
                        params.append(current_email)
                    query += " ORDER BY started_at DESC LIMIT 25"
                    cur.execute(query, tuple(params))
                    rows = cur.fetchall() or []
            except Exception:
                with conn.cursor() as cur:
                    query = """
                        SELECT id, scraper_email, scraper_mode, status, profiles_scraped,
                               started_at, completed_at
                        FROM scrape_runs
                    """
                    params = []
                    if only_current and current_email:
                        query += " WHERE LOWER(scraper_email) = ?"
                        params.append(current_email)
                    query += " ORDER BY started_at DESC LIMIT 25"
                    cur.execute(query, tuple(params))
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
        except Exception:
            rows = []

        self._history_rows = rows
        self.run_history_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            started_raw = str(row.get("started_at") or "")
            started = started_raw[:16].replace("T", " ")
            completed_raw = str(row.get("completed_at") or "")
            duration = "-"
            try:
                if started_raw and completed_raw:
                    dt_start = datetime.fromisoformat(started_raw.replace("Z", "").replace(" ", "T"))
                    dt_end = datetime.fromisoformat(completed_raw.replace("Z", "").replace(" ", "T"))
                    duration = _format_runtime_short((dt_end - dt_start).total_seconds())
            except Exception:
                duration = "-"

            values = [
                started,
                (row.get("scraper_email") or "unknown"),
                (row.get("scraper_mode") or "unknown"),
                str(row.get("profiles_scraped") or 0),
                duration,
                (row.get("status") or "unknown"),
                str(row.get("id") or ""),
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.run_history_table.setItem(row_idx, col, item)

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
        if os.getenv("DISABLE_DB", "0") == "1":
            return "gray", "Cloud DB disabled", "DISABLE_DB=1 in .env, so cloud DB checks are intentionally bypassed."

        backend_dir = os.path.join(get_base_dir(), "backend")
        if backend_dir not in sys.path:
            sys.path.insert(0, backend_dir)

        try:
            from database import get_connection, get_direct_mysql_connection
        except Exception as e:
            return "red", "Cloud DB unavailable", f"Database module failed to load. Details: {type(e).__name__} - {e}"

        try:
            # Deterministic cloud probe: table read from alumni.
            conn = get_direct_mysql_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM alumni")
                    row = cur.fetchone()
                    total = int(row[0]) if row else 0
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            return "green", "Cloud DB connected", f"Cloud probe succeeded (alumni rows: {total})."
        except Exception as e:
            # If app-level connection still works, fallback is active.
            try:
                conn = get_connection()
                try:
                    if conn.__class__.__name__ == "SQLiteConnectionWrapper":
                        return "yellow", "Cloud DB offline (SQLite fallback active)", "Cloud probe failed; app is writing locally via SQLite fallback."
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception:
                pass
            return "red", "Cloud DB unavailable", f"Cloud probe failed.\nDetails: {type(e).__name__} - {e}"

    def _check_cloud_status(self, force_probe=False):
        if force_probe or self._cloud_status_cache is None:
            self._cloud_status_cache = self._probe_cloud_status()
        return self._cloud_status_cache

    def _check_geocode_status(self):
        try:
            test_url = (
                "https://nominatim.openstreetmap.org/search"
                "?q=Denton%2C%20Texas&format=json&limit=1&countrycodes=us"
            )
            req = request.Request(test_url, headers={"User-Agent": "alumni-scraper-app/1.0"})
            with request.urlopen(req, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if isinstance(payload, list):
                if payload:
                    return "green", "Geocoding reachable", "Location API is reachable and returning coordinates."
                return "yellow", "Geocoding limited", "Service reachable, but sample location returned no results."
            return "yellow", "Geocoding unstable", "Service responded with an unexpected payload shape."
        except error.URLError as e:
            return "yellow", "Geocoding unstable", f"Network issue while probing geocoding service. Details: {e}"
        except Exception as e:
            return "red", "Geocoding unavailable", f"Check internet connection and retry later.\nDetails: {type(e).__name__} - {e}"

    def refresh_preflight_status(self, force_cloud_probe=False):
        cloud_state, cloud_text, cloud_tip = self._check_cloud_status(force_probe=force_cloud_probe)
        geo_state, geo_text, geo_tip = self._check_geocode_status()
        self._set_status_badge(self.cloud_status_label, cloud_state, cloud_text, cloud_tip)
        self._set_status_badge(self.geo_status_label, geo_state, geo_text, geo_tip)


    def open_settings(self):
        dialog = SettingsDialog(self)
        if dialog.exec():
            # Reload environment to sync any changes made in the dialog
            self.load_settings_from_env()
            self.refresh_preflight_status(force_cloud_probe=True)
            
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        self._refresh_about_metadata()
        
        # Left Panel (Settings)
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        left_layout.setSpacing(10)
        
        
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
        self.refresh_status_btn.clicked.connect(lambda: self.refresh_preflight_status(force_cloud_probe=True))
        status_layout.addWidget(self.refresh_status_btn)
        status_group.setLayout(status_layout)
        left_layout.addWidget(status_group)
        
        # 2. Target Profile Options
        target_group = QGroupBox("Scraping Mode & Targets")
        target_layout = QGridLayout()
        
        target_layout.addWidget(QLabel("Mode:"), 0, 0)
        mode_row = QHBoxLayout()
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["search", "review", "connections"])
        mode_row.addWidget(self.mode_combo)
        
        self.help_btn = QPushButton("?")
        self.help_btn.setFixedWidth(30)
        self.help_btn.clicked.connect(self.show_help)
        mode_row.addWidget(self.help_btn)
        
        self.manage_flags_btn = QPushButton("Manage Flags")
        self.manage_flags_btn.clicked.connect(self.open_flag_manager)
        mode_row.addWidget(self.manage_flags_btn)
        
        target_layout.addLayout(mode_row, 0, 1)
        
        # Connections file picker
        self.csv_path_label = QLabel("Connections CSV:")
        target_layout.addWidget(self.csv_path_label, 1, 0)
        
        csv_row = QHBoxLayout()
        self.csv_path_input = QLineEdit()
        self.csv_path_input.setPlaceholderText("Browse for Connections.csv...")
        csv_row.addWidget(self.csv_path_input)
        
        self.browse_csv_btn = QPushButton("Browse")
        self.browse_csv_btn.clicked.connect(self.browse_csv)
        csv_row.addWidget(self.browse_csv_btn)
        
        target_layout.addLayout(csv_row, 1, 1)
        
        target_layout.addWidget(QLabel("Disciplines:"), 2, 0, Qt.AlignmentFlag.AlignTop)
        
        disc_widget = QWidget()
        disc_layout = QGridLayout(disc_widget)
        disc_layout.setContentsMargins(0,0,0,0)
        self.discs = {}
        row = 0
        discipline_options = [
            ("software", "Software, Data, AI && Cybersecurity"),
            ("embedded", "Embedded, Electrical && Hardware Engineering"),
            ("mechanical", "Mechanical Engineering && Manufacturing"),
            ("construction", "Construction && Engineering Management"),
            ("biomedical", "Biomedical Engineering"),
        ]
        disc_layout.setHorizontalSpacing(4)
        disc_layout.setVerticalSpacing(6)
        for discipline_key, discipline_label in discipline_options:
            cb = QCheckBox(discipline_label)
            cb.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            self.discs[discipline_key] = cb
            disc_layout.addWidget(cb, row, 0)
            row += 1
        
        target_layout.addWidget(disc_widget, 2, 1)
        target_group.setLayout(target_layout)
        left_layout.addWidget(target_group)
        
        # 3. Delays
        delay_group = QGroupBox("Scrape Frequency (Anti-Ban)")
        delay_layout = QGridLayout()
        
        delay_layout.addWidget(QLabel("Preset:"), 0, 0)
        self.delay_combo = QComboBox()
        self.delay_combo.addItems(["Slow (2m - 10m)", "Medium (1m - 3m)", "Fast (15s - 60s)", "Custom"])
        self.delay_combo.setCurrentText("Medium (1m - 3m)")
        self.delay_combo.currentTextChanged.connect(self.on_delay_change)
        delay_layout.addWidget(self.delay_combo, 0, 1, 1, 3)
        
        delay_layout.addWidget(QLabel("Custom Min (s):"), 1, 0)
        self.min_delay = QLineEdit("60")
        self.min_delay.setEnabled(False)
        delay_layout.addWidget(self.min_delay, 1, 1)
        
        delay_layout.addWidget(QLabel("Max (s):"), 1, 2)
        self.max_delay = QLineEdit("180")
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
        self.hours_input = QLineEdit("0")
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

        sync_btn_layout.addWidget(self.geocode_btn)
        sync_btn_layout.addWidget(self.upload_db_btn)
        left_layout.addLayout(sync_btn_layout)
        
        left_panel.setFixedWidth(460)
        main_layout.addWidget(left_panel)
        
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

        history_group = QGroupBox("Recent Scrape Sessions")
        history_layout = QVBoxLayout()
        history_controls = QHBoxLayout()
        self.run_history_mine_only = QCheckBox("Only current email")
        self.run_history_mine_only.stateChanged.connect(self.refresh_run_history)
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
        history_group.setLayout(history_layout)
        right_layout.addWidget(history_group)
        
        main_layout.addWidget(right_panel)
        
        # Hook mode change to layout visibility
        self.mode_combo.currentTextChanged.connect(self.on_mode_change)
        self.on_mode_change(self.mode_combo.currentText())
        
        # Auto-load existing config
        self.load_settings_from_env()
        self._reset_run_metrics()
        self.refresh_preflight_status(force_cloud_probe=True)
        self.refresh_run_history()
        self._apply_modern_style()
        self._start_gui_autoreload_watcher()

    def on_mode_change(self, mode):
        is_conn = (mode == "connections")
        self.csv_path_label.setVisible(is_conn)
        self.csv_path_input.setVisible(is_conn)
        self.browse_csv_btn.setVisible(is_conn)

    def browse_csv(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Connections.csv", "", "CSV Files (*.csv);;All Files (*)")
        if file_path:
            self.csv_path_input.setText(file_path)

    def load_settings_from_env(self):
        base_dir = get_base_dir()
            
        env_path = os.path.join(base_dir, '.env')
        if not os.path.exists(env_path):
            return
            
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                if '=' in line:
                    key, val = line.split('=', 1)
                    key = key.strip()
                    val = val.strip()
                    
                    if key == "LINKEDIN_EMAIL" and val:
                        self.email_input.setText(val)
                    elif key == "LINKEDIN_PASSWORD" and val:
                        self.password_input.setText(val)
                    elif key == "CONNECTIONS_CSV" and val:
                        self.csv_path_input.setText(val)
                    elif key == "GUI_MAX_PROFILES" and val:
                        self.max_profiles.setText(val)
                    elif key == "GUI_MAX_RUNTIME_MINUTES" and val:
                        try:
                            total = int(val)
                            self.hours_input.setText(str(total // 60))
                            self.mins_input.setText(str(total % 60))
                        except: pass

    def on_delay_change(self, text):
        if text == "Custom":
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
                self.max_delay.setText("240")
            elif "Fast" in text:
                self.min_delay.setText("30")
                self.max_delay.setText("120")

    def append_console(self, text):
        stripped = (text or "").strip()
        if stripped.startswith("SUMMARY|"):
            payload = stripped.split("SUMMARY|", 1)[1]
            if "=" in payload:
                key, value = payload.split("=", 1)
                self._run_metrics[key.strip()] = value.strip()

        if stripped.startswith("ACTION|"):
            payload = stripped.split("ACTION|", 1)[1]
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

        line_lower = (text or "").lower()
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
        QMessageBox.information(self, "How to Download Connections.csv", 
            "1. Visit: https://www.linkedin.com/mypreferences/d/download-my-data\n"
            "2. Select 'Download larger data archive' (including connections, verifications, etc.)\n"
            "3. Click 'Request archive' and wait for LinkedIn to email you.\n"
            "4. When available, download the data and extract the zip file.\n"
            "5. Open the folder and find the 'Connections.csv' file. This contains all your connections with their URLs.\n"
            "6. Click the 'Browse' button in this app to select that exact file wherever you saved it!"
        )

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
        update_env("LINKEDIN_EMAIL", self.email_input.text().strip().lower())
        if self.password_input.text():
            update_env("LINKEDIN_PASSWORD", self.password_input.text())
            
        update_env("SCRAPER_MODE", self.mode_combo.currentText())
        
        selected_discs = [d for d, cb in self.discs.items() if cb.isChecked()]
        update_env("SEARCH_DISCIPLINES", ",".join(selected_discs) if selected_discs else "")
        
        update_env("CONNECTIONS_CSV", self.csv_path_input.text() or "Connections.csv")
        
        update_env("MIN_DELAY", self.min_delay.text() or "60")
        update_env("MAX_DELAY", self.max_delay.text() or "180")
        
        update_env("GUI_MAX_PROFILES", self.max_profiles.text() or "0")
        
        try:
            total_mins = int(self.hours_input.text() or 0) * 60 + int(self.mins_input.text() or 0)
        except ValueError:
            total_mins = 0
        update_env("GUI_MAX_RUNTIME_MINUTES", str(total_mins))

    def start_scraper(self):
        if not self.validate_inputs():
            return
            
        self.save_all_settings_to_env()
        
        self.console.clear()
        self._manual_intervention_needed = False
        self._manual_intervention_reason = ""
        self._suggest_restart_headed = False
        self._run_started_at = datetime.now()
        self._reset_run_metrics()
        self._run_metrics["user"] = self.email_input.text().strip().lower()
        self._run_metrics["mode"] = self.mode_combo.currentText()
        self.start_btn.setEnabled(False)
        self.geocode_btn.setEnabled(False)
        self.upload_db_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.stop_btn.setText("Stop")
        self.stop_after_profile_btn.setVisible(False)
        self.stop_immediately_btn.setVisible(False)
        
        self.worker = ScraperWorker()
        self.worker.output_signal.connect(self.append_console)
        self.worker.finished_signal.connect(self.on_scraper_finished)
        self.worker.start()

    def run_geocode(self):
        self.save_all_settings_to_env()
        self.console.clear()
        self.start_btn.setEnabled(False)
        self.geocode_btn.setEnabled(False)
        self.upload_db_btn.setEnabled(False)
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
        self.stop_btn.setEnabled(False)
        
        self.db_worker = DatabaseWorker()
        self.db_worker.output_signal.connect(self.append_console)
        self.db_worker.finished_signal.connect(self.on_db_finished)
        self.db_worker.start()

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
        self._reset_stop_controls()

    def on_scraper_finished(self):
        self.start_btn.setEnabled(True)
        self.geocode_btn.setEnabled(True)
        self.upload_db_btn.setEnabled(True)
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

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ScraperApp()
    window.show()
    sys.exit(app.exec())
