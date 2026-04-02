import sys
import os
import subprocess
import signal
import threading
import csv
import webbrowser
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QGridLayout, QGroupBox, QLabel, QLineEdit, QComboBox, 
    QCheckBox, QPushButton, QTextEdit, QMessageBox, QDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QFileDialog, QSizePolicy
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QFont


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
            
    def stop(self):
        self._is_stopped = True
        proc = self.process
        if not proc or proc.poll() is not None:
            return

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
                    rows = [dict(r) for r in (cur.fetchall() or [])]
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
                return dict(row)

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
                    flags = [dict(r) for r in (cur.fetchall() or [])]

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
        item = QTableWidgetItem("" if text is None else str(text))
        if not editable:
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.table.setItem(row_idx, col_idx, item)
        return item

    def load_data(self):
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
        
    def load_data(self):
        flagged_urls = {}
        if os.path.exists(self.txt_path):
            with open(self.txt_path, 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.split('#')
                    url = parts[0].strip().rstrip('/').lower()
                    reason = ""
                    if len(parts) > 2:
                        reason = parts[-1].strip()  # Scraper injects reason as the 2nd comment part
                    elif len(parts) == 2 and not (" " in parts[1].strip() and len(parts[1].strip().split()) <= 3):
                        # Catch if there is only 1 comment part and it looks like a reason not a name
                        reason = parts[1].strip()
                        
                    if url: flagged_urls[url] = reason
                    
        profiles = []
        if os.path.exists(self.csv_path):
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = (row.get('linkedin_url', '') or row.get('profile_url', '')).strip().rstrip('/')
                    if not url: continue
                    name = f"{row.get('first', '')} {row.get('last', '')}".strip()
                    date_str = row.get('scraped_at', '')
                    profiles.append({'url': url, 'name': name, 'date': date_str})
                    
        # Sort by date descending
        def get_date(p):
            try: return datetime.fromisoformat(p['date'].replace('Z', '+00:00'))
            except: return datetime.min
        profiles.sort(key=get_date, reverse=True)
        
        self.table.setRowCount(len(profiles))
        for i, p in enumerate(profiles):
            cb = QCheckBox()
            url_key = p['url'].lower()
            is_flagged = url_key in flagged_urls
            cb.setChecked(is_flagged)
            reason_text = flagged_urls.get(url_key, "") if is_flagged else ""
            
            # Center checkbox
            cb_widget = QWidget()
            cb_layout = QHBoxLayout(cb_widget)
            cb_layout.addWidget(cb)
            cb_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cb_layout.setContentsMargins(0,0,0,0)
            
            self.table.setCellWidget(i, 0, cb_widget)
            self.table.setItem(i, 1, QTableWidgetItem(p['date'][:10] if p['date'] else "Unknown"))
            self.table.setItem(i, 2, QTableWidgetItem(p['name']))
            self.table.setItem(i, 3, QTableWidgetItem(reason_text))
            self.table.setItem(i, 4, QTableWidgetItem(p['url']))
            
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
                            parsed = int(new_val)
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

class ScraperApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("UNT Alumni Scraper")
        self.resize(850, 650)
        
        self.worker = None
        self.db_worker = None
        self.geocode_worker = None
        self._pending_upload_after_geocode = False
        self._run_started_at = None
        self._run_metrics = {}
        self.init_ui()

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

    def _check_cloud_status(self):
        try:
            backend_dir = os.path.join(get_base_dir(), "backend")
            if backend_dir not in sys.path:
                sys.path.insert(0, backend_dir)
            from database import get_direct_mysql_connection

            conn = get_direct_mysql_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            return "green", "Cloud DB connected", "Cloud writes are available."
        except Exception as e:
            return "red", "Cloud DB unavailable", f"Check internet/.env DB creds, then retry.\nDetails: {e}"

    def _check_geocode_status(self):
        try:
            backend_dir = os.path.join(get_base_dir(), "backend")
            if backend_dir not in sys.path:
                sys.path.insert(0, backend_dir)
            from geocoding import geocode_location_with_status

            coords, status = geocode_location_with_status("Denton, Texas")
            if coords:
                return "green", "Geocoding reachable", "Location API is reachable and returning coordinates."
            if status == "unknown_location":
                return "yellow", "Geocoding limited", "Service reachable, but sample location was not resolved."
            return "red", "Geocoding unavailable", "Check internet connection and retry geocode backfill later."
        except Exception as e:
            return "red", "Geocoding unavailable", f"Check internet connection and retry later.\nDetails: {e}"

    def refresh_preflight_status(self):
        cloud_state, cloud_text, cloud_tip = self._check_cloud_status()
        geo_state, geo_text, geo_tip = self._check_geocode_status()
        self._set_status_badge(self.cloud_status_label, cloud_state, cloud_text, cloud_tip)
        self._set_status_badge(self.geo_status_label, geo_state, geo_text, geo_tip)

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        
        # Left Panel (Settings)
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        left_layout.setSpacing(10)
        
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
        
        self.show_pass_cb = QCheckBox("Show")
        self.show_pass_cb.toggled.connect(self.toggle_password)
        cred_layout.addWidget(self.show_pass_cb, 1, 2)
        
        cred_group.setLayout(cred_layout)
        left_layout.addWidget(cred_group)

        status_group = QGroupBox("Preflight Status")
        status_layout = QVBoxLayout()
        self.cloud_status_label = QLabel("● Checking cloud DB...")
        self.geo_status_label = QLabel("● Checking geocoding...")
        status_layout.addWidget(self.cloud_status_label)
        status_layout.addWidget(self.geo_status_label)
        self.refresh_status_btn = QPushButton("Refresh Status")
        self.refresh_status_btn.clicked.connect(self.refresh_preflight_status)
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
            ("software", "Software, Data, AI & Cybersecurity"),
            ("embedded", "Embedded, Electrical & Hardware Engineering"),
            ("mechanical", "Mechanical Engineering & Manufacturing (includes Energy + Materials)"),
            ("construction", "Construction & Engineering Management"),
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
        
        self.stop_btn = QPushButton("Stop Scraper")
        self.stop_btn.setMinimumHeight(40)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.stop_scraper)
        
        btn_layout.addWidget(self.start_btn)
        btn_layout.addWidget(self.stop_btn)
        left_layout.addLayout(btn_layout)
        
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
        
        console_label = QLabel("Console Output")
        right_layout.addWidget(console_label)
        
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4; font-family: Consolas, monospace;")
        right_layout.addWidget(self.console)
        
        main_layout.addWidget(right_panel)
        
        # Hook mode change to layout visibility
        self.mode_combo.currentTextChanged.connect(self.on_mode_change)
        self.on_mode_change(self.mode_combo.currentText())
        
        # Auto-load existing config
        self.load_settings_from_env()
        self._reset_run_metrics()
        self.refresh_preflight_status()

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
                self.max_delay.setText("180")
            elif "Fast" in text:
                self.min_delay.setText("15")
                self.max_delay.setText("60")

    def append_console(self, text):
        stripped = (text or "").strip()
        if stripped.startswith("SUMMARY|"):
            payload = stripped.split("SUMMARY|", 1)[1]
            if "=" in payload:
                key, value = payload.split("=", 1)
                self._run_metrics[key.strip()] = value.strip()

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
        
        update_env("MIN_DELAY", self.min_delay.text())
        update_env("MAX_DELAY", self.max_delay.text())
        
        update_env("GUI_MAX_PROFILES", self.max_profiles.text())
        
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
        self._run_started_at = datetime.now()
        self._reset_run_metrics()
        self._run_metrics["user"] = self.email_input.text().strip().lower()
        self._run_metrics["mode"] = self.mode_combo.currentText()
        self.start_btn.setEnabled(False)
        self.geocode_btn.setEnabled(False)
        self.upload_db_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        
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
        if self.worker:
            self.worker.stop()
            self.stop_btn.setEnabled(False)

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
        self.stop_btn.setEnabled(False)

    def on_scraper_finished(self):
        self.start_btn.setEnabled(True)
        self.geocode_btn.setEnabled(True)
        self.upload_db_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self._append_gui_summary_block()
        self.refresh_preflight_status()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ScraperApp()
    window.show()
    sys.exit(app.exec())
