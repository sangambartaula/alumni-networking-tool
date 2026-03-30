import sys
import os
import subprocess
import threading
import csv
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QGridLayout, QGroupBox, QLabel, QLineEdit, QComboBox, 
    QCheckBox, QPushButton, QTextEdit, QMessageBox, QDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QFileDialog
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QFont

# Update .env utility
def update_env(key, value):
    # Determine base directory depending on if frozen by PyInstaller
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
        if sys.platform == 'darwin':
            base_dir = os.path.abspath(os.path.join(base_dir, '../../..'))
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
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
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
            if sys.platform == 'darwin': # Mac App Bundle
                base_dir = os.path.abspath(os.path.join(base_dir, '../../..'))
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            
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
            self.process = subprocess.Popen(
                [python_exec, scraper_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=base_dir
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
        if self.process:
            self.process.terminate()
            self.output_signal.emit("\nSent termination signal...\n")

class FlagManagerDialog(QDialog):
    def __init__(self, base_dir, parent=None):
        super().__init__(parent)
        self.base_dir = base_dir
        self.setWindowTitle("Manage Review Flags")
        self.resize(800, 500)
        
        self.csv_path = os.path.join(self.base_dir, 'scraper', 'output', 'UNT_Alumni_Data.csv')
        self.txt_path = os.path.join(self.base_dir, 'scraper', 'output', 'flagged_for_review.txt')
        
        self.init_ui()
        self.load_data()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Flag", "Date Scraped", "Name", "Profile URL"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        
        layout.addWidget(self.table)
        
        btn_layout = QHBoxLayout()
        self.sel_all_btn = QPushButton("Select All")
        self.clear_all_btn = QPushButton("Clear All")
        self.save_btn = QPushButton("Save Flags")
        
        self.sel_all_btn.clicked.connect(self.select_all)
        self.clear_all_btn.clicked.connect(self.clear_all)
        self.save_btn.clicked.connect(self.save_flags)
        
        btn_layout.addWidget(self.sel_all_btn)
        btn_layout.addWidget(self.clear_all_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self.save_btn)
        
        layout.addLayout(btn_layout)
        
    def load_data(self):
        flagged_urls = set()
        if os.path.exists(self.txt_path):
            with open(self.txt_path, 'r', encoding='utf-8') as f:
                for line in f:
                    url = line.split('#')[0].strip().rstrip('/').lower()
                    if url: flagged_urls.add(url)
                    
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
            is_flagged = p['url'].lower() in flagged_urls
            cb.setChecked(is_flagged)
            
            # Center checkbox
            cb_widget = QWidget()
            cb_layout = QHBoxLayout(cb_widget)
            cb_layout.addWidget(cb)
            cb_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cb_layout.setContentsMargins(0,0,0,0)
            
            self.table.setCellWidget(i, 0, cb_widget)
            self.table.setItem(i, 1, QTableWidgetItem(p['date'][:10] if p['date'] else "Unknown"))
            self.table.setItem(i, 2, QTableWidgetItem(p['name']))
            self.table.setItem(i, 3, QTableWidgetItem(p['url']))
            
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
        for i in range(self.table.rowCount()):
            cb_widget = self.table.cellWidget(i, 0)
            if cb_widget and cb_widget.layout().itemAt(0).widget().isChecked():
                url = self.table.item(i, 3).text()
                name = self.table.item(i, 2).text()
                flagged.append(f"{url} # {name}")
                
        os.makedirs(os.path.dirname(self.txt_path), exist_ok=True)
        with open(self.txt_path, 'w', encoding='utf-8') as f:
            for item in flagged:
                f.write(f"{item}\n")
                
        QMessageBox.information(self, "Saved", f"Successfully saved {len(flagged)} profiles for review!")
        self.accept()

class ScraperApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("UNT Alumni Scraper")
        self.resize(850, 650)
        
        self.worker = None
        self.init_ui()

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
        row, col = 0, 0
        for d in ["software", "embedded", "mechanical", "construction", "biomedical", "materials"]:
            cb = QCheckBox(d)
            self.discs[d] = cb
            disc_layout.addWidget(cb, row, col)
            col += 1
            if col > 1:
                col = 0
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
        
        # 5. Data Flags
        flag_group = QGroupBox("Missing Data Flags")
        flag_layout = QVBoxLayout()
        self.flag_grad = QCheckBox("Flag missing graduation year")
        self.flag_deg = QCheckBox("Flag missing degree info")
        self.flag_exp = QCheckBox("Flag missing experience data")
        self.flag_exp.setChecked(True)
        flag_layout.addWidget(self.flag_grad)
        flag_layout.addWidget(self.flag_deg)
        flag_layout.addWidget(self.flag_exp)
        flag_group.setLayout(flag_layout)
        left_layout.addWidget(flag_group)
        
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
        
        left_panel.setFixedWidth(380)
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

    def on_mode_change(self, mode):
        is_conn = (mode == "connections")
        self.csv_path_label.setVisible(is_conn)
        self.csv_path_input.setVisible(is_conn)
        self.browse_csv_btn.setVisible(is_conn)

    def browse_csv(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Connections.csv", "", "CSV Files (*.csv);;All Files (*)")
        if file_path:
            self.csv_path_input.setText(file_path)

    def get_base_dir(self):
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
            if sys.platform == 'darwin':
                base_dir = os.path.abspath(os.path.join(base_dir, '../../..'))
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))
        return base_dir

    def load_settings_from_env(self):
        base_dir = self.get_base_dir()
            
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
        self.console.insertPlainText(text)
        scrollbar = self.console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

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
        dialog = FlagManagerDialog(self.get_base_dir(), self)
        dialog.exec()

    def validate_inputs(self):
        base_dir = self.get_base_dir()
        mode = self.mode_combo.currentText()
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
        update_env("LINKEDIN_EMAIL", self.email_input.text())
        if self.password_input.text():
            update_env("LINKEDIN_PASSWORD", self.password_input.text())
            
        update_env("SCRAPER_MODE", self.mode_combo.currentText())
        
        selected_discs = [d for d, cb in self.discs.items() if cb.isChecked()]
        update_env("SEARCH_DISCIPLINES", ",".join(selected_discs) if selected_discs else "")
        
        update_env("CONNECTIONS_CSV", self.csv_path_input.text() or "Connections.csv")
        
        update_env("MIN_DELAY", self.min_delay.text())
        update_env("MAX_DELAY", self.max_delay.text())
        
        update_env("FLAG_MISSING_GRAD_YEAR", str(self.flag_grad.isChecked()).lower())
        update_env("FLAG_MISSING_DEGREE", str(self.flag_deg.isChecked()).lower())
        update_env("FLAG_MISSING_EXPERIENCE_DATA", str(self.flag_exp.isChecked()).lower())
        
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
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        
        self.worker = ScraperWorker()
        self.worker.output_signal.connect(self.append_console)
        self.worker.finished_signal.connect(self.on_scraper_finished)
        self.worker.start()

    def stop_scraper(self):
        if self.worker:
            self.worker.stop()
            self.stop_btn.setEnabled(False)

    def on_scraper_finished(self):
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ScraperApp()
    window.show()
    sys.exit(app.exec())
