import sys
import os
import subprocess
import threading
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QGridLayout, QGroupBox, QLabel, QLineEdit, QComboBox, 
    QCheckBox, QPushButton, QTextEdit, QMessageBox
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
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["search", "review", "connections"])
        target_layout.addWidget(self.mode_combo, 0, 1)
        
        target_layout.addWidget(QLabel("Disciplines:"), 1, 0, Qt.AlignmentFlag.AlignTop)
        
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
        
        target_layout.addWidget(disc_widget, 1, 1)
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
        
        # Auto-load existing config
        self.load_settings_from_env()

    def load_settings_from_env(self):
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
            if sys.platform == 'darwin':
                base_dir = os.path.abspath(os.path.join(base_dir, '../../..'))
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            
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

    def validate_inputs(self):
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
