import sys
import json
import asyncio
import threading
import queue
from time import time

# PyQt5 imports
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QListWidget, QTextEdit, QLabel, QGroupBox
)
from PyQt5.QtCore import QTimer

# Import application components
from src import Application, init_db
from src.database.repository import DatabaseRepository
from src.services.alert_service import AlertService
from src.services.proxy_service import ProxyService
from src.services.item_service import ItemService
from src.services.gem_service import GemService
from src.services.monitoring_service import MonitoringService
from src.utils import logging as log_util

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Dota 2 Prismatic Gems Parser Dashboard")
        self.resize(800, 600)
        
        # Control flags and threads
        self.running = False
        self.app_thread = None
        
        # Shared log queue and worker stats dictionary
        self.log_queue = queue.Queue()
        self.worker_stats = {"gem": {}, "item": {}}
        
        # Set up logging with the shared log_queue.
        log_util.setup_logging(log_queue=self.log_queue)
        
        # Initialize application components
        init_db()  # initializing the database
        db_repository = DatabaseRepository()
        alert_service = AlertService()
        proxy_service = ProxyService()
        item_service = ItemService(db_repository)
        gem_service = GemService(db_repository)
        monitoring_service = MonitoringService(db_repository, alert_service)
        
        # Create the Application instance.
        self.application = Application(item_service, gem_service, monitoring_service, alert_service, proxy_service)
        
        self.setup_ui()
        self.start_timers()

    def setup_ui(self):
        # Create a central widget and a QTabWidget.
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)
        
        # Dashboard Tab
        self.dashboard_tab = QWidget()
        self.tabs.addTab(self.dashboard_tab, "Dashboard")
        dash_layout = QVBoxLayout(self.dashboard_tab)
        
        # Worker group boxes and list widgets
        # Gem Workers
        self.gem_group = QGroupBox("Gem Workers")
        gem_layout = QVBoxLayout(self.gem_group)
        self.gem_list = QListWidget()
        gem_layout.addWidget(self.gem_list)
        
        # Item Workers
        self.item_group = QGroupBox("Item Workers")
        item_layout = QVBoxLayout(self.item_group)
        self.item_list = QListWidget()
        item_layout.addWidget(self.item_list)
        
        # Add worker groups horizontally
        workers_layout = QHBoxLayout()
        workers_layout.addWidget(self.gem_group)
        workers_layout.addWidget(self.item_group)
        dash_layout.addLayout(workers_layout)
        
        # Start/Stop Button
        self.start_stop_button = QPushButton("Start")
        self.start_stop_button.clicked.connect(self.toggle_start_stop)
        dash_layout.addWidget(self.start_stop_button)
        
        # Logs Tab
        self.logs_tab = QWidget()
        self.tabs.addTab(self.logs_tab, "Logs")
        logs_layout = QVBoxLayout(self.logs_tab)
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        logs_layout.addWidget(self.log_text)
    
    def start_timers(self):
        # Timer for polling log queue (every 100ms)
        self.log_timer = QTimer()
        self.log_timer.timeout.connect(self.poll_log_queue)
        self.log_timer.start(100)
        
        # Timer for dashboard update (every 1 second)
        self.dashboard_timer = QTimer()
        self.dashboard_timer.timeout.connect(self.update_dashboard)
        self.dashboard_timer.start(1000)
    
    def toggle_start_stop(self):
        if not self.running:
            self.running = True
            self.start_stop_button.setText("Stop")
            # Start the background thread that runs the async application.
            self.app_thread = threading.Thread(target=self.run_async_app, daemon=True)
            self.app_thread.start()
        else:
            self.running = False
            self.start_stop_button.setText("Start")
            # Signal the application to shutdown gracefully.
            self.application.stop()
    
    def run_async_app(self):
        # Create a new asyncio event loop and run the application.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.application.run())
        except Exception as e:
            import logging
            logging.getLogger('main').error(f"Application encountered an error: {e}")
        finally:
            loop.close()
    
    def poll_log_queue(self):
        """
        Poll the shared log queue. For any structured log entries that include worker update information,
        update our worker_stats dictionary. Also, display all log messages in the logs tab.
        """
        try:
            while True:
                msg = self.log_queue.get_nowait()
                try:
                    log_entry = json.loads(msg)
                    # Check if this log message is a worker update. We expect an "event" key.
                    if log_entry.get("event") == "PROCESS_UPDATE":
                        service = log_entry.get("service")
                        worker = log_entry.get("worker")
                        current_item = log_entry.get("current_item", "Idle")
                        avg_time = log_entry.get("avg_time", 0)
                        if service and worker:
                            if service not in self.worker_stats:
                                self.worker_stats[service] = {}
                            self.worker_stats[service][worker] = {"current_item": current_item, "avg_time": avg_time}
                except Exception:
                    # Not a structured worker update.
                    pass
                # Append the raw log message to the logs text.
                self.log_text.append(msg)
        except queue.Empty:
            pass  # no more messages for now
    
    def update_dashboard(self):
        """
        Update the dashboard lists with the current status of gem and item workers.
        """
        self.gem_list.clear()
        for worker, stats in self.worker_stats.get("gem", {}).items():
            display = f"{worker}: {stats.get('current_item', 'Idle')} (Avg: {stats.get('avg_time', 0)}s)"
            self.gem_list.addItem(display)
            
        self.item_list.clear()
        for worker, stats in self.worker_stats.get("item", {}).items():
            display = f"{worker}: {stats.get('current_item', 'Idle')} (Avg: {stats.get('avg_time', 0)}s)"
            self.item_list.addItem(display)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())