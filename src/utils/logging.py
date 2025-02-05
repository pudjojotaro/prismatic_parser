import logging
import sys
from pathlib import Path

def setup_logging(log_queue=None):
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Clear existing log file
    log_file = log_dir / "app.log"
    if log_file.exists():
        log_file.unlink()
    
    # Force UTF-8 encoding for log file
    file_handler = logging.FileHandler('logs/app.log', encoding='utf-8')
    
    # Force UTF-8 encoding for console output
    if sys.platform == 'win32':
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')
    
    console_handler = logging.StreamHandler(sys.stdout)
    
    handlers = [file_handler, console_handler]
    
    # If a log_queue is provided, add our custom queue handler.
    if log_queue is not None:
        from src.utils.queue_log_handler import QueueLogHandler
        queue_handler = QueueLogHandler(log_queue)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        queue_handler.setFormatter(formatter)
        handlers.append(queue_handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    # Create and return loggers for the different components
    loggers = {
        'item_service': logging.getLogger('item_service'),
        'gem_service': logging.getLogger('gem_service'),
        'monitoring_service': logging.getLogger('monitoring_service'),
        'alert_service': logging.getLogger('alert_service'),
        'parsing': logging.getLogger('parsing'),
        'database': logging.getLogger('database')
    }
    
    return loggers