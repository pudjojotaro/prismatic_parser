import logging
import sys
from pathlib import Path

def setup_logging():
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
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[file_handler, console_handler]
    )
    
    # Create loggers for different components
    loggers = {
        'item_service': logging.getLogger('item_service'),
        'gem_service': logging.getLogger('gem_service'),
        'monitoring_service': logging.getLogger('monitoring_service'),
        'alert_service': logging.getLogger('alert_service'),
        'parsing': logging.getLogger('parsing'),
        'database': logging.getLogger('database')
    }
    
    return loggers