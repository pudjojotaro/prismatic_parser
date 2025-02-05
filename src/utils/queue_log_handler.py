import logging
import json

class QueueLogHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            # Use the handler formatter if assigned; otherwise use a default format.
            formatter = self.formatter or logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            log_entry = {
                "timestamp": formatter.formatTime(record),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage()
            }
            # If extra structured data exists, include it.
            if hasattr(record, "worker_info"):
                log_entry.update(record.worker_info)
            self.log_queue.put(json.dumps(log_entry))
        except Exception:
            self.handleError(record) 