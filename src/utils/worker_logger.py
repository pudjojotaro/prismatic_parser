import logging
from typing import Optional

class WorkerLogger:
    def __init__(self, service_name: str, proxy: str, item_name: Optional[str] = None):
        self.logger = logging.getLogger(service_name)
        self.proxy = proxy
        self.item_name = item_name
        
    def _format_message(self, message: str) -> str:
        prefix = f"[Worker {self.proxy}]"
        if self.item_name:
            prefix += f" [{self.item_name}]"
        return f"{prefix} {message}"
        
    def info(self, message: str):
        self.logger.info(self._format_message(message))
        
    def error(self, message: str, exc_info=None):
        self.logger.error(self._format_message(message), exc_info=exc_info)
        
    def debug(self, message: str):
        self.logger.debug(self._format_message(message))
        
    def set_item(self, item_name: str):
        self.item_name = item_name

    def warning(self, message: str):
        self.logger.warning(self._format_message(message))