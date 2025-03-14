import os
import logging
from logging.handlers import RotatingFileHandler


class AppLogger:

    def __init__(self):
        self.log_dir = 'logs'
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, "api.log")
        logging.basicConfig(
            level=logging.INFO,  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                RotatingFileHandler(self.log_file, maxBytes=5 * 1024 * 1024, backupCount=5),  # Rotate logs when size > 5MB
                logging.StreamHandler()  # Also log to console
            ]
        )
        self.logger = logging.getLogger("api_logger")
