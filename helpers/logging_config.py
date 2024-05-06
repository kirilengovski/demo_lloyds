import logging


class ColoredFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: '\033[94m',   # Blue
        logging.INFO: '\033[92m',    # Green
        logging.WARNING: '\033[93m',  # Yellow
        logging.ERROR: '\033[91m',    # Red
        logging.CRITICAL: '\033[95m'  # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        log_color = self.COLORS.get(record.levelno, self.RESET)
        log_msg = super().format(record)
        return f'{log_color}{log_msg}{self.RESET}'
