import logging
import sys
from typing import Optional


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Set up a logger with both file and console handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent adding handlers multiple times
    if logger.handlers:
        return logger

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler("app.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# Global logger instance
logger = setup_logger(__name__)
