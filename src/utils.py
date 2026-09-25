# src/utils.py
"""
Shared utilities: logging setup, DB helpers, type-safe casters.
"""
import os
import sys
import logging


def setup_logging(log_level: str = None, log_file: str = "logs/pipeline.log") -> None:
    """
    Configure root logger once. Call this ONLY from the top-level entry point
    (run_etl.py or pipeline.py). All modules just do getLogger(__name__).
    """
    level = getattr(logging, (log_level or os.getenv("LOG_LEVEL", "INFO")).upper(), logging.INFO)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    root_logger = logging.getLogger()
    if root_logger.handlers:
        # Already configured — don't stack handlers
        return

    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # File handler — UTF-8 so emojis/unicode survive on Windows
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)

    # Stream handler — force UTF-8 on Windows consoles that default to cp1252
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    try:
        sh.stream.reconfigure(encoding="utf-8")  # Python 3.7+
    except AttributeError:
        pass

    root_logger.setLevel(level)
    root_logger.addHandler(fh)
    root_logger.addHandler(sh)


def get_db_path(connection_string: str) -> str:
    """Extract filesystem path from an sqlite:/// connection string."""
    if connection_string.startswith("sqlite:///"):
        return connection_string[len("sqlite:///"):]
    return connection_string


def safe_float(value, default=None):
    """Cast to Python float; return default on None/NaN to avoid SQLite type coercion bugs."""
    if value is None:
        return default
    try:
        import math
        f = float(value)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default
