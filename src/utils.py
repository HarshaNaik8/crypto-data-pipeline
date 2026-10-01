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


def create_db_engine(connection_string: str = None):
    """
    Universal database engine creator with auto-negotiation for:
      - SQLite (sqlite:///)
      - PostgreSQL via psycopg 3 (postgresql+psycopg://)
      - PostgreSQL via psycopg2 (postgresql+psycopg2://)
      - PostgreSQL via pg8000 (postgresql+pg8000://)
    Automatically handles driver fallbacks, SSL parameters, and cloud environments.
    """
    from sqlalchemy import create_engine

    conn_str = connection_string or os.getenv("NEON_DB_URL") or os.getenv("DB_CONNECTION_STRING", "sqlite:///crypto_pipeline.db")
    if not conn_str or not ("postgresql" in conn_str or "postgres" in conn_str):
        return create_engine(conn_str)

    # Detect installed drivers
    has_psycopg3 = False
    try:
        import psycopg
        has_psycopg3 = True
    except ImportError:
        pass

    has_psycopg2 = False
    try:
        import psycopg2
        has_psycopg2 = True
    except ImportError:
        pass

    has_pg8000 = False
    try:
        import pg8000
        has_pg8000 = True
    except ImportError:
        pass

    # Helper to apply cloud-resilient pool settings
    def _create_robust_engine(url: str, **kwargs):
        # pool_pre_ping prevents OperationalError when Neon Serverless Postgres goes to sleep and kills connections
        # pool_recycle prevents stale connections that have exceeded the cloud provider's TCP timeout
        kwargs.setdefault("pool_pre_ping", True)
        kwargs.setdefault("pool_recycle", 300)
        return create_engine(url, **kwargs)

    # Helper for pg8000 URL rewrite
    def _to_pg8000(url: str):
        import ssl
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(url)
        cleaned = urlunparse(("postgresql+pg8000", parsed.netloc, parsed.path, parsed.params, "", parsed.fragment))
        return _create_robust_engine(cleaned, connect_args={"ssl_context": ssl.create_default_context()})

    # If URL requests psycopg 3 specifically
    if "postgresql+psycopg://" in conn_str:
        if has_psycopg3:
            return _create_robust_engine(conn_str)
        elif has_psycopg2:
            return _create_robust_engine(conn_str.replace("postgresql+psycopg://", "postgresql+psycopg2://"))
        elif has_pg8000:
            return _to_pg8000(conn_str)

    # If URL requests psycopg 2 specifically
    if "postgresql+psycopg2://" in conn_str:
        if has_psycopg2:
            return _create_robust_engine(conn_str)
        elif has_psycopg3:
            return _create_robust_engine(conn_str.replace("postgresql+psycopg2://", "postgresql+psycopg://"))
        elif has_pg8000:
            return _to_pg8000(conn_str)

    # If generic postgresql:// or postgres://
    if conn_str.startswith("postgresql://") or conn_str.startswith("postgres://"):
        if has_psycopg3:
            target = conn_str.replace("postgresql://", "postgresql+psycopg://", 1).replace("postgres://", "postgresql+psycopg://", 1)
            return _create_robust_engine(target)
        elif has_psycopg2:
            target = conn_str.replace("postgresql://", "postgresql+psycopg2://", 1).replace("postgres://", "postgresql+psycopg2://", 1)
            return _create_robust_engine(target)
        elif has_pg8000:
            return _to_pg8000(conn_str)

    return _create_robust_engine(conn_str)

