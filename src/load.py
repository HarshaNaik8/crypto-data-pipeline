# src/load.py
"""
Database loader with:
  - Star-schema DDL with UNIQUE constraint enforced at DB level.
  - Idempotent UPSERT using INSERT OR REPLACE (SQLite's native atomic upsert).
  - Explicit float casting so SQLite never silently stores ints for decimal cols.
  - Bulk load — one transaction per call, not one connection per row.
"""

import os
import logging
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

from src.utils import get_db_path, safe_float

load_dotenv()
logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Handles loading transformed data into SQL database with proper UPSERT logic."""

    def __init__(self, connection_string: str = None):
        self.connection_string = connection_string or os.getenv(
            "DB_CONNECTION_STRING", "sqlite:///crypto_pipeline.db"
        )
        self.db_path = get_db_path(self.connection_string)
        self.engine = create_engine(self.connection_string)
        logger.info("DatabaseLoader connected to: %s", self.connection_string)

    # ── DDL ───────────────────────────────────────────────────────────────────

    def create_tables(self) -> None:
        """Create star-schema tables + indexes if they don't exist."""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_symbol (
                    symbol_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol_code VARCHAR(10)  UNIQUE NOT NULL,
                    asset_name  VARCHAR(50),
                    asset_type  VARCHAR(20)  DEFAULT 'crypto'
                )
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_market_data (
                    fact_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol_id       INTEGER NOT NULL,
                    price_usd       REAL,
                    market_cap      REAL,
                    volume_24h      REAL,
                    change_24h      REAL,
                    rolling_avg_7d  REAL,
                    rolling_avg_30d REAL,
                    daily_return    REAL,
                    volatility_7d   REAL,
                    record_timestamp TEXT NOT NULL,
                    UNIQUE (symbol_id, record_timestamp),
                    FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id)
                )
            """))

            # Composite unique index (explicit, for query planner)
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uix_symbol_date "
                "ON fact_market_data(symbol_id, record_timestamp)"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_timestamp "
                "ON fact_market_data(record_timestamp)"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_symbol_id "
                "ON fact_market_data(symbol_id)"
            ))
            conn.commit()
        logger.info("Tables and indexes created/verified.")

    # ── Dimension upsert ──────────────────────────────────────────────────────

    def upsert_symbols(self, df: pd.DataFrame) -> dict:
        """
        Ensure all symbols in df exist in dim_symbol.
        Returns {SYMBOL_CODE: symbol_id} mapping.
        """
        codes = df["symbol"].str.upper().unique()
        symbol_map: dict = {}

        with self.engine.connect() as conn:
            for code in codes:
                conn.execute(
                    text(
                        "INSERT OR IGNORE INTO dim_symbol (symbol_code, asset_name, asset_type) "
                        "VALUES (:code, :name, 'crypto')"
                    ),
                    {"code": code, "name": code.capitalize()},
                )
            conn.commit()

            for code in codes:
                row = conn.execute(
                    text("SELECT symbol_id FROM dim_symbol WHERE symbol_code = :code"),
                    {"code": code},
                ).fetchone()
                if row:
                    symbol_map[code] = row[0]

        logger.info("Symbol map: %s", symbol_map)
        return symbol_map

    # ── Fact load ─────────────────────────────────────────────────────────────

    def load_fact(self, df: pd.DataFrame) -> None:
        """
        Upsert NEW rows into fact_market_data.
        Uses INSERT OR REPLACE so the DB-level UNIQUE constraint is the
        authoritative guard against duplicates — no application-level SELECT needed.

        Granularity: one record per (symbol, calendar date YYYY-MM-DD).
        """
        if df.empty:
            logger.info("No rows to load.")
            return

        df = df.copy()

        # 1. Ensure dim_symbol rows exist and get IDs
        symbol_map = self.upsert_symbols(df)
        df["symbol_id"] = df["symbol"].str.upper().map(symbol_map)
        missing = df["symbol_id"].isna()
        if missing.any():
            logger.warning("Could not resolve symbol_id for: %s", df.loc[missing, "symbol"].tolist())
            df = df[~missing].copy()

        # 2. Daily granularity: truncate timestamp to YYYY-MM-DD string
        df["record_timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d")

        # 3. If multiple runs happened on same day, keep the last one
        df = df.sort_values("timestamp").drop_duplicates(
            subset=["symbol_id", "record_timestamp"], keep="last"
        )

        # 4. Bulk INSERT OR REPLACE — atomic upsert per row in one transaction
        #    Using raw sqlite3 for performance (no per-row SQLAlchemy overhead)
        conn = sqlite3.connect(self.db_path)
        try:
            rows_loaded = 0
            for _, row in df.iterrows():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO fact_market_data (
                        symbol_id, price_usd, market_cap, volume_24h, change_24h,
                        rolling_avg_7d, rolling_avg_30d, daily_return, volatility_7d,
                        record_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(row["symbol_id"]),
                        safe_float(row.get("price_usd")),
                        safe_float(row.get("market_cap")),
                        safe_float(row.get("volume_24h")),
                        safe_float(row.get("change_24h")),
                        safe_float(row.get("rolling_avg_7d")),
                        safe_float(row.get("rolling_avg_30d")),
                        safe_float(row.get("daily_return"), default=0.0),
                        safe_float(row.get("volatility_7d"), default=0.0),
                        str(row["record_timestamp"]),
                    ),
                )
                rows_loaded += 1

            conn.commit()
            logger.info("Loaded %d rows into fact_market_data (INSERT OR REPLACE).", rows_loaded)
        except Exception as exc:
            conn.rollback()
            logger.error("Load failed — transaction rolled back: %s", exc)
            raise
        finally:
            conn.close()

    # ── Maintenance ───────────────────────────────────────────────────────────

    def clear_all_data(self) -> None:
        """DANGER: Delete ALL data. Use only for dev/reset."""
        with self.engine.connect() as conn:
            conn.execute(text("PRAGMA foreign_keys = OFF"))
            conn.execute(text("DELETE FROM fact_market_data"))
            conn.execute(text("DELETE FROM dim_symbol"))
            conn.execute(text("PRAGMA foreign_keys = ON"))
            conn.commit()
        logger.warning("ALL DATA DELETED from fact_market_data and dim_symbol!")

    # ── Orchestrator ──────────────────────────────────────────────────────────

    def run(self, transformed_df: pd.DataFrame) -> None:
        """Full load pipeline: create tables → load fact."""
        logger.info("Starting database load...")
        self.create_tables()
        self.load_fact(transformed_df)
        logger.info("Database load complete.")


# ── Standalone test ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import glob
    from src.utils import setup_logging

    setup_logging()
    transformed_files = sorted(glob.glob("data/transformed/transformed_*.parquet"))
    if not transformed_files:
        logger.error("No transformed files found. Run transform.py first.")
        raise SystemExit(1)

    df = pd.read_parquet(transformed_files[-1])
    loader = DatabaseLoader()
    loader.run(df)
    logger.info("Standalone load test completed.")