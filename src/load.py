# src/load.py
"""
Database loader with:
  - Universal SQL dialect support (SQLite & PostgreSQL / Neon Cloud).
  - Star-schema DDL with UNIQUE constraint enforced at DB level.
  - Idempotent True In-Place UPSERT using ON CONFLICT (symbol_id, record_timestamp) DO UPDATE.
  - Fixes auto-increment sequence jumps by avoiding DELETE+INSERT semantics.
  - Bulk transactional load via SQLAlchemy engine.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from src.utils import get_db_path, safe_float

load_dotenv()
logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Handles loading transformed data into SQL database with proper UPSERT logic."""

    def __init__(self, connection_string: str = None):
        self.connection_string = connection_string or os.getenv(
            "NEON_DB_URL"
        ) or os.getenv(
            "DB_CONNECTION_STRING", "sqlite:///crypto_pipeline.db"
        )
        self.is_postgres = "postgresql" in self.connection_string
        self.db_path = get_db_path(self.connection_string)
        self.engine = create_engine(self.connection_string)
        logger.info("DatabaseLoader connected to: %s (PostgreSQL=%s)", self.connection_string.split("@")[-1], self.is_postgres)

    # ── DDL ───────────────────────────────────────────────────────────────────

    def create_tables(self) -> None:
        """Create star-schema tables + indexes if they don't exist."""
        with self.engine.begin() as conn:
            if self.is_postgres:
                # PostgreSQL / Neon DDL
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS dim_symbol (
                        symbol_id   SERIAL PRIMARY KEY,
                        symbol_code VARCHAR(10) UNIQUE NOT NULL,
                        asset_name  VARCHAR(50),
                        asset_type  VARCHAR(20) DEFAULT 'crypto'
                    );
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS fact_market_data (
                        fact_id         SERIAL PRIMARY KEY,
                        symbol_id       INTEGER NOT NULL REFERENCES dim_symbol(symbol_id),
                        price_usd       DOUBLE PRECISION,
                        market_cap      DOUBLE PRECISION,
                        volume_24h      DOUBLE PRECISION,
                        change_24h      DOUBLE PRECISION,
                        rolling_avg_7d  DOUBLE PRECISION,
                        rolling_avg_30d DOUBLE PRECISION,
                        daily_return    DOUBLE PRECISION,
                        volatility_7d   DOUBLE PRECISION,
                        record_timestamp VARCHAR(20) NOT NULL,
                        CONSTRAINT uix_symbol_date UNIQUE (symbol_id, record_timestamp)
                    );
                """))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pg_timestamp ON fact_market_data(record_timestamp);"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pg_symbol_id ON fact_market_data(symbol_id);"))
            else:
                # SQLite DDL
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS dim_symbol (
                        symbol_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol_code VARCHAR(10)  UNIQUE NOT NULL,
                        asset_name  VARCHAR(50),
                        asset_type  VARCHAR(20)  DEFAULT 'crypto'
                    );
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
                    );
                """))
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

        logger.info("Tables and indexes created/verified.")

    # ── Dimension upsert ──────────────────────────────────────────────────────

    def upsert_symbols(self, df: pd.DataFrame) -> dict:
        """
        Ensure all symbols in df exist in dim_symbol.
        Returns {SYMBOL_CODE: symbol_id} mapping.
        """
        codes = df["symbol"].str.upper().unique()
        symbol_map: dict = {}

        with self.engine.begin() as conn:
            for code in codes:
                # Standard SQL ON CONFLICT DO NOTHING works on both Postgres and SQLite 3.24+
                conn.execute(
                    text(
                        "INSERT INTO dim_symbol (symbol_code, asset_name, asset_type) "
                        "VALUES (:code, :name, 'crypto') "
                        "ON CONFLICT (symbol_code) DO NOTHING"
                    ),
                    {"code": code, "name": code.capitalize()},
                )

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
        Uses true IN-PLACE ON CONFLICT DO UPDATE on both SQLite and PostgreSQL.
        Guarantees:
        - Primary key fact_id never skips numbers or changes on updates.
        - Idempotent and thread-safe.
        - Granularity: one record per (symbol, calendar date YYYY-MM-DD).
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

        # 4. True In-Place UPSERT via SQLAlchemy — works across SQLite & PostgreSQL
        upsert_query = text("""
            INSERT INTO fact_market_data (
                symbol_id, price_usd, market_cap, volume_24h, change_24h,
                rolling_avg_7d, rolling_avg_30d, daily_return, volatility_7d,
                record_timestamp
            ) VALUES (
                :symbol_id, :price_usd, :market_cap, :volume_24h, :change_24h,
                :rolling_avg_7d, :rolling_avg_30d, :daily_return, :volatility_7d,
                :record_timestamp
            )
            ON CONFLICT (symbol_id, record_timestamp) DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                market_cap = EXCLUDED.market_cap,
                volume_24h = EXCLUDED.volume_24h,
                change_24h = EXCLUDED.change_24h,
                rolling_avg_7d = EXCLUDED.rolling_avg_7d,
                rolling_avg_30d = EXCLUDED.rolling_avg_30d,
                daily_return = EXCLUDED.daily_return,
                volatility_7d = EXCLUDED.volatility_7d
        """)

        with self.engine.begin() as conn:
            rows_loaded = 0
            for _, row in df.iterrows():
                conn.execute(
                    upsert_query,
                    {
                        "symbol_id": int(row["symbol_id"]),
                        "price_usd": safe_float(row.get("price_usd")),
                        "market_cap": safe_float(row.get("market_cap")),
                        "volume_24h": safe_float(row.get("volume_24h")),
                        "change_24h": safe_float(row.get("change_24h")),
                        "rolling_avg_7d": safe_float(row.get("rolling_avg_7d")),
                        "rolling_avg_30d": safe_float(row.get("rolling_avg_30d")),
                        "daily_return": safe_float(row.get("daily_return"), default=0.0),
                        "volatility_7d": safe_float(row.get("volatility_7d"), default=0.0),
                        "record_timestamp": str(row["record_timestamp"]),
                    },
                )
                rows_loaded += 1

            logger.info("Loaded %d rows into fact_market_data (True in-place UPSERT).", rows_loaded)

    # ── Maintenance ───────────────────────────────────────────────────────────

    def clear_all_data(self) -> None:
        """DANGER: Delete ALL data. Use only for dev/reset."""
        with self.engine.begin() as conn:
            if not self.is_postgres:
                conn.execute(text("PRAGMA foreign_keys = OFF"))
            conn.execute(text("DELETE FROM fact_market_data"))
            conn.execute(text("DELETE FROM dim_symbol"))
            if not self.is_postgres:
                conn.execute(text("PRAGMA foreign_keys = ON"))
        logger.warning("ALL DATA DELETED from fact_market_data and dim_symbol!")

    # ── Orchestrator ──────────────────────────────────────────────────────────

    def run(self, transformed_df: pd.DataFrame) -> None:
        """Full load pipeline: create tables → load fact."""
        self.create_tables()
        self.load_fact(transformed_df)