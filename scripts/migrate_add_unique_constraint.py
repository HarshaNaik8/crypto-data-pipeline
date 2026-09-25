# scripts/migrate_add_unique_constraint.py
"""
One-time migration: adds UNIQUE(symbol_id, record_timestamp) constraint
to the existing fact_market_data table.

SQLite does not support ALTER TABLE ADD CONSTRAINT, so we recreate the table:
  1. Rename old table to _old
  2. Create new table with UNIQUE constraint
  3. Copy data (deduplicating — keep the last record per day per symbol)
  4. Drop old table
  5. Recreate indexes

Safe to run multiple times (idempotent).
"""

import sqlite3
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.utils import setup_logging

setup_logging()
logger = logging.getLogger("migration")

DB_PATH = "crypto_pipeline.db"


def run_migration() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check if migration already done (UNIQUE index exists)
    cur.execute("PRAGMA index_list('fact_market_data')")
    indexes = [row[1] for row in cur.fetchall()]
    if "uix_symbol_date" in indexes:
        logger.info("Migration already applied (uix_symbol_date exists). Nothing to do.")
        conn.close()
        return

    logger.info("Starting migration: adding UNIQUE constraint to fact_market_data ...")

    cur.executescript("""
        -- Step 1: Rename existing table
        ALTER TABLE fact_market_data RENAME TO fact_market_data_old;

        -- Step 2: Create new table with UNIQUE constraint and REAL column types
        CREATE TABLE fact_market_data (
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

        -- Step 3: Copy data, deduplicating by keeping the MAX fact_id per (symbol, date)
        INSERT INTO fact_market_data (
            fact_id, symbol_id, price_usd, market_cap, volume_24h, change_24h,
            rolling_avg_7d, rolling_avg_30d, daily_return, volatility_7d, record_timestamp
        )
        SELECT fact_id, symbol_id, 
               CAST(price_usd AS REAL),
               CAST(market_cap AS REAL),
               CAST(volume_24h AS REAL),
               CAST(change_24h AS REAL),
               CAST(rolling_avg_7d AS REAL),
               CAST(rolling_avg_30d AS REAL),
               CAST(daily_return AS REAL),
               CAST(volatility_7d AS REAL),
               record_timestamp
        FROM fact_market_data_old
        WHERE fact_id IN (
            SELECT MAX(fact_id)
            FROM fact_market_data_old
            GROUP BY symbol_id, DATE(record_timestamp)
        );

        -- Step 4: Recreate indexes
        CREATE UNIQUE INDEX IF NOT EXISTS uix_symbol_date
            ON fact_market_data(symbol_id, record_timestamp);
        CREATE INDEX IF NOT EXISTS idx_timestamp
            ON fact_market_data(record_timestamp);
        CREATE INDEX IF NOT EXISTS idx_symbol_id
            ON fact_market_data(symbol_id);

        -- Step 5: Drop old table
        DROP TABLE fact_market_data_old;
    """)

    conn.commit()

    # Verify
    cur.execute("SELECT COUNT(*) FROM fact_market_data")
    count = cur.fetchone()[0]
    logger.info("Migration complete. %d rows in fact_market_data.", count)

    cur.execute("PRAGMA index_list('fact_market_data')")
    for row in cur.fetchall():
        logger.info("  Index: %s (unique=%s)", row[1], bool(row[2]))

    conn.close()


if __name__ == "__main__":
    run_migration()
