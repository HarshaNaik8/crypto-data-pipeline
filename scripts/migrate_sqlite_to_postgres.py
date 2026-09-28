# scripts/migrate_sqlite_to_postgres.py
"""
Migration utility: Copies all existing dimensional and fact records
from the local SQLite database into Neon Cloud PostgreSQL.

Guarantees:
- Zero data loss: Preserves all historical records from Day 1 to present.
- Idempotent: Uses ON CONFLICT to avoid duplicate primary keys or conflicts.
"""

import sys
import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Safe UTF-8 console output for Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

LOCAL_SQLITE_PATH = "crypto_pipeline.db"
NEON_URL = os.getenv("NEON_DB_URL") or os.getenv("DB_CONNECTION_STRING")


def migrate():
    if not os.path.exists(LOCAL_SQLITE_PATH):
        print(f"Error: Local SQLite database '{LOCAL_SQLITE_PATH}' not found!")
        sys.exit(1)

    if not NEON_URL or "postgresql" not in NEON_URL:
        print("Error: NEON_DB_URL or PostgreSQL DB_CONNECTION_STRING not found in environment!")
        sys.exit(1)

    print("Connecting to local SQLite database...")
    sqlite_conn = sqlite3.connect(LOCAL_SQLITE_PATH)

    print("Connecting to Neon Cloud PostgreSQL database...")
    pg_engine = create_engine(NEON_URL)

    # 1. Create tables in PostgreSQL if not exist
    with pg_engine.begin() as conn:
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
    print("Neon PostgreSQL schema verified.")

    # 2. Migrate dim_symbol
    dim_df = pd.read_sql("SELECT symbol_id, symbol_code, asset_name, asset_type FROM dim_symbol", sqlite_conn)
    print(f"Migrating {len(dim_df)} records from dim_symbol...")
    with pg_engine.begin() as conn:
        for _, row in dim_df.iterrows():
            conn.execute(text("""
                INSERT INTO dim_symbol (symbol_id, symbol_code, asset_name, asset_type)
                VALUES (:symbol_id, :symbol_code, :asset_name, :asset_type)
                ON CONFLICT (symbol_code) DO NOTHING
            """), dict(row))
        conn.execute(text("SELECT setval(pg_get_serial_sequence('dim_symbol', 'symbol_id'), COALESCE(MAX(symbol_id), 1)) FROM dim_symbol;"))

    # 3. Migrate fact_market_data
    fact_df = pd.read_sql("SELECT * FROM fact_market_data ORDER BY record_timestamp, symbol_id", sqlite_conn)
    print(f"Migrating {len(fact_df)} records from fact_market_data...")

    with pg_engine.begin() as conn:
        for _, row in fact_df.iterrows():
            row_dict = dict(row)
            conn.execute(text("""
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
            """), row_dict)

    sqlite_conn.close()

    # 4. Verify in Postgres
    with pg_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_market_data")).scalar()
        sample_rows = conn.execute(text("""
            SELECT f.fact_id, d.symbol_code, f.price_usd, f.daily_return, f.record_timestamp
            FROM fact_market_data f
            JOIN dim_symbol d ON f.symbol_id = d.symbol_id
            ORDER BY f.record_timestamp, d.symbol_code
        """)).fetchall()

        print(f"SUCCESS: Migration Complete! Neon fact_market_data now has {count} rows.")
        print("Sample data in Neon PostgreSQL:")
        for r in sample_rows:
            print(f"  fact_id={r[0]} | {r[1]} | ${r[2]:,.2f} | return={r[3]:.2f}% | date={r[4]}")


if __name__ == "__main__":
    migrate()
