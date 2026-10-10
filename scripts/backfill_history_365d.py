# scripts/backfill_history_365d.py
"""
Historical Data Backfill Engine (365 Days)
Fixes the "5-8 Day Cold-Start" Problem:
- Fetches 365 daily data points from CoinGecko market_chart API.
- Computes genuine rolling 7d/30d moving averages, daily returns, and 7d rolling volatility.
- Upserts all 365 records per coin into Neon PostgreSQL (and local SQLite).
- Refreshes vw_weekly_trends with ISO-8601 week formatting.
"""

import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()
from src.utils import create_db_engine

COIN_CONFIG = {
    "BITCOIN": {"id": "bitcoin", "symbol_id": 1},
    "ETHEREUM": {"id": "ethereum", "symbol_id": 2},
    "SOLANA": {"id": "solana", "symbol_id": 3},
}

BASE_URL = "https://api.coingecko.com/api/v3"


def fetch_coin_history(coin_id: str, days: int = 365) -> pd.DataFrame:
    """Fetch daily market chart history from CoinGecko public demo endpoint."""
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": str(days), "interval": "daily"}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json"}

    print(f"Fetching {days} days of history for {coin_id} from CoinGecko...")
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    prices = data.get("prices", [])
    caps = {item[0]: item[1] for item in data.get("market_caps", [])}
    vols = {item[0]: item[1] for item in data.get("total_volumes", [])}

    records = []
    for ts, price in prices:
        # Convert ms timestamp to UTC date string
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        records.append({
            "record_timestamp": dt,
            "price_usd": float(price),
            "market_cap": float(caps.get(ts, 0.0)),
            "volume_24h": float(vols.get(ts, 0.0)),
        })

    df = pd.DataFrame(records)
    # Deduplicate on date keeping latest in case of intra-day overlap
    df = df.drop_duplicates(subset=["record_timestamp"], keep="last").sort_values("record_timestamp").reset_index(drop=True)
    return df


def compute_financial_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute true rolling averages, daily percentage return, and 7-day volatility."""
    df = df.copy()
    
    # 1. Daily return percentage
    df["daily_return"] = df["price_usd"].pct_change() * 100.0
    df["daily_return"] = df["daily_return"].fillna(0.0)

    # 2. 24h change (from CoinGecko definition)
    df["change_24h"] = df["daily_return"]

    # 3. Rolling Moving Averages (true 7-day and 30-day)
    df["rolling_avg_7d"] = df["price_usd"].rolling(window=7, min_periods=1).mean()
    df["rolling_avg_30d"] = df["price_usd"].rolling(window=30, min_periods=1).mean()

    # 4. 7-Day Rolling Volatility (stdev of daily returns)
    df["volatility_7d"] = df["daily_return"].rolling(window=7, min_periods=2).std().fillna(0.0)

    return df


def backfill_all():
    db_url = os.getenv("NEON_DB_URL") or os.getenv("DB_CONNECTION_STRING")
    if not db_url:
        print("ERROR: No database URL configured in environment!")
        return

    print("Connecting to Database...")
    engine = create_db_engine(db_url)
    is_postgres = "postgresql" in db_url

    # Check / Add data_source column if it doesn't exist
    if is_postgres:
        with engine.begin() as conn:
            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='fact_market_data' AND column_name='data_source'
                    ) THEN
                        ALTER TABLE fact_market_data ADD COLUMN data_source VARCHAR(20) DEFAULT 'coingecko';
                    END IF;
                END $$;
            """))
            print("Verified 'data_source' column in Neon PostgreSQL fact_market_data.")

    total_loaded = 0
    for symbol_code, info in COIN_CONFIG.items():
        coin_id = info["id"]
        symbol_id = info["symbol_id"]

        try:
            raw_df = fetch_coin_history(coin_id, days=365)
            enriched_df = compute_financial_features(raw_df)
            enriched_df["symbol_id"] = symbol_id
            enriched_df["data_source"] = "coingecko"

            print(f"Loaded {len(enriched_df)} historical records for {symbol_code} ({enriched_df['record_timestamp'].min()} to {enriched_df['record_timestamp'].max()})")

            # Upsert into fact_market_data
            upsert_sql = """
                INSERT INTO fact_market_data (
                    symbol_id, price_usd, market_cap, volume_24h, change_24h,
                    rolling_avg_7d, rolling_avg_30d, daily_return, volatility_7d,
                    record_timestamp, data_source
                ) VALUES (
                    :symbol_id, :price_usd, :market_cap, :volume_24h, :change_24h,
                    :rolling_avg_7d, :rolling_avg_30d, :daily_return, :volatility_7d,
                    :record_timestamp, :data_source
                )
                ON CONFLICT (symbol_id, record_timestamp) DO UPDATE SET
                    price_usd       = EXCLUDED.price_usd,
                    market_cap      = EXCLUDED.market_cap,
                    volume_24h      = EXCLUDED.volume_24h,
                    change_24h      = EXCLUDED.change_24h,
                    rolling_avg_7d  = EXCLUDED.rolling_avg_7d,
                    rolling_avg_30d = EXCLUDED.rolling_avg_30d,
                    daily_return    = EXCLUDED.daily_return,
                    volatility_7d   = EXCLUDED.volatility_7d,
                    data_source     = EXCLUDED.data_source;
            """ if is_postgres else """
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
                    price_usd       = EXCLUDED.price_usd,
                    market_cap      = EXCLUDED.market_cap,
                    volume_24h      = EXCLUDED.volume_24h,
                    change_24h      = EXCLUDED.change_24h,
                    rolling_avg_7d  = EXCLUDED.rolling_avg_7d,
                    rolling_avg_30d = EXCLUDED.rolling_avg_30d,
                    daily_return    = EXCLUDED.daily_return,
                    volatility_7d   = EXCLUDED.volatility_7d;
            """

            with engine.begin() as conn:
                for _, row in enriched_df.iterrows():
                    params = {
                        "symbol_id": int(row["symbol_id"]),
                        "price_usd": float(row["price_usd"]),
                        "market_cap": float(row["market_cap"]),
                        "volume_24h": float(row["volume_24h"]),
                        "change_24h": float(row["change_24h"]),
                        "rolling_avg_7d": float(row["rolling_avg_7d"]),
                        "rolling_avg_30d": float(row["rolling_avg_30d"]),
                        "daily_return": float(row["daily_return"]),
                        "volatility_7d": float(row["volatility_7d"]),
                        "record_timestamp": str(row["record_timestamp"]),
                    }
                    if is_postgres:
                        params["data_source"] = str(row["data_source"])
                    conn.execute(text(upsert_sql), params)

            total_loaded += len(enriched_df)
            print(f"Successfully upserted {len(enriched_df)} rows for {symbol_code} into database.")

            # Gentle rate-limiting sleep between coins
            time.sleep(3)

        except Exception as exc:
            print(f"ERROR backfilling {symbol_code}: {exc}")

    # Refresh vw_weekly_trends with ISO-8601 formatting
    print("\nRefreshing vw_weekly_trends with ISO-8601 week formatting...")
    from src.weekly_aggregate import create_weekly_aggregate_view
    create_weekly_aggregate_view(db_url)

    # Verification
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_market_data")).scalar()
        date_span = conn.execute(text("SELECT MIN(record_timestamp), MAX(record_timestamp) FROM fact_market_data")).fetchall()
        print("\n========================================================")
        print(" BACKFILL SUMMARY (365 DAYS COMPLETE)")
        print("========================================================")
        print(f"Total Rows in fact_market_data: {count}")
        print(f"Historical Date Range:          {date_span[0][0]} to {date_span[0][1]}")
        print("========================================================")


if __name__ == "__main__":
    backfill_all()
