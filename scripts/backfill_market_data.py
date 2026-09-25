# scripts/backfill_market_data.py
"""
Backfills missing market_cap, volume_24h, and change_24h for historical rows
that currently have NULLs.

Uses CoinGecko /coins/{id}/market_chart endpoint which returns:
  - prices     : [[unix_ms, price], ...]
  - market_caps: [[unix_ms, market_cap], ...]
  - total_volumes: [[unix_ms, volume], ...]

Strategy:
  1. Query DB for all rows where market_cap IS NULL.
  2. For each (symbol, date_range), fetch market_chart data from CoinGecko.
  3. Merge by date and UPDATE the rows.
  4. Recompute daily_return / rolling averages after filling.

Run once: python scripts/backfill_market_data.py
"""

import os
import sys
import time
import logging
import sqlite3
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.utils import setup_logging

load_dotenv(override=True)
setup_logging()
logger = logging.getLogger("backfill")

BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
DB_PATH = "crypto_pipeline.db"

# CoinGecko coin IDs (symbol_code → coin_id)
SYMBOL_TO_COIN_ID = {
    "BITCOIN": "bitcoin",
    "ETHEREUM": "ethereum",
    "SOLANA": "solana",
}

RATE_LIMIT_DELAY = 12  # seconds between API calls (free tier: ~5/min)


def fetch_market_chart(coin_id: str, days: int = 90) -> pd.DataFrame:
    """
    Fetch daily OHLCV from CoinGecko market_chart endpoint.
    Returns DataFrame with columns: date, price, market_cap, volume_24h.
    """
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days, "interval": "daily"}

    for attempt in range(5):
        try:
            logger.info("Fetching market_chart for %s (days=%d, attempt=%d)...", coin_id, days, attempt + 1)
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                logger.warning("Rate limited (429). Waiting %ds before retry...", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            prices = pd.DataFrame(data["prices"], columns=["ts_ms", "price"])
            mcaps = pd.DataFrame(data["market_caps"], columns=["ts_ms", "market_cap"])
            vols = pd.DataFrame(data["total_volumes"], columns=["ts_ms", "volume_24h"])

            df = prices.merge(mcaps, on="ts_ms").merge(vols, on="ts_ms")
            df["date"] = pd.to_datetime(df["ts_ms"], unit="ms").dt.date
            df = df.groupby("date").agg(
                price=("price", "last"),
                market_cap=("market_cap", "last"),
                volume_24h=("volume_24h", "last"),
            ).reset_index()

            logger.info("Fetched %d daily records for %s.", len(df), coin_id)
            return df

        except requests.exceptions.RequestException as exc:
            logger.error("Request failed (attempt %d): %s", attempt + 1, exc)
            time.sleep(RATE_LIMIT_DELAY)

    raise RuntimeError(f"Failed to fetch market_chart for {coin_id} after 5 attempts.")


def get_null_rows(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return all fact_market_data rows where market_cap IS NULL."""
    df = pd.read_sql(
        """
        SELECT f.fact_id, d.symbol_code, f.record_timestamp
        FROM fact_market_data f
        JOIN dim_symbol d ON f.symbol_id = d.symbol_id
        WHERE f.market_cap IS NULL
        ORDER BY d.symbol_code, f.record_timestamp
        """,
        conn,
    )
    return df


def update_row(conn: sqlite3.Connection, fact_id: int, price: float, market_cap: float, volume: float) -> None:
    conn.execute(
        """
        UPDATE fact_market_data
        SET price_usd  = ?,
            market_cap = ?,
            volume_24h = ?
        WHERE fact_id = ?
        """,
        (price, market_cap, volume, fact_id),
    )


def recompute_features(conn: sqlite3.Connection) -> None:
    """
    After backfilling raw values, recompute daily_return, rolling averages,
    and volatility using Python/Pandas (SQLite has no window functions for std).
    """
    logger.info("Recomputing financial features for all rows...")

    df = pd.read_sql(
        """
        SELECT f.fact_id, d.symbol_code AS symbol, f.record_timestamp, f.price_usd
        FROM fact_market_data f
        JOIN dim_symbol d ON f.symbol_id = d.symbol_id
        ORDER BY d.symbol_code, f.record_timestamp
        """,
        conn,
    )

    df["record_timestamp"] = pd.to_datetime(df["record_timestamp"])
    df = df.sort_values(["symbol", "record_timestamp"])

    df["rolling_avg_7d"] = df.groupby("symbol")["price_usd"].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    )
    df["rolling_avg_30d"] = df.groupby("symbol")["price_usd"].transform(
        lambda x: x.rolling(30, min_periods=1).mean()
    )
    df["daily_return"] = df.groupby("symbol")["price_usd"].pct_change() * 100
    df["volatility_7d"] = df.groupby("symbol")["daily_return"].transform(
        lambda x: x.rolling(7, min_periods=2).std()
    )

    df["daily_return"] = df["daily_return"].fillna(0.0)
    df["volatility_7d"] = df["volatility_7d"].fillna(0.0)

    updated = 0
    for _, row in df.iterrows():
        conn.execute(
            """
            UPDATE fact_market_data
            SET rolling_avg_7d  = ?,
                rolling_avg_30d = ?,
                daily_return    = ?,
                volatility_7d   = ?
            WHERE fact_id = ?
            """,
            (
                float(row["rolling_avg_7d"]),
                float(row["rolling_avg_30d"]),
                float(row["daily_return"]),
                float(row["volatility_7d"]),
                int(row["fact_id"]),
            ),
        )
        updated += 1

    conn.commit()
    logger.info("Recomputed features for %d rows.", updated)


def run_backfill() -> None:
    conn = sqlite3.connect(DB_PATH)

    null_rows = get_null_rows(conn)
    if null_rows.empty:
        logger.info("No NULL rows found — nothing to backfill. Done!")
        conn.close()
        return

    logger.info("Found %d rows with NULL market_cap.", len(null_rows))

    # Group by symbol and fetch market_chart once per symbol
    symbols_needed = null_rows["symbol_code"].unique()
    chart_cache: dict = {}

    for symbol_code in symbols_needed:
        coin_id = SYMBOL_TO_COIN_ID.get(symbol_code.upper())
        if not coin_id:
            logger.warning("No CoinGecko ID mapping for %s — skipping.", symbol_code)
            continue

        try:
            chart_df = fetch_market_chart(coin_id, days=90)
            chart_cache[symbol_code] = chart_df
        except RuntimeError as exc:
            logger.error("Skipping %s: %s", symbol_code, exc)

        # Respect free-tier rate limit
        logger.info("Waiting %ds before next API call...", RATE_LIMIT_DELAY)
        time.sleep(RATE_LIMIT_DELAY)

    # Update each NULL row
    filled = 0
    for _, null_row in null_rows.iterrows():
        symbol = null_row["symbol_code"]
        row_date = date.fromisoformat(str(null_row["record_timestamp"])[:10])

        if symbol not in chart_cache:
            continue

        chart = chart_cache[symbol]
        match = chart[chart["date"] == row_date]

        if match.empty:
            logger.warning("No market_chart data for %s on %s — leaving NULL.", symbol, row_date)
            continue

        m = match.iloc[0]
        update_row(conn, int(null_row["fact_id"]), float(m["price"]), float(m["market_cap"]), float(m["volume_24h"]))
        filled += 1
        logger.info("Updated %s %s: price=%.2f, mcap=%.0f, vol=%.0f", symbol, row_date, m["price"], m["market_cap"], m["volume_24h"])

    conn.commit()
    logger.info("Backfilled %d / %d rows.", filled, len(null_rows))

    # Recompute all derived features with corrected data
    recompute_features(conn)

    conn.close()
    logger.info("Backfill complete!")


if __name__ == "__main__":
    run_backfill()
