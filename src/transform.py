# src/transform.py
"""
Data transformer: validates raw API data, loads historical context,
engineers financial features (rolling averages, daily return, volatility).

Key design decisions:
  - Loads historical data from DB so rolling windows have enough context.
  - Returns ONLY the current-day rows after enrichment (not all history).
    The full combined dataset is used internally for calculation only.
  - Symbol→ID mapping is read dynamically from dim_symbol — never hardcoded.
"""

import os
import logging
import sqlite3

import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional

from src.utils import get_db_path, safe_float

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Professional data transformer with feature engineering.
    Handles nulls, rolling averages, returns, and volatility.
    Loads historical data from database for accurate time-series calculations.
    """

    def __init__(self, raw_df: pd.DataFrame, connection_string: str = "sqlite:///crypto_pipeline.db"):
        self.raw_df = raw_df.copy()
        self.cleaned_df: Optional[pd.DataFrame] = None
        self.enriched_df: Optional[pd.DataFrame] = None
        self.connection_string = connection_string
        self.db_path = get_db_path(connection_string)

    # ── Validation ────────────────────────────────────────────────────────────

    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data quality checks; drops bad rows rather than raising."""
        if df.empty:
            raise ValueError("DataFrame is empty — nothing to transform.")

        required = ["symbol", "price_usd", "timestamp"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)

        bad_price = df["price_usd"] <= 0
        if bad_price.any():
            logger.warning("Dropping %d rows with zero/negative price_usd", bad_price.sum())
            df = df[~bad_price].copy()

        return df

    # ── Historical loader ─────────────────────────────────────────────────────

    def _load_historical_data(self) -> pd.DataFrame:
        """
        Load past records from fact_market_data for rolling-feature context.
        Uses raw sqlite3 (not SQLAlchemy) — pd.read_sql requires a DBAPI connection.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='fact_market_data'"
            )
            if not cursor.fetchone():
                logger.info("fact_market_data does not exist yet (first run).")
                conn.close()
                return pd.DataFrame()

            hist_df = pd.read_sql(
                """
                SELECT
                    d.symbol_code  AS symbol,
                    f.price_usd,
                    f.market_cap,
                    f.volume_24h,
                    f.change_24h,
                    f.record_timestamp AS timestamp
                FROM fact_market_data f
                JOIN dim_symbol d ON f.symbol_id = d.symbol_id
                ORDER BY f.record_timestamp
                """,
                conn,
            )
            conn.close()

            if hist_df.empty:
                logger.info("No historical records in DB yet.")
                return pd.DataFrame()

            hist_df["timestamp"] = pd.to_datetime(hist_df["timestamp"])
            logger.info(
                "Loaded %d historical records (%s → %s)",
                len(hist_df),
                hist_df["timestamp"].min().date(),
                hist_df["timestamp"].max().date(),
            )
            return hist_df

        except Exception as exc:
            logger.warning("Could not load historical data: %s — using current data only.", exc)
            return pd.DataFrame()

    # ── Null handling ─────────────────────────────────────────────────────────

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Forward-fill price/volume per symbol; drop rows where price is still null."""
        df = df.copy().sort_values(["symbol", "timestamp"])
        for col in ["price_usd", "market_cap", "volume_24h"]:
            if col in df.columns:
                df[col] = df.groupby("symbol")[col].ffill()
        df = df.dropna(subset=["price_usd"])
        logger.info("After null handling: %d rows", len(df))
        return df

    # ── Feature engineering ───────────────────────────────────────────────────

    def _calculate_rolling_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer time-series features per symbol:
          - rolling_avg_7d   : 7-day price moving average
          - rolling_avg_30d  : 30-day price moving average
          - daily_return     : % price change vs previous row (pct_change * 100)
          - volatility_7d    : 7-day rolling std of daily_return
        """
        df = df.copy().sort_values(["symbol", "timestamp"])

        df["rolling_avg_7d"] = df.groupby("symbol")["price_usd"].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )
        df["rolling_avg_30d"] = df.groupby("symbol")["price_usd"].transform(
            lambda x: x.rolling(window=30, min_periods=1).mean()
        )
        df["daily_return"] = (
            df.groupby("symbol")["price_usd"].pct_change() * 100
        )
        df["volatility_7d"] = df.groupby("symbol")["daily_return"].transform(
            lambda x: x.rolling(window=7, min_periods=2).std()
        )

        # Sensible defaults for first-row NaNs
        df["rolling_avg_7d"] = df["rolling_avg_7d"].fillna(df["price_usd"])
        df["rolling_avg_30d"] = df["rolling_avg_30d"].fillna(df["price_usd"])
        df["daily_return"] = df["daily_return"].fillna(0.0)
        df["volatility_7d"] = df["volatility_7d"].fillna(0.0)

        non_zero = (df["daily_return"] != 0).sum()
        logger.info("Feature engineering done — %d rows with non-zero daily_return", non_zero)
        return df

    # ── Symbol-ID mapping (dynamic, not hardcoded) ────────────────────────────

    def _add_dimension_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map symbol strings → symbol_id integers using the actual dim_symbol table.
        Falls back to 0 for unknown symbols (will be created by the loader anyway).
        """
        df = df.copy()

        if "symbol_id" in df.columns:
            # Already mapped (came from historical load — contains symbol_id from DB)
            return df

        symbol_map: dict = {}
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute("SELECT symbol_code, symbol_id FROM dim_symbol")
            symbol_map = {row[0].upper(): row[1] for row in cursor.fetchall()}
            conn.close()
            logger.info("Loaded symbol map from DB: %s", symbol_map)
        except Exception as exc:
            logger.warning("Could not load symbol map from DB: %s — using 0 as placeholder.", exc)

        df["symbol_id"] = df["symbol"].str.upper().map(symbol_map).fillna(0).astype(int)
        return df

    # ── Orchestrator ──────────────────────────────────────────────────────────

    def transform(self) -> pd.DataFrame:
        """
        Full transformation pipeline.
        Returns only TODAY's enriched rows — the historical rows are used
        internally for rolling calculations but not re-loaded into the DB.
        """
        logger.info("=" * 60)
        logger.info("STARTING TRANSFORMATION PIPELINE")
        logger.info("=" * 60)

        # Step 0: Capture which dates are "new" (from current extraction)
        current_df = self.raw_df.copy()
        current_df["timestamp"] = pd.to_datetime(current_df["timestamp"])
        current_dates = set(current_df["timestamp"].dt.date.unique())

        # Step 1: Load historical context
        hist_df = self._load_historical_data()

        # Step 2: Combine for rolling calculations
        if not hist_df.empty:
            combined = pd.concat([hist_df, current_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["symbol", "timestamp"], keep="last")
        else:
            combined = current_df.copy()
            logger.info("No historical data — first run mode.")

        # Step 3: Validate & clean
        combined = self._validate_data(combined)
        combined = self._handle_nulls(combined)

        # Step 4: Feature engineering on full combined set
        enriched = self._calculate_rolling_features(combined)

        # Step 5: Dimension mapping
        enriched = self._add_dimension_mapping(enriched)

        # Step 6: Ensure all output columns exist
        final_columns = [
            "timestamp", "symbol", "symbol_id", "price_usd",
            "market_cap", "volume_24h", "change_24h",
            "rolling_avg_7d", "rolling_avg_30d",
            "daily_return", "volatility_7d",
        ]
        for col in final_columns:
            if col not in enriched.columns:
                enriched[col] = None

        enriched = enriched[final_columns].copy()

        # Step 7: Keep only the NEW rows for loading into DB
        new_rows = enriched[enriched["timestamp"].dt.date.isin(current_dates)].copy()

        logger.info("=" * 60)
        logger.info("TRANSFORMATION COMPLETE")
        logger.info("  Total combined rows : %d", len(enriched))
        logger.info("  New rows to load    : %d", len(new_rows))
        logger.info(
            "  Date range (new)    : %s",
            sorted(new_rows["timestamp"].dt.date.unique()),
        )
        logger.info("=" * 60)

        # Save parquet backup (full enriched set for analysis/audit)
        os.makedirs("data/transformed", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        parquet_path = f"data/transformed/transformed_{ts}.parquet"
        enriched.to_parquet(parquet_path, index=False)
        logger.info("Transformed data saved to %s", parquet_path)

        self.enriched_df = new_rows
        return new_rows


# ── Standalone test ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import glob
    from src.utils import setup_logging

    setup_logging()

    raw_files = sorted(glob.glob("data/raw/raw_*.json"))
    if not raw_files:
        logger.error("No raw files found. Run extract.py first.")
        raise SystemExit(1)

    latest_raw = raw_files[-1]
    logger.info("Loading raw data from %s", latest_raw)
    raw_df = pd.read_json(latest_raw)

    transformer = DataTransformer(raw_df)
    result = transformer.transform()
    print(result[["symbol", "timestamp", "price_usd", "daily_return", "volatility_7d"]].to_string())