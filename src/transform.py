# src/transform.py

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class DataTransformer:
    """
    Professional data transformer with feature engineering.
    Handles nulls, rolling averages, returns, and volatility.
    """
    
    def __init__(self, raw_df: pd.DataFrame):
        self.raw_df = raw_df.copy()
        self.cleaned_df = None
        self.enriched_df = None
        
    def _validate_data(self) -> bool:
        """Basic data quality checks."""
        if self.raw_df.empty:
            raise ValueError("Raw DataFrame is empty")
        
        # Check required columns
        required = ['symbol', 'price_usd', 'timestamp']
        missing = [col for col in required if col not in self.raw_df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        # Ensure price is positive
        if (self.raw_df['price_usd'] <= 0).any():
            logger.warning("Negative or zero prices found, dropping rows")
            self.raw_df = self.raw_df[self.raw_df['price_usd'] > 0]
        
        # Convert timestamp to datetime
        self.raw_df['timestamp'] = pd.to_datetime(self.raw_df['timestamp'])
        
        return True
    
    def _handle_nulls(self) -> pd.DataFrame:
        """
        Forward-fill nulls for price and volume.
        In production, you'd also interpolate or use last-observation-carried-forward.
        """
        df = self.raw_df.copy()
        
        # Sort by symbol and timestamp
        df = df.sort_values(['symbol', 'timestamp'])
        
        # For each symbol, forward-fill missing values
        df[['price_usd', 'market_cap', 'volume_24h']] = df.groupby('symbol')[['price_usd', 'market_cap', 'volume_24h']].ffill()
        
        # Drop any remaining nulls (only at the very beginning of the series)
        df = df.dropna(subset=['price_usd', 'volume_24h'])
        
        logger.info(f"Null handling complete. Shape: {df.shape}")
        return df
    
    def _calculate_rolling_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer features:
        - rolling_avg_7d: 7-day moving average of price
        - rolling_avg_30d: 30-day moving average
        - daily_return: percentage change from previous day
        - volatility: 7-day standard deviation of daily returns
        """
        # Since we only have hourly data, we simulate 'days' by grouping by date.
        # For a real pipeline with daily data, you'd resample. Here we'll use rolling(7) on hourly data.
        # To make it realistic, we treat each fetch as a new row.
        
        df = df.sort_values(['symbol', 'timestamp'])
        
        # 7-day rolling average (using last 7 rows per symbol)
        df['rolling_avg_7d'] = df.groupby('symbol')['price_usd'].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )
        
        # 30-day rolling average (using last 30 rows)
        df['rolling_avg_30d'] = df.groupby('symbol')['price_usd'].transform(
            lambda x: x.rolling(window=30, min_periods=1).mean()
        )
        
        # Daily return (percentage change from previous row per symbol)
        df['daily_return'] = df.groupby('symbol')['price_usd'].pct_change() * 100
        
        # 7-day volatility (std dev of daily returns, rolling)
        df['volatility_7d'] = df.groupby('symbol')['daily_return'].transform(
            lambda x: x.rolling(window=7, min_periods=1).std()
        )
        
        # Fill NaN from rolling calculations (first few rows) with 0
        df['rolling_avg_7d'] = df['rolling_avg_7d'].fillna(df['price_usd'])
        df['rolling_avg_30d'] = df['rolling_avg_30d'].fillna(df['price_usd'])
        df['daily_return'] = df['daily_return'].fillna(0)
        df['volatility_7d'] = df['volatility_7d'].fillna(0)
        
        logger.info("Feature engineering complete.")
        return df
    
    def _add_dimension_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare for SQL star schema:
        - Map symbol codes to integer IDs (in production, you'd query the dimension table).
        - Here we create a static mapping for now.
        """
        # Hardcoded mapping – in production, this would come from dim_symbol
        symbol_map = {
            'BITCOIN': 1,
            'ETHEREUM': 2,
            'SOLANA': 3
        }
        df['symbol_id'] = df['symbol'].map(symbol_map)
        
        # If any symbol is unmapped, assign a default (should not happen)
        df['symbol_id'] = df['symbol_id'].fillna(0).astype(int)
        
        return df
    
    def transform(self) -> pd.DataFrame:
        """Orchestrate the entire transformation pipeline."""
        logger.info("Starting transformation pipeline...")
        
        # Step 1: Validate
        self._validate_data()
        
        # Step 2: Handle nulls
        cleaned = self._handle_nulls()
        self.cleaned_df = cleaned
        
        # Step 3: Feature engineering
        enriched = self._calculate_rolling_features(cleaned)
        
        # Step 4: Dimension mapping
        enriched = self._add_dimension_mapping(enriched)
        
        # Step 5: Final column selection (order matters for SQL)
        final_columns = [
            'timestamp', 'symbol', 'symbol_id', 'price_usd', 
            'market_cap', 'volume_24h', 'change_24h',
            'rolling_avg_7d', 'rolling_avg_30d', 
            'daily_return', 'volatility_7d'
        ]
        self.enriched_df = enriched[final_columns]
        
        # Log stats
        logger.info(f"Transformation complete. Shape: {self.enriched_df.shape}")
        logger.info(f"Columns: {self.enriched_df.columns.tolist()}")
        
        # Save transformed backup
        transformed_path = f"data/transformed/transformed_{datetime.now().strftime('%Y%m%d_%H%M')}.parquet"
        os.makedirs("data/transformed", exist_ok=True)
        self.enriched_df.to_parquet(transformed_path, index=False)
        logger.info(f"Transformed data saved to {transformed_path}")
        
        return self.enriched_df


# --- Standalone test ---
if __name__ == "__main__":
    # Simulate: load the latest raw JSON from extract
    import glob
    logging.basicConfig(level=logging.INFO)
    
    raw_files = sorted(glob.glob("data/raw/raw_*.json"))
    if not raw_files:
        logger.error("No raw files found. Run extract.py first.")
        exit(1)
    
    latest_raw = raw_files[-1]
    logger.info(f"Loading raw data from {latest_raw}")
    
    raw_df = pd.read_json(latest_raw)
    transformer = DataTransformer(raw_df)
    transformed_df = transformer.transform()
    
    print("\n--- Sample of Transformed Data ---")
    print(transformed_df.head())
    print(f"\nTotal rows: {len(transformed_df)}")