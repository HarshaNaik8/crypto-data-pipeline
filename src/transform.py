# src/transform.py

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class DataTransformer:
    """
    Professional data transformer with feature engineering.
    Handles nulls, rolling averages, returns, and volatility.
    Loads historical data from database for accurate time-series calculations.
    """
    
    def __init__(self, raw_df: pd.DataFrame, connection_string: str = "sqlite:///crypto_pipeline.db"):
        self.raw_df = raw_df.copy()
        self.cleaned_df = None
        self.enriched_df = None
        self.connection_string = connection_string
        
    def _validate_data(self) -> bool:
        """Basic data quality checks."""
        if self.raw_df.empty:
            raise ValueError("Raw DataFrame is empty")
        
        required = ['symbol', 'price_usd', 'timestamp']
        missing = [col for col in required if col not in self.raw_df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        if (self.raw_df['price_usd'] <= 0).any():
            logger.warning("Negative or zero prices found, dropping rows")
            self.raw_df = self.raw_df[self.raw_df['price_usd'] > 0]
        
        self.raw_df['timestamp'] = pd.to_datetime(self.raw_df['timestamp'])
        return True
    
    def _load_historical_data(self) -> pd.DataFrame:
        """
        Load existing historical data from the database.
        Uses SQLAlchemy properly.
        """
        import sqlite3
        
        try:
            # Direct SQLite connection - more reliable!
            conn = sqlite3.connect('crypto_pipeline.db')
            
            # Check if table exists
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fact_market_data'")
            if not cursor.fetchone():
                logger.info("No historical data found (fact_market_data table does not exist)")
                conn.close()
                return pd.DataFrame()
            
            # Query historical data
            hist_df = pd.read_sql("""
                SELECT 
                    f.symbol_id,
                    d.symbol_code as symbol,
                    f.price_usd,
                    f.record_timestamp as timestamp
                FROM fact_market_data f
                JOIN dim_symbol d ON f.symbol_id = d.symbol_id
                ORDER BY f.record_timestamp
            """, conn)
            
            conn.close()
            
            if not hist_df.empty:
                logger.info(f"Loaded {len(hist_df)} historical records from database")
                hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp'])
                return hist_df
            else:
                logger.info("No historical records found in database")
                return pd.DataFrame()
                
        except Exception as e:
            logger.warning(f"Could not load historical data: {e}")
            return pd.DataFrame()
    
    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Forward-fill nulls for price and volume."""
        df = df.copy()
        df = df.sort_values(['symbol', 'timestamp'])
        
        for col in ['price_usd', 'market_cap', 'volume_24h']:
            if col in df.columns:
                df[col] = df.groupby('symbol')[col].ffill()
        
        df = df.dropna(subset=['price_usd', 'volume_24h'])
        logger.info(f"Null handling complete. Shape: {df.shape}")
        return df
    
    def _calculate_rolling_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer features:
        - rolling_avg_7d: 7-day moving average
        - rolling_avg_30d: 30-day moving average
        - daily_return: percentage change from previous day
        - volatility_7d: 7-day standard deviation of daily returns
        """
        df = df.copy()
        df = df.sort_values(['symbol', 'timestamp'])
        
        # 7-day rolling average
        df['rolling_avg_7d'] = df.groupby('symbol')['price_usd'].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )
        
        # 30-day rolling average
        df['rolling_avg_30d'] = df.groupby('symbol')['price_usd'].transform(
            lambda x: x.rolling(window=30, min_periods=1).mean()
        )
        
        # Daily return (percentage change from previous row)
        df['daily_return'] = df.groupby('symbol')['price_usd'].pct_change() * 100
        
        # 7-day volatility (std dev of daily returns)
        df['volatility_7d'] = df.groupby('symbol')['daily_return'].transform(
            lambda x: x.rolling(window=7, min_periods=1).std()
        )
        
        # Fill NaN with 0
        df['rolling_avg_7d'] = df['rolling_avg_7d'].fillna(df['price_usd'])
        df['rolling_avg_30d'] = df['rolling_avg_30d'].fillna(df['price_usd'])
        df['daily_return'] = df['daily_return'].fillna(0)
        df['volatility_7d'] = df['volatility_7d'].fillna(0)
        
        logger.info("Feature engineering complete.")
        return df
    
    def _add_dimension_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add symbol_id mapping."""
        df = df.copy()
        
        if 'symbol_id' in df.columns:
            return df
        
        symbol_map = {
            'BITCOIN': 1,
            'ETHEREUM': 2,
            'SOLANA': 3
        }
        df['symbol_id'] = df['symbol'].map(symbol_map)
        df['symbol_id'] = df['symbol_id'].fillna(0).astype(int)
        return df
    
    def transform(self) -> pd.DataFrame:
        """Orchestrate the entire transformation pipeline."""
        logger.info("Starting transformation pipeline...")
        
        # Step 0: Load historical data
        logger.info("Loading historical data from database...")
        hist_df = self._load_historical_data()
        
        # Combine historical + current
        if not hist_df.empty:
            current_df = self.raw_df.copy()
            combined_df = pd.concat([hist_df, current_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=['symbol', 'timestamp'], 
                keep='last'
            )
            combined_df = combined_df.sort_values(['symbol', 'timestamp'])
            logger.info(f"Combined dataset: {len(combined_df)} records ({len(hist_df)} historical + {len(current_df)} current)")
            self.raw_df = combined_df
        else:
            logger.info("No historical data available. Using only current data.")
        
        # Step 1: Validate
        self._validate_data()
        
        # Step 2: Handle nulls
        cleaned = self._handle_nulls(self.raw_df)
        self.cleaned_df = cleaned
        
        # Step 3: Feature engineering
        enriched = self._calculate_rolling_features(cleaned)
        
        # Step 4: Dimension mapping
        enriched = self._add_dimension_mapping(enriched)
        
        # Step 5: Final column selection
        final_columns = [
            'timestamp', 'symbol', 'symbol_id', 'price_usd', 
            'market_cap', 'volume_24h', 'change_24h',
            'rolling_avg_7d', 'rolling_avg_30d', 
            'daily_return', 'volatility_7d'
        ]
        
        for col in final_columns:
            if col not in enriched.columns:
                enriched[col] = 0
        
        self.enriched_df = enriched[final_columns]
        
        logger.info(f"Transformation complete. Shape: {self.enriched_df.shape}")
        
        # Save transformed backup
        transformed_path = f"data/transformed/transformed_{datetime.now().strftime('%Y%m%d_%H%M')}.parquet"
        os.makedirs("data/transformed", exist_ok=True)
        self.enriched_df.to_parquet(transformed_path, index=False)
        logger.info(f"Transformed data saved to {transformed_path}")
        
        return self.enriched_df


if __name__ == "__main__":
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
    print(transformed_df.head(10))
    print(f"\nTotal rows: {len(transformed_df)}")