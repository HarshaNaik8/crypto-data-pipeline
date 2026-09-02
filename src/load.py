# src/load.py

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Handles loading transformed data into SQL database with proper UPSERT logic."""
    
    def __init__(self, connection_string=None):
        self.connection_string = connection_string or os.getenv("DB_CONNECTION_STRING", "sqlite:///crypto_pipeline.db")
        self.engine = create_engine(self.connection_string)
        logger.info(f"Connected to database: {self.connection_string}")
    
    def create_tables(self):
        """Create star-schema tables if they don't exist."""
        with self.engine.connect() as conn:
            # 1. Dimension table: dim_symbol
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_symbol (
                    symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol_code VARCHAR(10) UNIQUE NOT NULL,
                    asset_name VARCHAR(50),
                    asset_type VARCHAR(20)
                )
            """))
            
            # 2. Fact table: fact_market_data
            # NOTE: Using daily granularity - we'll handle UPSERT via DATE() function
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_market_data (
                    fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol_id INTEGER NOT NULL,
                    price_usd DECIMAL(18,6),
                    market_cap DECIMAL(30,2),
                    volume_24h DECIMAL(30,2),
                    change_24h DECIMAL(10,6),
                    rolling_avg_7d DECIMAL(18,6),
                    rolling_avg_30d DECIMAL(18,6),
                    daily_return DECIMAL(10,6),
                    volatility_7d DECIMAL(10,6),
                    record_timestamp DATETIME,
                    FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id)
                )
            """))
            
            # 3. Indexes for fast queries
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_timestamp ON fact_market_data(record_timestamp)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_symbol_id ON fact_market_data(symbol_id)"))
            
            conn.commit()
            logger.info("Tables and indexes created/verified successfully.")
    
    def upsert_symbols(self, df: pd.DataFrame):
        """Insert new symbols into dim_symbol, return mapping of symbol_code -> symbol_id."""
        symbols = df[['symbol']].drop_duplicates()
        symbol_map = {}
        
        with self.engine.connect() as conn:
            for _, row in symbols.iterrows():
                code = row['symbol'].upper()
                # Insert if not exists
                conn.execute(text("""
                    INSERT OR IGNORE INTO dim_symbol (symbol_code, asset_name, asset_type)
                    VALUES (:code, :name, 'crypto')
                """), {"code": code, "name": code.capitalize()})
                conn.commit()
                
                # Fetch the ID
                result = conn.execute(text("SELECT symbol_id FROM dim_symbol WHERE symbol_code = :code"), {"code": code})
                symbol_map[code] = result.fetchone()[0]
        
        return symbol_map
    
    def load_fact(self, df: pd.DataFrame):
        """
        Insert or UPDATE records using daily granularity.
        For each symbol, only keep ONE record per day (latest price).
        """
        df = df.copy()
        
        # Upsert symbols
        symbol_map = self.upsert_symbols(df)
        df['symbol_id'] = df['symbol'].map(symbol_map)
        
        # Rename timestamp
        df = df.rename(columns={'timestamp': 'record_timestamp'})
        df['record_timestamp'] = pd.to_datetime(df['record_timestamp'])
        
        # 🔥 CRITICAL: Convert to DAILY granularity (YYYY-MM-DD)
        df['record_date'] = df['record_timestamp'].dt.date
        
        # Select columns
        fact_cols = ['symbol_id', 'price_usd', 'market_cap', 'volume_24h', 'change_24h',
                    'rolling_avg_7d', 'rolling_avg_30d', 'daily_return', 'volatility_7d', 'record_date']
        
        df_to_load = df[fact_cols].copy()
        df_to_load = df_to_load.rename(columns={'record_date': 'record_timestamp'})
        
        with self.engine.connect() as conn:
            for _, row in df_to_load.iterrows():
                # Check if this symbol already has a record for this date
                existing = conn.execute(text("""
                    SELECT 1 FROM fact_market_data 
                    WHERE symbol_id = :sid AND DATE(record_timestamp) = DATE(:ts)
                """), {"sid": row['symbol_id'], "ts": row['record_timestamp']}).fetchone()
                
                if existing:
                    # UPDATE existing record
                    conn.execute(text("""
                        UPDATE fact_market_data SET
                            price_usd = :price,
                            market_cap = :mcap,
                            volume_24h = :vol,
                            change_24h = :chg,
                            rolling_avg_7d = :ra7,
                            rolling_avg_30d = :ra30,
                            daily_return = :ret,
                            volatility_7d = :vol7
                        WHERE symbol_id = :sid AND DATE(record_timestamp) = DATE(:ts)
                    """), {
                        "price": row['price_usd'],
                        "mcap": row['market_cap'],
                        "vol": row['volume_24h'],
                        "chg": row['change_24h'],
                        "ra7": row['rolling_avg_7d'],
                        "ra30": row['rolling_avg_30d'],
                        "ret": row['daily_return'],
                        "vol7": row['volatility_7d'],
                        "sid": row['symbol_id'],
                        "ts": row['record_timestamp']
                    })
                    # ✅ FIXED: row['record_timestamp'] is already a date object, so no .date() needed
                    logger.debug(f"UPDATED: symbol_id={row['symbol_id']}, date={row['record_timestamp']}")
                else:
                    # INSERT new record
                    conn.execute(text("""
                        INSERT INTO fact_market_data (
                            symbol_id, price_usd, market_cap, volume_24h, change_24h,
                            rolling_avg_7d, rolling_avg_30d, daily_return, volatility_7d, record_timestamp
                        ) VALUES (
                            :sid, :price, :mcap, :vol, :chg,
                            :ra7, :ra30, :ret, :vol7, :ts
                        )
                    """), {
                        "sid": row['symbol_id'],
                        "price": row['price_usd'],
                        "mcap": row['market_cap'],
                        "vol": row['volume_24h'],
                        "chg": row['change_24h'],
                        "ra7": row['rolling_avg_7d'],
                        "ra30": row['rolling_avg_30d'],
                        "ret": row['daily_return'],
                        "vol7": row['volatility_7d'],
                        "ts": row['record_timestamp']
                    })
                    # ✅ FIXED: row['record_timestamp'] is already a date object, so no .date() needed
                    logger.debug(f"INSERTED: symbol_id={row['symbol_id']}, date={row['record_timestamp']}")
            
            conn.commit()
            logger.info(f"Loaded {len(df_to_load)} records into fact_market_data (daily UPSERT)")
    
    def clear_all_data(self):
        """⚠️ DANGER: Delete ALL data from fact_market_data and dim_symbol."""
        with self.engine.connect() as conn:
            # Disable foreign key constraints temporarily
            conn.execute(text("PRAGMA foreign_keys = OFF"))
            conn.execute(text("DELETE FROM fact_market_data"))
            conn.execute(text("DELETE FROM dim_symbol"))
            conn.execute(text("PRAGMA foreign_keys = ON"))
            conn.commit()
            logger.warning("⚠️ ALL DATA DELETED from fact_market_data and dim_symbol!")
    
    def run(self, transformed_df: pd.DataFrame):
        """Orchestrate the entire load process."""
        logger.info("Starting database load...")
        self.create_tables()
        self.load_fact(transformed_df)
        logger.info("Database load complete!")


# --- Standalone test ---
if __name__ == "__main__":
    import glob
    import pandas as pd
    logging.basicConfig(level=logging.INFO)
    
    # Load latest transformed data
    transformed_files = sorted(glob.glob("data/transformed/transformed_*.parquet"))
    if not transformed_files:
        logger.error("No transformed files found. Run transform.py first.")
        exit(1)
    
    latest = transformed_files[-1]
    df = pd.read_parquet(latest)
    
    loader = DatabaseLoader()
    
    # OPTION: To clear existing data, uncomment the line below:
    # loader.clear_all_data()
    
    loader.run(df)
    logger.info("Load test completed.")