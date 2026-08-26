import os
import json
import logging
import sys
import requests
import backoff
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Optional

# Load .env file (override existing env vars)
load_dotenv(override=True)

# Fix logging encoding for Windows
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)
logger = logging.getLogger(__name__)

class CoinGeckoExtractor:
    """Professional API extractor with retry logic and data validation."""
    
    def __init__(self):
        self.base_url = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
        # Default symbols if not in .env
        symbols_str = os.getenv("SYMBOLS", "bitcoin,ethereum,solana")
        self.symbols = [s.strip() for s in symbols_str.split(",") if s.strip()]
        self.raw_data_path = "data/raw/"
        os.makedirs(self.raw_data_path, exist_ok=True)
        logger.info(f"Initialized with symbols: {self.symbols}")
        
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, requests.exceptions.Timeout),
        max_tries=5,
        giveup=lambda e: e.response is not None and e.response.status_code < 500
    )
    def _fetch_price(self, symbol: str) -> Optional[Dict]:
        """Fetch current price data for a single symbol."""
        url = f"{self.base_url}/simple/price"
        params = {
            "ids": symbol.lower(),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true"
        }
        
        logger.info(f"Fetching data for {symbol}...")
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if symbol.lower() in data:
            raw = data[symbol.lower()]
            return {
                "symbol": symbol.upper(),
                "price_usd": raw.get("usd"),
                "market_cap": raw.get("usd_market_cap"),
                "volume_24h": raw.get("usd_24h_vol"),
                "change_24h": raw.get("usd_24h_change"),
                "timestamp": datetime.utcnow().isoformat()
            }
        return None
    
    def extract_all(self) -> pd.DataFrame:
        """Fetch data for all symbols and return a clean DataFrame."""
        records = []
        for sym in self.symbols:
            try:
                result = self._fetch_price(sym)
                if result:
                    records.append(result)
                    logger.info(f"Successfully fetched {sym}")
                else:
                    logger.warning(f"No data returned for {sym}")
            except Exception as e:
                logger.error(f"Failed to fetch {sym}: {str(e)}")
                continue
        
        if not records:
            raise ValueError("No data extracted from API")
        
        df = pd.DataFrame(records)
        
        # Save raw backup
        backup_file = f"{self.raw_data_path}/raw_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        df.to_json(backup_file, orient="records", date_format="iso")
        logger.info(f"Raw data saved to {backup_file}")
        
        return df

if __name__ == "__main__":
    logger.info("Starting Extract Test...")
    extractor = CoinGeckoExtractor()
    df = extractor.extract_all()
    print(df.head())
    logger.info("Extract test completed.")