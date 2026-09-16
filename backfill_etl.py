# backfill_etl.py
"""
Backfill historical data for specific dates.
Usage: python backfill_etl.py --date 2026-09-11
       python backfill_etl.py --start 2026-09-11 --end 2026-09-15
"""

import os
import sys
import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
import requests
import time
from src.transform import DataTransformer
from src.load import DatabaseLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("Backfill")

class BackfillETL:
    def __init__(self, target_date):
        self.target_date = target_date
        self.loader = DatabaseLoader()
        self.symbols = ['bitcoin', 'ethereum', 'solana']
    
    def _fetch_historical_price(self, symbol, date):
        url = "https://api.coingecko.com/api/v3/coins/{}/history"
        params = {"date": date.strftime("%d-%m-%Y"), "localization": "false"}
        
        for attempt in range(3):
            try:
                response = requests.get(url.format(symbol), params=params, timeout=30)
                if response.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                data = response.json()
                market_data = data.get("market_data", {})
                return {
                    "symbol": symbol.upper(),
                    "price_usd": market_data.get("current_price", {}).get("usd", 0),
                    "market_cap": market_data.get("market_cap", {}).get("usd", 0),
                    "volume_24h": market_data.get("total_volume", {}).get("usd", 0),
                    "change_24h": 0,
                    "timestamp": datetime.combine(date, datetime.min.time()).isoformat()
                }
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed to fetch {symbol} for {date}: {e}")
                    return None
                time.sleep(5)
        return None
    
    def run(self):
        logger.info(f"🔄 Backfilling {self.target_date.strftime('%Y-%m-%d')}")
        records = []
        for symbol in self.symbols:
            result = self._fetch_historical_price(symbol, self.target_date)
            if result and result['price_usd'] > 0:
                records.append(result)
                logger.info(f"✅ {symbol}: ${result['price_usd']:,.2f}")
            else:
                logger.warning(f"⚠️ Failed: {symbol}")
        
        if not records:
            logger.error("❌ No data fetched")
            return False
        
        raw_df = pd.DataFrame(records)
        transformer = DataTransformer(raw_df)
        transformed_df = transformer.transform()
        self.loader.run(transformed_df)
        logger.info(f"✅ Backfill complete for {self.target_date.strftime('%Y-%m-%d')}")
        return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Single date (YYYY-MM-DD)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="Last N days")
    args = parser.parse_args()
    
    dates = []
    if args.date:
        dates = [datetime.strptime(args.date, "%Y-%m-%d")]
    elif args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d")
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    elif args.days:
        end = datetime.now()
        dates = [end - timedelta(days=i) for i in range(args.days)]
        dates.reverse()
    else:
        dates = [datetime.now()]
    
    today = datetime.now().strftime('%Y-%m-%d')
    for date in dates:
        if date.strftime('%Y-%m-%d') == today:
            logger.info(f"⚠️ Skipping today ({today}) - use run_etl.py")
            continue
        backfill = BackfillETL(date)
        backfill.run()
        time.sleep(10)  # rate limit safety

if __name__ == "__main__":
    main()