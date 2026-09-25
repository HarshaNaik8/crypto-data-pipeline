# src/extract.py
"""
CoinGecko API extractor.
- Fetches live prices for all configured symbols.
- Exponential backoff with correct 429 handling (retries, not bail-out).
- Saves raw JSON audit trail to data/raw/.
"""

import os
import logging
import requests
import backoff
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Optional

load_dotenv(override=True)
logger = logging.getLogger(__name__)


def _should_give_up(exc: requests.exceptions.RequestException) -> bool:
    """
    Give up on genuine client errors (4xx) EXCEPT:
      - 429 Too Many Requests  → must retry (rate limit)
      - 500/502/503/504         → server errors, also retry
    """
    resp = getattr(exc, "response", None)
    if resp is None:
        return False  # network error — keep retrying
    code = resp.status_code
    # Retry on rate-limit and server errors; give up on all other 4xx
    return code not in (429, 500, 502, 503, 504) and code >= 400


class CoinGeckoExtractor:
    """Professional API extractor with retry logic and data validation."""

    def __init__(self):
        self.base_url = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
        symbols_str = os.getenv("SYMBOLS", "bitcoin,ethereum,solana")
        self.symbols: List[str] = [s.strip() for s in symbols_str.split(",") if s.strip()]
        self.raw_data_path = "data/raw"
        os.makedirs(self.raw_data_path, exist_ok=True)
        logger.info("Initialized CoinGeckoExtractor with symbols: %s", self.symbols)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, requests.exceptions.Timeout),
        max_tries=5,
        giveup=_should_give_up,
        jitter=backoff.full_jitter,
    )
    def _fetch_price(self, symbol: str) -> Optional[Dict]:
        """Fetch current price data for a single symbol from /simple/price."""
        url = f"{self.base_url}/simple/price"
        params = {
            "ids": symbol.lower(),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
        }

        logger.info("Fetching live data for %s ...", symbol)
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()

        data = response.json()
        key = symbol.lower()
        if key not in data:
            logger.warning("Symbol %s not found in API response", symbol)
            return None

        raw = data[key]
        return {
            "symbol": symbol.upper(),
            "price_usd": raw.get("usd"),
            "market_cap": raw.get("usd_market_cap"),
            "volume_24h": raw.get("usd_24h_vol"),
            "change_24h": raw.get("usd_24h_change"),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def extract_all(self) -> pd.DataFrame:
        """Fetch data for all configured symbols; raise if none succeed."""
        records = []
        for sym in self.symbols:
            try:
                result = self._fetch_price(sym)
                if result:
                    records.append(result)
                    logger.info("Successfully fetched %s", sym)
                else:
                    logger.warning("No data returned for %s", sym)
            except Exception as exc:
                logger.error("Failed to fetch %s: %s", sym, exc)

        if not records:
            raise ValueError("No data extracted from CoinGecko API — all symbols failed.")

        df = pd.DataFrame(records)

        # Audit trail
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        backup_file = os.path.join(self.raw_data_path, f"raw_{ts}.json")
        df.to_json(backup_file, orient="records", date_format="iso")
        logger.info("Raw data saved to %s", backup_file)

        return df


# ── Standalone test ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from src.utils import setup_logging
    setup_logging()
    extractor = CoinGeckoExtractor()
    df = extractor.extract_all()
    print(df.to_string())