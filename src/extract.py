# src/extract.py
"""
Resilient Cryptocurrency Market Data Extractor.
- Primary: CoinGecko API v3 (Batch fetch with exponential backoff & rate-limit resilience).
- Fallback: Binance Public API (Instant, high-capacity fallback if CoinGecko is throttled/blocked).
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

# Standard circulating supply estimates for market cap calculation if fallback is used
ESTIMATED_SUPPLY = {
    "BITCOIN": 19_750_000,
    "ETHEREUM": 120_200_000,
    "SOLANA": 465_000_000,
}

BINANCE_SYMBOL_MAP = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
}


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
    return code not in (429, 500, 502, 503, 504) and code >= 400


class CoinGeckoExtractor:
    """Professional, fault-tolerant API extractor with retry logic and multi-source fallback."""

    def __init__(self):
        self.base_url = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
        self.api_key = os.getenv("COINGECKO_API_KEY", "").strip()
        symbols_str = os.getenv("SYMBOLS", "bitcoin,ethereum,solana")
        self.symbols: List[str] = [s.strip() for s in symbols_str.split(",") if s.strip()]
        self.raw_data_path = "data/raw"
        os.makedirs(self.raw_data_path, exist_ok=True)
        logger.info("Initialized CoinGeckoExtractor with symbols: %s", self.symbols)

    def _get_headers(self) -> Dict[str, str]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["x-cg-demo-api-key"] = self.api_key
        return headers

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, requests.exceptions.Timeout),
        max_tries=3,
        giveup=_should_give_up,
        jitter=backoff.full_jitter,
    )
    def _fetch_batch_coingecko(self) -> Dict[str, Dict]:
        """Fetch all configured symbols from CoinGecko in a single request."""
        url = f"{self.base_url}/simple/price"
        params = {
            "ids": ",".join(s.lower() for s in self.symbols),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
        }
        resp = requests.get(url, params=params, headers=self._get_headers(), timeout=12)
        resp.raise_for_status()
        return resp.json()

    def _fetch_binance_fallback(self) -> List[Dict]:
        """High-reliability fallback to Binance Public API if CoinGecko is throttled."""
        logger.warning("Attempting Binance Public API fallback for symbols: %s", self.symbols)
        records = []
        now_ts = datetime.utcnow().isoformat()

        for sym in self.symbols:
            pair = BINANCE_SYMBOL_MAP.get(sym.lower())
            if not pair:
                continue
            try:
                url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={pair}"
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    d = resp.json()
                    if isinstance(d, dict) and "lastPrice" in d and d["lastPrice"]:
                        sym_upper = sym.upper()
                        price = float(d["lastPrice"])
                        vol = float(d.get("quoteVolume", 0.0))
                        change = float(d.get("priceChangePercent", 0.0))
                        mcap = price * ESTIMATED_SUPPLY.get(sym_upper, 1_000_000)

                        records.append({
                            "symbol": sym_upper,
                            "price_usd": price,
                            "market_cap": mcap,
                            "volume_24h": vol,
                            "change_24h": change,
                            "timestamp": now_ts,
                        })
                        logger.info("Successfully fetched %s from Binance fallback: $%s", sym_upper, price)
            except Exception as e:
                logger.error("Binance fallback failed for %s: %s", sym, e)

        return records

    def extract_all(self) -> pd.DataFrame:
        """
        Fetch data for all symbols.
        Tries CoinGecko batch first; transparently falls back to Binance if rate-limited.
        """
        records = []
        coingecko_data = None

        # 1. Try CoinGecko primary
        try:
            coingecko_data = self._fetch_batch_coingecko()
        except Exception as exc:
            logger.warning("CoinGecko extraction failed or rate-limited (%s). Invoking fallback...", exc)

        if coingecko_data and isinstance(coingecko_data, dict):
            now_ts = datetime.utcnow().isoformat()
            for sym in self.symbols:
                raw = coingecko_data.get(sym.lower())
                if raw and isinstance(raw, dict) and "usd" in raw:
                    records.append({
                        "symbol": sym.upper(),
                        "price_usd": raw.get("usd"),
                        "market_cap": raw.get("usd_market_cap"),
                        "volume_24h": raw.get("usd_24h_vol"),
                        "change_24h": raw.get("usd_24h_change"),
                        "timestamp": now_ts,
                    })
                    logger.info("Successfully fetched %s from CoinGecko", sym)

        # 2. If CoinGecko didn't yield all symbols, invoke Binance fallback
        if len(records) < len(self.symbols):
            logger.info("CoinGecko provided %d/%d records — running fallback for missing items...", len(records), len(self.symbols))
            fetched_syms = {r["symbol"] for r in records}
            fallback_records = self._fetch_binance_fallback()
            for fb in fallback_records:
                if fb["symbol"] not in fetched_syms:
                    records.append(fb)

        if not records:
            raise ValueError("No data extracted from CoinGecko API or fallback providers — all symbols failed.")

        df = pd.DataFrame(records)

        # Audit trail
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        backup_file = os.path.join(self.raw_data_path, f"raw_{ts}.json")
        df.to_json(backup_file, orient="records", date_format="iso")
        logger.info("Raw data saved to %s (%d records)", backup_file, len(df))

        return df


# ── Standalone test ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from src.utils import setup_logging
    setup_logging()
    extractor = CoinGeckoExtractor()
    df = extractor.extract_all()
    print(df.to_string())