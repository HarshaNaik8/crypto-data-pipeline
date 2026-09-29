# src/extract.py
"""
Resilient Cryptocurrency Market Data Extractor.
- Primary: CoinGecko API v3 (Global aggregator with batch querying).
- Secondary: CoinPaprika API v1 (Global aggregator fallback with identical metrics, zero rate-limit blocks on cloud runners).
- Tertiary: Binance Public API (Exchange fallback for non-US runners).
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

# Symbol mappings for multi-source fallbacks
COINPAPRIKA_MAP = {
    "bitcoin": "btc-bitcoin",
    "ethereum": "eth-ethereum",
    "solana": "sol-solana",
}

BINANCE_SYMBOL_MAP = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
}

ESTIMATED_SUPPLY = {
    "BITCOIN": 19_750_000,
    "ETHEREUM": 120_200_000,
    "SOLANA": 465_000_000,
}


def _should_give_up(exc: requests.exceptions.RequestException) -> bool:
    resp = getattr(exc, "response", None)
    if resp is None:
        return False
    code = resp.status_code
    return code not in (429, 500, 502, 503, 504) and code >= 400


class CoinGeckoExtractor:
    """Enterprise multi-source crypto extractor with zero cloud downtime."""

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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["x-cg-demo-api-key"] = self.api_key
        return headers

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, requests.exceptions.Timeout),
        max_tries=2,
        giveup=_should_give_up,
        jitter=backoff.full_jitter,
    )
    def _fetch_batch_coingecko(self) -> Dict[str, Dict]:
        url = f"{self.base_url}/simple/price"
        params = {
            "ids": ",".join(s.lower() for s in self.symbols),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
        }
        resp = requests.get(url, params=params, headers=self._get_headers(), timeout=8)
        resp.raise_for_status()
        return resp.json()

    def _fetch_coinpaprika_fallback(self) -> List[Dict]:
        """Global aggregator fallback via CoinPaprika (identical metrics: VWAP price, volume, cap)."""
        logger.info("Fetching market data via CoinPaprika global aggregator fallback...")
        records = []
        now_ts = datetime.utcnow().isoformat()

        for sym in self.symbols:
            paprika_id = COINPAPRIKA_MAP.get(sym.lower())
            if not paprika_id:
                continue
            try:
                url = f"https://api.coinpaprika.com/v1/tickers/{paprika_id}"
                resp = requests.get(url, timeout=8)
                if resp.status_code == 200:
                    d = resp.json()
                    quotes = d.get("quotes", {}).get("USD", {})
                    price = float(quotes.get("price", 0.0))
                    if price > 0:
                        records.append({
                            "symbol": sym.upper(),
                            "price_usd": price,
                            "market_cap": float(quotes.get("market_cap", 0.0)),
                            "volume_24h": float(quotes.get("volume_24h", 0.0)),
                            "change_24h": float(quotes.get("percent_change_24h", 0.0)),
                            "timestamp": now_ts,
                        })
                        logger.info("Successfully fetched %s from CoinPaprika: $%s", sym.upper(), price)
            except Exception as exc:
                logger.warning("CoinPaprika failed for %s: %s", sym, exc)

        return records

    def _fetch_binance_fallback(self) -> List[Dict]:
        """Binance fallback for non-US runner environments."""
        logger.info("Attempting Binance Public API fallback...")
        records = []
        now_ts = datetime.utcnow().isoformat()

        for sym in self.symbols:
            pair = BINANCE_SYMBOL_MAP.get(sym.lower())
            if not pair:
                continue
            try:
                url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={pair}"
                resp = requests.get(url, timeout=8)
                if resp.status_code == 200:
                    d = resp.json()
                    if isinstance(d, dict) and d.get("lastPrice"):
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
                logger.warning("Binance fallback failed for %s: %s", sym, e)

        return records

    def extract_all(self) -> pd.DataFrame:
        """
        Multi-source extraction engine:
        1. CoinGecko (Primary)
        2. CoinPaprika (Global Aggregator Fallback - works everywhere in cloud)
        3. Binance (Exchange Fallback)
        """
        records = []
        coingecko_data = None

        # 1. Try CoinGecko primary
        try:
            coingecko_data = self._fetch_batch_coingecko()
        except Exception as exc:
            logger.warning("CoinGecko primary failed or throttled (%s). Switching to fallback aggregators...", exc)

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

        # 2. Try CoinPaprika if any symbols are missing
        if len(records) < len(self.symbols):
            fetched_syms = {r["symbol"] for r in records}
            paprika_records = self._fetch_coinpaprika_fallback()
            for rec in paprika_records:
                if rec["symbol"] not in fetched_syms:
                    records.append(rec)
                    fetched_syms.add(rec["symbol"])

        # 3. Try Binance if still missing
        if len(records) < len(self.symbols):
            fetched_syms = {r["symbol"] for r in records}
            binance_records = self._fetch_binance_fallback()
            for rec in binance_records:
                if rec["symbol"] not in fetched_syms:
                    records.append(rec)

        if not records:
            raise ValueError("No data extracted from CoinGecko, CoinPaprika, or Binance — all symbols failed.")

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