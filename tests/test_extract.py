# tests/test_extract.py
"""
Unit tests for CoinGeckoExtractor.
These tests mock the HTTP layer — no live API calls.
"""

import os
import sys
import json
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.extract import CoinGeckoExtractor, _should_give_up
import requests


class TestShouldGiveUp:
    """Test the backoff giveup logic."""

    def _make_exc(self, status_code):
        exc = requests.exceptions.HTTPError()
        exc.response = MagicMock()
        exc.response.status_code = status_code
        return exc

    def test_gives_up_on_400(self):
        assert _should_give_up(self._make_exc(400)) is True

    def test_gives_up_on_403(self):
        assert _should_give_up(self._make_exc(403)) is True

    def test_retries_on_429(self):
        assert _should_give_up(self._make_exc(429)) is False

    def test_retries_on_500(self):
        assert _should_give_up(self._make_exc(500)) is False

    def test_retries_on_503(self):
        assert _should_give_up(self._make_exc(503)) is False

    def test_retries_on_network_error(self):
        exc = requests.exceptions.ConnectionError("no connection")
        assert _should_give_up(exc) is False


class TestCoinGeckoExtractor:

    MOCK_RESPONSE = {
        "bitcoin": {
            "usd": 85000.0,
            "usd_market_cap": 1_700_000_000_000.0,
            "usd_24h_vol": 30_000_000_000.0,
            "usd_24h_change": 1.5,
        },
        "ethereum": {
            "usd": 2700.0,
            "usd_market_cap": 325_000_000_000.0,
            "usd_24h_vol": 15_000_000_000.0,
            "usd_24h_change": 2.1,
        },
        "solana": {
            "usd": 115.0,
            "usd_market_cap": 56_000_000_000.0,
            "usd_24h_vol": 5_000_000_000.0,
            "usd_24h_change": -0.5,
        },
    }

    @patch("src.extract.requests.get")
    def test_extract_all_returns_dataframe(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self.MOCK_RESPONSE
        mock_get.return_value = mock_resp

        extractor = CoinGeckoExtractor()
        df = extractor.extract_all()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3  # bitcoin, ethereum, solana

    @patch("src.extract.requests.get")
    def test_extract_all_has_required_columns(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self.MOCK_RESPONSE
        mock_get.return_value = mock_resp

        extractor = CoinGeckoExtractor()
        df = extractor.extract_all()

        for col in ["symbol", "price_usd", "market_cap", "volume_24h", "change_24h", "timestamp"]:
            assert col in df.columns, f"Missing column: {col}"

    @patch("src.extract.requests.get")
    def test_symbol_is_uppercased(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self.MOCK_RESPONSE
        mock_get.return_value = mock_resp

        extractor = CoinGeckoExtractor()
        df = extractor.extract_all()

        assert all(s == s.upper() for s in df["symbol"]), "Symbols should be uppercase"

    @patch("src.extract.requests.get")
    def test_all_symbols_fail_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {}  # No data for any symbol
        mock_get.return_value = mock_resp

        extractor = CoinGeckoExtractor()
        with pytest.raises(ValueError, match="No data extracted"):
            extractor.extract_all()
