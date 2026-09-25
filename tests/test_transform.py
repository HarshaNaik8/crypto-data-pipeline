# tests/test_transform.py
"""
Unit tests for DataTransformer.
Uses in-memory SQLite so no live DB is required.
"""

import math
import sqlite3
import tempfile
import os
import gc
import pytest
import pandas as pd
from datetime import datetime, timedelta

# Allow importing from project root
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.transform import DataTransformer


def _make_raw_df(n_days: int = 5, symbols=("BITCOIN", "ETHEREUM")) -> pd.DataFrame:
    """Create a synthetic multi-symbol multi-day raw DataFrame."""
    records = []
    base = datetime(2026, 9, 1)
    prices = {"BITCOIN": 70000.0, "ETHEREUM": 2000.0, "SOLANA": 100.0}

    for day in range(n_days):
        ts = (base + timedelta(days=day)).isoformat()
        for sym in symbols:
            sym_upper = sym.upper()
            price = prices.get(sym_upper, 1000.0) * (1 + 0.01 * (day + 1))
            records.append({
                "symbol": sym_upper,
                "price_usd": price,
                "market_cap": price * 19_000_000,
                "volume_24h": price * 100_000,
                "change_24h": 1.0 * (day + 1),
                "timestamp": ts,
            })
    return pd.DataFrame(records)


def _safe_remove(path: str) -> None:
    """Remove file, ignoring Windows file-lock errors."""
    gc.collect()  # force-close any SQLite connections held by GC-pending objects
    try:
        if os.path.exists(path):
            os.remove(path)
    except PermissionError:
        pass  # Windows may hold the file momentarily — not a test failure


class TestDataTransformer:

    def test_basic_transform_returns_dataframe(self):
        df = _make_raw_df(5)
        t = DataTransformer(df, connection_string="sqlite:///test_basic.db")
        result = t.transform()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_output_has_required_columns(self):
        df = _make_raw_df(5)
        t = DataTransformer(df, connection_string="sqlite:///test_cols.db")
        result = t.transform()
        for col in ["price_usd", "daily_return", "volatility_7d", "rolling_avg_7d", "rolling_avg_30d"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_daily_return_is_nonzero_after_first_row(self):
        df = _make_raw_df(5, symbols=("BITCOIN",))
        t = DataTransformer(df, connection_string="sqlite:///test_return.db")
        result = t.transform()
        non_zero = (result["daily_return"] != 0).sum()
        assert non_zero >= 1, "Expected at least one non-zero daily_return"

    def test_no_negative_volatility(self):
        df = _make_raw_df(10)
        t = DataTransformer(df, connection_string="sqlite:///test_vol.db")
        result = t.transform()
        assert (result["volatility_7d"] >= 0).all(), "volatility_7d should never be negative"

    def test_empty_dataframe_raises(self):
        df = pd.DataFrame()
        t = DataTransformer(df, connection_string="sqlite:///test_empty.db")
        with pytest.raises((ValueError, Exception)):
            t.transform()

    def test_symbol_upper_cased(self):
        df = _make_raw_df(3, symbols=("bitcoin",))  # lowercase input
        t = DataTransformer(df, connection_string="sqlite:///test_upper.db")
        result = t.transform()
        assert all(s == s.upper() for s in result["symbol"].unique() if isinstance(s, str))

    def test_no_nan_in_critical_columns(self):
        df = _make_raw_df(7)
        t = DataTransformer(df, connection_string="sqlite:///test_nan.db")
        result = t.transform()
        for col in ["price_usd", "daily_return", "rolling_avg_7d", "rolling_avg_30d", "volatility_7d"]:
            nan_count = result[col].isna().sum()
            assert nan_count == 0, f"{col} has {nan_count} NaN values"

    def teardown_method(self, method):
        """Clean up test DB files — tolerates Windows file locks."""
        for name in ["test_basic", "test_cols", "test_return", "test_vol", "test_empty", "test_upper", "test_nan"]:
            _safe_remove(f"{name}.db")


class TestLoadTransformIntegration:
    """Integration test: transform + load → DB → verify data."""

    def test_load_inserts_rows(self):
        from src.load import DatabaseLoader

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn_str = f"sqlite:///{db_path}"
            df = _make_raw_df(3)
            t = DataTransformer(df, connection_string=conn_str)
            result = t.transform()

            loader = DatabaseLoader(connection_string=conn_str)
            loader.run(result)
            loader.engine.dispose()  # release SQLAlchemy pool before unlink

            conn = sqlite3.connect(db_path)
            count = conn.execute("SELECT COUNT(*) FROM fact_market_data").fetchone()[0]
            conn.close()

            # 3 days x 2 symbols = 6 rows
            assert count == 6, f"Expected 6 rows, got {count}"
        finally:
            _safe_remove(db_path)

    def test_upsert_no_duplicates(self):
        from src.load import DatabaseLoader

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn_str = f"sqlite:///{db_path}"
            df = _make_raw_df(3)

            # First run
            t1 = DataTransformer(df, connection_string=conn_str)
            loader = DatabaseLoader(connection_string=conn_str)
            loader.run(t1.transform())

            # Second run — should not duplicate
            t2 = DataTransformer(df, connection_string=conn_str)
            loader.run(t2.transform())
            loader.engine.dispose()

            conn = sqlite3.connect(db_path)
            count = conn.execute("SELECT COUNT(*) FROM fact_market_data").fetchone()[0]
            conn.close()

            assert count == 6, f"Duplicate rows after 2 runs! Got {count} rows instead of 6"
        finally:
            _safe_remove(db_path)

    def test_fact_has_unique_constraint(self):
        from src.load import DatabaseLoader

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            conn_str = f"sqlite:///{db_path}"
            loader = DatabaseLoader(connection_string=conn_str)
            loader.create_tables()
            loader.engine.dispose()

            conn = sqlite3.connect(db_path)
            cur = conn.execute("PRAGMA index_list('fact_market_data')")
            index_names = [row[1] for row in cur.fetchall()]
            conn.close()

            assert "uix_symbol_date" in index_names, "Missing UNIQUE index uix_symbol_date"
        finally:
            _safe_remove(db_path)
