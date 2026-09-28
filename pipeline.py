# pipeline.py
"""
Core ETL orchestrator — pure function, no scheduler loop here.
Importable from run_etl.py (Task Scheduler) or any other entry point.
Includes end-to-end execution timing and Discord/Webhook alerting.
"""

import time
import logging
from datetime import datetime

from src.utils import setup_logging
from src.extract import CoinGeckoExtractor
from src.transform import DataTransformer
from src.load import DatabaseLoader
from src.weekly_aggregate import create_weekly_aggregate_view
from src.alerts import notify_success, notify_failure

logger = logging.getLogger("pipeline")


def run_pipeline(connection_string: str | None = None) -> bool:
    """
    Full ETL: Extract → Transform → Load → Refresh weekly view → Observability Alerts.
    Automatically connects to Neon Cloud Postgres if configured, otherwise falls back to SQLite.
    Returns True on success, False on failure.
    """
    conn_str = connection_string or os.getenv("NEON_DB_URL") or os.getenv("DB_CONNECTION_STRING", "sqlite:///crypto_pipeline.db")
    start_time = time.time()
    current_phase = "INITIALIZATION"
    raw_df = None
    transformed_df = None

    logger.info("=" * 60)
    logger.info("Pipeline started at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    try:
        # ── Extract ──────────────────────────────────────────────────────────
        current_phase = "EXTRACT"
        logger.info("Phase 1: EXTRACT")
        extractor = CoinGeckoExtractor()
        raw_df = extractor.extract_all()
        logger.info("Extracted %d records.", len(raw_df))

        # ── Transform ─────────────────────────────────────────────────────────
        current_phase = "TRANSFORM"
        logger.info("Phase 2: TRANSFORM")
        transformer = DataTransformer(raw_df, connection_string=conn_str)
        transformed_df = transformer.transform()
        logger.info("Transformed %d new rows with financial features.", len(transformed_df))

        # ── Load ──────────────────────────────────────────────────────────────
        current_phase = "LOAD"
        logger.info("Phase 3: LOAD")
        loader = DatabaseLoader(connection_string=conn_str)
        loader.run(transformed_df)

        # ── Refresh weekly view ───────────────────────────────────────────────
        current_phase = "REFRESH WEEKLY VIEW"
        logger.info("Phase 4: REFRESH WEEKLY VIEW")
        create_weekly_aggregate_view(conn_str)

        duration = time.time() - start_time
        logger.info("Pipeline completed successfully in %.2fs at %s", duration, datetime.now().strftime("%H:%M:%S"))
        logger.info("=" * 60)

        # ── Data Observability Alert (Success Heartbeat) ───────────────────────
        asset_summaries = []
        if transformed_df is not None and not transformed_df.empty:
            for _, row in transformed_df.iterrows():
                asset_summaries.append({
                    "symbol": str(row.get("symbol", "")).upper(),
                    "price": float(row.get("price_usd", 0.0)),
                    "change_24h": float(row.get("change_24h", 0.0)) if row.get("change_24h") is not None else 0.0,
                })

        notify_success(
            duration_seconds=duration,
            extracted_count=len(raw_df) if raw_df is not None else 0,
            loaded_count=len(transformed_df) if transformed_df is not None else 0,
            asset_summaries=asset_summaries,
        )

        return True

    except Exception as exc:
        duration = time.time() - start_time
        logger.error("Pipeline FAILED during %s after %.2fs: %s", current_phase, duration, exc, exc_info=True)
        logger.info("=" * 60)

        # ── Data Observability Alert (Failure Incident) ────────────────────────
        notify_failure(
            phase=current_phase,
            error_message=str(exc),
            duration_seconds=duration,
        )

        return False


# ── Direct execution ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    setup_logging()
    success = run_pipeline()
    raise SystemExit(0 if success else 1)