# pipeline.py
"""
Core ETL orchestrator — pure function, no scheduler loop here.
Importable from run_etl.py (Task Scheduler) or any other entry point.
"""

import logging
from datetime import datetime

from src.utils import setup_logging
from src.extract import CoinGeckoExtractor
from src.transform import DataTransformer
from src.load import DatabaseLoader
from src.weekly_aggregate import create_weekly_aggregate_view

logger = logging.getLogger("pipeline")


def run_pipeline(connection_string: str = "sqlite:///crypto_pipeline.db") -> bool:
    """
    Full ETL: Extract → Transform → Load → Refresh weekly view.
    Returns True on success, False on failure.
    """
    logger.info("=" * 60)
    logger.info("Pipeline started at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    try:
        # ── Extract ──────────────────────────────────────────────────────────
        logger.info("Phase 1: EXTRACT")
        extractor = CoinGeckoExtractor()
        raw_df = extractor.extract_all()
        logger.info("Extracted %d records.", len(raw_df))

        # ── Transform ─────────────────────────────────────────────────────────
        logger.info("Phase 2: TRANSFORM")
        transformer = DataTransformer(raw_df, connection_string=connection_string)
        transformed_df = transformer.transform()
        logger.info("Transformed %d new rows with financial features.", len(transformed_df))

        # ── Load ──────────────────────────────────────────────────────────────
        logger.info("Phase 3: LOAD")
        loader = DatabaseLoader(connection_string=connection_string)
        loader.run(transformed_df)

        # ── Refresh weekly view ───────────────────────────────────────────────
        logger.info("Phase 4: REFRESH WEEKLY VIEW")
        create_weekly_aggregate_view(connection_string)

        logger.info("Pipeline completed successfully at %s", datetime.now().strftime("%H:%M:%S"))
        logger.info("=" * 60)
        return True

    except Exception as exc:
        logger.error("Pipeline FAILED: %s", exc, exc_info=True)
        logger.info("=" * 60)
        return False


# ── Direct execution ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    setup_logging()
    success = run_pipeline()
    raise SystemExit(0 if success else 1)