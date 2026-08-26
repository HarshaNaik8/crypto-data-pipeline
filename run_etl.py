# run_etl.py
"""Single-run ETL script for Task Scheduler."""
import sys
import logging
from pipeline import run_pipeline

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("TaskScheduler")
    logger.info("Scheduled ETL started by Task Scheduler")
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        sys.exit(1)
    logger.info("Scheduled ETL completed successfully")