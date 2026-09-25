# run_etl.py
"""
Single-run entry point for Windows Task Scheduler.
Exits with code 0 (success) or 1 (failure) so Task Scheduler can detect failures.
"""
import sys
from src.utils import setup_logging
from pipeline import run_pipeline

if __name__ == "__main__":
    setup_logging()
    success = run_pipeline()
    sys.exit(0 if success else 1)