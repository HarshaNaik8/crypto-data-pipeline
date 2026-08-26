# pipeline.py

import logging
import time
import schedule
from datetime import datetime
from src.extract import CoinGeckoExtractor
from src.transform import DataTransformer
from src.load import DatabaseLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Orchestrator")

def run_pipeline():
    """Full ETL pipeline: Extract -> Transform -> Load."""
    logger.info("="*50)
    logger.info(f"Starting pipeline execution at {datetime.now()}")
    logger.info("="*50)
    
    try:
        # Step 1: Extract
        logger.info("Phase 1: EXTRACT")
        extractor = CoinGeckoExtractor()
        raw_df = extractor.extract_all()
        logger.info(f"Extracted {len(raw_df)} records")
        
        # Step 2: Transform
        logger.info("Phase 2: TRANSFORM")
        transformer = DataTransformer(raw_df)
        transformed_df = transformer.transform()
        logger.info(f"Transformed {len(transformed_df)} records with features")
        
        # Step 3: Load
        logger.info("Phase 3: LOAD")
        loader = DatabaseLoader()
        loader.run(transformed_df)
        
        logger.info("✅ Pipeline completed successfully!")
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}", exc_info=True)
        # In production, send alert (email/Slack) here

# --- Schedule Configuration ---
# Run every day at 9:00 AM
schedule.every().day.at("09:00").do(run_pipeline)

# For testing RIGHT NOW, uncomment this line to run every 30 seconds:
# schedule.every(30).seconds.do(run_pipeline)

if __name__ == "__main__":
    logger.info("🚀 Orchestrator started. Waiting for scheduled runs...")
    logger.info("Next run scheduled for 09:00 AM daily.")
    
    # Run once immediately for testing
    logger.info("Running initial pipeline immediately...")
    run_pipeline()
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute