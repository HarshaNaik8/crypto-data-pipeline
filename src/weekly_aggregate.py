# src/weekly_aggregate.py

import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

def create_weekly_aggregate_view(connection_string="sqlite:///crypto_pipeline.db"):
    """
    Creates a view `vw_weekly_trends` that computes weekly averages.
    This acts like a stored procedure in SQL Server.
    """
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        # Drop view if exists (SQLite)
        conn.execute(text("DROP VIEW IF EXISTS vw_weekly_trends"))
        
        # Create the view with weekly aggregations
        conn.execute(text("""
            CREATE VIEW vw_weekly_trends AS
            WITH weekly_data AS (
                SELECT 
                    symbol_id,
                    strftime('%Y-%W', record_timestamp) AS year_week,
                    AVG(price_usd) AS avg_price,
                    AVG(volume_24h) AS avg_volume,
                    AVG(volatility_7d) AS avg_volatility,
                    MAX(price_usd) - MIN(price_usd) AS price_range,
                    COUNT(*) AS record_count
                FROM fact_market_data
                GROUP BY symbol_id, year_week
            )
            SELECT 
                wd.*,
                ds.symbol_code,
                ds.asset_name
            FROM weekly_data wd
            JOIN dim_symbol ds ON wd.symbol_id = ds.symbol_id
            ORDER BY wd.year_week DESC, wd.symbol_id
        """))
        
        conn.commit()
        logger.info("✅ Weekly aggregate view 'vw_weekly_trends' created successfully.")
        
        # Show sample output
        result = conn.execute(text("SELECT * FROM vw_weekly_trends LIMIT 10"))
        rows = result.fetchall()
        logger.info(f"Sample data (first {len(rows)} rows):")
        for row in rows:
            logger.info(row)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_weekly_aggregate_view()