# src/weekly_aggregate.py

import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def create_weekly_aggregate_view(connection_string=None):
    """
    Creates a view `vw_weekly_trends` that computes weekly averages.
    Dialect-aware: supports both SQLite strftime() and PostgreSQL to_char().
    """
    conn_str = connection_string or os.getenv("NEON_DB_URL") or os.getenv("DB_CONNECTION_STRING", "sqlite:///crypto_pipeline.db")
    engine = create_engine(conn_str)
    is_postgres = "postgresql" in conn_str

    week_expr = "to_char(record_timestamp::date, 'YYYY-WW')" if is_postgres else "strftime('%Y-%W', record_timestamp)"

    view_sql = f"""
        CREATE VIEW vw_weekly_trends AS
        WITH weekly_data AS (
            SELECT 
                symbol_id,
                {week_expr} AS year_week,
                AVG(price_usd) AS avg_price,
                AVG(volume_24h) AS avg_volume,
                AVG(volatility_7d) AS avg_volatility,
                MAX(price_usd) - MIN(price_usd) AS price_range,
                COUNT(*) AS record_count
            FROM fact_market_data
            GROUP BY symbol_id, {week_expr}
        )
        SELECT 
            wd.*,
            ds.symbol_code,
            ds.asset_name
        FROM weekly_data wd
        JOIN dim_symbol ds ON wd.symbol_id = ds.symbol_id
        ORDER BY wd.year_week DESC, wd.symbol_id
    """

    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS vw_weekly_trends"))
        conn.execute(text(view_sql))
        logger.info("Weekly aggregate view 'vw_weekly_trends' created successfully (Postgres=%s).", is_postgres)

        result = conn.execute(text("SELECT * FROM vw_weekly_trends LIMIT 10"))
        rows = result.fetchall()
        logger.info("Sample weekly aggregate data (first %d rows):", len(rows))
        for row in rows:
            logger.info(row)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_weekly_aggregate_view()