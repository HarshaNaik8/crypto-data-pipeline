# debug_db.py
import sqlite3

conn = sqlite3.connect('crypto_pipeline.db')

print("=" * 80)
print("SCHEMA (column types)")
print("=" * 80)
schema = conn.execute("SELECT sql FROM sqlite_master WHERE name='fact_market_data'").fetchone()[0]
print(schema)

print("\n" + "=" * 80)
print("ACTUAL STORED VALUES (with types)")
print("=" * 80)
rows = conn.execute("""
    SELECT fact_id, symbol_id, price_usd, daily_return, 
           typeof(daily_return) as ret_type,
           volatility_7d, typeof(volatility_7d) as vol_type,
           record_timestamp
    FROM fact_market_data
    ORDER BY fact_id
""").fetchall()

print(f"{'id':<4} {'sym':<4} {'price':<12} {'daily_ret':<15} {'type':<8} {'volatility':<15} {'type':<8} {'date'}")
print("-" * 90)
for r in rows:
    print(f"{r[0]:<4} {r[1]:<4} {r[2]:<12} {r[3]:<15} {r[4]:<8} {r[5]:<15} {r[6]:<8} {r[7]}")

conn.close()