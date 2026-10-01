# 🚀 Enterprise Real-Time Crypto Data Pipeline & Observability Sentinel

[![CI / Testing Pipeline](https://github.com/HarshaNaik8/crypto-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/HarshaNaik8/crypto-data-pipeline/actions)
[![Daily Cloud ETL](https://github.com/HarshaNaik8/crypto-data-pipeline/actions/workflows/daily_etl.yml/badge.svg)](https://github.com/HarshaNaik8/crypto-data-pipeline/actions/workflows/daily_etl.yml)
[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.14-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Cloud Database](https://img.shields.io/badge/Neon_PostgreSQL-Serverless_AWS-00E599?style=flat&logo=postgresql&logoColor=white)](https://neon.tech/)
[![Testing](https://img.shields.io/badge/Tests-25%2F25%20Passing-brightgreen?style=flat&logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![24/7 Bot Sentinel](https://img.shields.io/badge/Discord_Bot-24%2F7_Cloud_Container-5865F2?style=flat&logo=discord&logoColor=white)](https://discord.com/)
[![Power BI](https://img.shields.io/badge/Power_BI-Direct_Warehouse_BI-F2C811?style=flat&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An enterprise-grade, end-to-end cloud data engineering platform that automatically extracts, validates, feature-engineers, stores, monitors, and visualizes live cryptocurrency market data (Bitcoin, Ethereum, Solana).

Engineered for **100% autonomous cloud operation** ($0/month budget, zero credit cards needed), featuring a **3-tier resilient API extraction engine**, **in-place PostgreSQL/Neon UPSERTs**, **automated daily GitHub Actions cloud execution**, an **always-on 24/7 Discord Sentinel Bot** with 11 slash commands, and an **interactive Power BI Business Intelligence suite**.

---

## 🏗️ 1. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph Ingestion ["1. Multi-Tier Resilient Ingestion (Extract)"]
        CG["Tier 1: CoinGecko API v3<br/>(Batch Price, Cap, 24h Vol)"] -->|403/429 CloudBlock| CP["Tier 2: CoinPaprika API v1<br/>(Global VWAP, Zero Geo-Blocks)"]
        CP -->|Fallback| BN["Tier 3: Binance Spot API<br/>(Exchange Order Book)"]
        CG --> EXT[src/extract.py]
        CP --> EXT
        BN --> EXT
        EXT --> RAW[(data/raw/raw_YYYYMMDD_HHMM.json)]
    end

    subgraph Transformation ["2. Financial Feature Engineering (Transform)"]
        EXT --> TRF[src/transform.py]
        DB_HIST[(Historical Market Data)] -.->|Load Past Context| TRF
        TRF -->|Calculate Rolling 7d/30d Avg, Daily Return, Volatility| ENR[Enriched Feature Set]
        ENR --> PRQ[(data/transformed/*.parquet)]
        ENR --> LOAD_READY[Clean Current-Day Delta]
    end

    subgraph Storage ["3. Cloud Data Warehouse (Load)"]
        LOAD_READY --> LOAD[src/load.py]
        LOAD -->|"Dynamic Dim Key Lookup"| DIM[(dim_symbol)]
        LOAD -->|"True In-Place UPSERT<br/>ON CONFLICT DO UPDATE"| FACT[(fact_market_data)]
        FACT -->|"Dialect-Aware DDL View"| VIEW[(vw_weekly_trends)]
        DB_TARGET{{"Storage Target"}}
        LOAD --> DB_TARGET
        DB_TARGET -->|"Cloud Primary"| NEON[("Neon Serverless PostgreSQL<br/>(AWS Singapore)")]
        DB_TARGET -->|"Local Fallback"| SQLITE[("Local SQLite3 Engine<br/>crypto_pipeline.db")]
    end

    subgraph Orchestration ["4. Autonomous Cloud Orchestration"]
        CRON["GitHub Actions Cron<br/>00:05 UTC (05:35 AM IST)"] --> GHA[daily_etl.yml Runner]
        GHA --> RUN_ETL[run_etl.py]
        RUN_ETL --> PIPE[pipeline.py]
        LOCAL_TASK["Windows Task Scheduler<br/>(Optional Local Runner)"] -.-> RUN_ETL
    end

    subgraph Observability ["5. 24/7 Cloud Sentinel Bot & Alerting"]
        PIPE -->|Webhook Heartbeat / Incident Alert| HOOK[Discord Channel #general]
        BOT_HOST["bot-hosting.net<br/>Isolated Linux Container"] --> BOT[bot/sentinel_bot.py]
        BOT -->|11 Slash Commands| DISCORD[Discord App / Server]
        DISCORD -->|/health, /cloud_db, /verify_etl, /run, /resources| BOT
        BOT -.->|"pg8000 + SSL"| NEON
    end

    subgraph BI ["6. Interactive Business Intelligence"]
        FACT -.->|"Direct DB Connection / ODBC DSN"| PBI["Power BI Desktop / Dashboard"]
        DIM -.->|"Star Schema (1:N Single Direction)"| PBI
        VIEW -.->|"Aggregated Trends & Visuals"| PBI
    end
```

---

## 🎯 2. Financial Feature Engineering & Formulas

In institutional quantitative finance, decision-makers rely on continuous time-series metrics to measure momentum, assess downside risk, and dynamically adjust portfolio weights.

### Mathematical Formulations

1. **Daily Percentage Return ($R_t$)**:
   $$\text{Daily Return}_t = \left(\frac{P_t - P_{t-1}}{P_{t-1}}\right) \times 100$$
   *Measures the exact percentage price change of the asset between consecutive days.*

2. **Simple Moving Averages (7-Day & 30-Day)**:
   $$\text{SMA}_{k,t} = \frac{1}{k}\sum_{i=0}^{k-1} P_{t-i} \quad \text{for } k \in \{7, 30\}$$
   *Filters out high-frequency market noise to reveal underlying intermediate and monthly price trends.*

3. **7-Day Rolling Volatility ($\sigma_{7d}$)**:
   $$\sigma_{7d} = \sqrt{\frac{1}{n-1} \sum_{i=1}^{n} (R_i - \bar{R})^2} \quad \text{over 7-day rolling window}$$
   *Quantifies historical asset turbulence and market volatility exposure.*

---

## 🗄️ 3. Star-Schema Dimensional Modeling

The warehouse uses a dimensional star schema optimized for analytical query performance, strict referential integrity, and seamless reporting across PostgreSQL and SQLite:

```
           ┌────────────────────────────┐
           │         dim_symbol         │
           ├────────────────────────────┤
           │ symbol_id (PK, SERIAL)     │◄───────────┐
           │ symbol_code (VARCHAR, UNQ) │            │ (1:N Cardinality)
           │ asset_name (VARCHAR)       │            │
           │ asset_type (VARCHAR)       │            │
           └────────────────────────────┘            │
                                                     │
           ┌────────────────────────────┐            │
           │      fact_market_data      │            │
           ├────────────────────────────┤            │
           │ fact_id (PK, SERIAL)       │            │
           │ symbol_id (FK)             │────────────┘
           │ price_usd (DOUBLE PREC)    │
           │ market_cap (DOUBLE PREC)   │
           │ volume_24h (DOUBLE PREC)   │
           │ change_24h (DOUBLE PREC)   │
           │ rolling_avg_7d (DOUBLE)    │
           │ rolling_avg_30d (DOUBLE)   │
           │ daily_return (DOUBLE PREC) │
           │ volatility_7d (DOUBLE PREC)│
           │ record_timestamp (VARCHAR) │
           ├────────────────────────────┤
           │ UNIQUE(symbol_id, timestamp│
           └────────────────────────────┘
                          │
                          ▼ (Analytical Materialized View)
           ┌────────────────────────────┐
           │      vw_weekly_trends      │
           ├────────────────────────────┤
           │ symbol_id, symbol_code     │
           │ asset_name                 │
           │ year_week (YYYY-WW)        │
           │ avg_price, avg_volume      │
           │ avg_volatility, price_range│
           │ record_count               │
           └────────────────────────────┘
```

### In-Place Idempotent UPSERT (`ON CONFLICT DO UPDATE`)

To prevent duplicate entries and avoid auto-increment primary key jumps caused by `DELETE + INSERT` semantics, the pipeline executes a **True In-Place UPSERT**:

```sql
INSERT INTO fact_market_data (
    symbol_id, price_usd, market_cap, volume_24h, change_24h,
    rolling_avg_7d, rolling_avg_30d, daily_return, volatility_7d, record_timestamp
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol_id, record_timestamp) 
DO UPDATE SET
    price_usd       = EXCLUDED.price_usd,
    market_cap      = EXCLUDED.market_cap,
    volume_24h      = EXCLUDED.volume_24h,
    change_24h      = EXCLUDED.change_24h,
    rolling_avg_7d  = EXCLUDED.rolling_avg_7d,
    rolling_avg_30d = EXCLUDED.rolling_avg_30d,
    daily_return    = EXCLUDED.daily_return,
    volatility_7d   = EXCLUDED.volatility_7d;
```

---

## 🌐 4. Cloud Infrastructure & Multi-Driver Database Engine

### Universal Database Engine (`create_db_engine`)
The pipeline runs seamlessly across local developer workstations, GitHub Actions runners, and containerized Linux environments through an intelligent auto-negotiation engine in `src/utils.py`:

- **Psycopg 3 (`psycopg[binary]`)**: Modern official driver for Python 3.11–3.14+ with C binary extensions.
- **Psycopg 2 (`psycopg2-binary`)**: Legacy production adapter.
- **pg8000 (`pg8000`)**: Pure-Python PostgreSQL driver with custom SSL context (used on lightweight container hosts where C compilers are unavailable).
- **SQLite3**: Fully embedded offline database engine.

```python
from src.utils import create_db_engine
# Auto-negotiates the optimal driver without manual dialect conversions
engine = create_db_engine(os.getenv("NEON_DB_URL"))
```

---

## 🤖 5. 24/7 Discord Sentinel Bot (`bot/sentinel_bot.py`)

Hosted 24/7 on an isolated Linux cloud container (`bot-hosting.net`), the Sentinel Bot acts as a dedicated Command & Control Center for data pipeline observability.

### Complete Slash Commands Suite (11 Commands)

| Command | Category | Description |
|---|---|---|
| `/health` | **Diagnostics** | Real-time full-stack health report (Neon DB status, Python 3.14 runtime, API reachability, container telemetry). |
| `/cloud_db` | **Database** | Live Neon PostgreSQL connection telemetry, round-trip latency (ms), storage usage, and row counts. |
| `/verify_etl` | **Audit** | Verifies if today's ETL execution successfully populated records in Neon PostgreSQL. |
| `/resources` | **Navigation** | Centralized project directory linking to Neon Console, GitHub Actions, Discord Server, and APIs. |
| `/run` | **Execution** | Dispatches an immediate remote execution of the GitHub Actions Cloud ETL workflow via GitHub REST API. |
| `/status` | **Monitoring** | Pipeline health, latest batch execution timestamps, and failure incident telemetry. |
| `/refresh_views` | **Analytics** | Re-computes and refreshes the analytical view `vw_weekly_trends` across all historical data. |
| `/pipeline_summary` | **Statistics** | High-level summary of all tracked symbols, aggregate row counts, and date ranges. |
| `/export_parquet` | **Backup** | Exports fact tables into compressed Apache Parquet audit archives. |
| `/test` | **Quality** | Runs local unit test verification suite guidelines and diagnostic tests. |
| `/help` | **Manual** | Interactive documentation guide explaining all commands and architecture details. |

---

## ⏰ 6. Cloud Automation & Scheduling

The cloud pipeline is configured to execute daily at **00:05 UTC (05:35 AM IST)** via GitHub Actions:
- **Optimal Crypto Market Timing:** Global cryptocurrency daily bars close at **00:00 UTC**. Running at 00:05 UTC ensures the final daily closing prices, daily returns, and trading volumes are 100% captured without missing volatility.
- **Zero Laptop Dependency:** Completely automated in the cloud without requiring a local machine to be turned on.

```yaml
# .github/workflows/daily_etl.yml
name: Daily Scheduled ETL Pipeline
on:
  schedule:
    - cron: '5 0 * * *'  # 00:05 UTC = 05:35 AM IST
  workflow_dispatch:      # 1-click on-demand manual trigger
```

---

## 📊 7. Power BI Business Intelligence & Reporting

The warehouse is connected directly to **Microsoft Power BI Desktop** (`crypto_dash.pbix`) for executive reporting.

### Model View (Star Schema)
- `dim_symbol[symbol_id]` $\xrightarrow{1:N}$ `fact_market_data[symbol_id]`
  - **Cardinality:** 1 to Many (`1:*`)
  - **Cross filter direction:** Single (`dim_symbol` filters `fact_market_data`)
- `vw_weekly_trends`: Standalone aggregated analytical view.

### Recommended Visualizations
1. **Executive KPI Cards**:
   - `Latest Price`: `SELECTEDVALUE(fact_market_data[price_usd])` formatted as `$#,##0.00`
   - `24h Return`: `SELECTEDVALUE(fact_market_data[change_24h])` with dynamic conditional color formatting (Green for $\ge 0$, Red for $< 0$)
   - `7d Volatility`: `SELECTEDVALUE(fact_market_data[volatility_7d])` formatted as `0.00%`
2. **Asset Slicer**:
   - Tile or pill slicer for `dim_symbol[symbol_code]` (BTC, ETH, SOL).
3. **Price & Rolling Averages Line Chart**:
   - **X-Axis:** `record_timestamp`
   - **Y-Axis:** `price_usd`, `rolling_avg_7d`, `rolling_avg_30d`
4. **Weekly Market Overview Table**:
   - Aggregated metrics from `vw_weekly_trends`: `year_week`, `avg_price`, `avg_volume`, `price_range`, `record_count`.

---

## 🧪 8. Automated Testing Suite (25 Tests Passing)

All modules are strictly tested with automated mock fixtures covering API rate limiting (HTTP 429), exponential jitter backoffs, server error retries (500/503), schema integrity, and Discord alert payloads:

```bash
$ pytest tests/ -v
============================= test session starts =============================
platform win32 -- Python 3.11.9, pytest-7.4.0, pluggy-1.6.0
collected 25 items

tests/test_alerts.py .....                                               [ 20%]
tests/test_extract.py ..........                                         [ 60%]
tests/test_transform.py ..........                                       [100%]

============================= 25 passed in 3.37s ==============================
```

---

## 🛠️ 9. Quickstart & Installation Guide

### Prerequisites
- Python 3.11+
- Git
- Neon PostgreSQL Account (or local SQLite)

### 1. Clone & Setup Virtual Environment
```bash
git clone https://github.com/HarshaNaik8/crypto-data-pipeline.git
cd crypto-data-pipeline

python -m venv venv
# Windows:
.\venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment (`.env`)
```ini
# Core Configuration
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
SYMBOLS=bitcoin,ethereum,solana
LOG_LEVEL=INFO

# Cloud Database (Neon PostgreSQL)
NEON_DB_URL=postgresql+psycopg://user:password@endpoint.neon.tech/crypto_db?sslmode=require

# Observability
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/your-webhook-id/your-webhook-token
DISCORD_BOT_TOKEN=your-bot-token
```

### 4. Execute Pipeline Locally
```bash
python run_etl.py
```

### 5. Launch Discord Sentinel Bot Locally (or via bot-hosting.net)
```bash
python bot/sentinel_bot.py
```

---

## 👤 Author & License

**Harsha Naik**  
- **GitHub:** [@HarshaNaik8](https://github.com/HarshaNaik8)  
- **LinkedIn:** [Harsha Naik](https://www.linkedin.com/in/harsha-naik-664694292/)

Licensed under the [MIT License](LICENSE).