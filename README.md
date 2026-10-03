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
        EXT --> RAW["data/raw/raw_YYYYMMDD_HHMM.json"]
    end

    subgraph Transformation ["2. Financial Feature Engineering (Transform)"]
        EXT --> TRF[src/transform.py]
        DB_HIST["Historical Market Data"] -.->|Load Past Context| TRF
        TRF -->|"Calculate Rolling 7d/30d Avg, Daily Return, Volatility"| ENR[Enriched Feature Set]
        ENR --> PRQ["data/transformed/*.parquet"]
        ENR --> LOAD_READY[Clean Current-Day Delta]
    end

    subgraph Storage ["3. Cloud Data Warehouse (Load)"]
        LOAD_READY --> LOAD[src/load.py]
        LOAD -->|"Dynamic Dim Key Lookup"| DIM["dim_symbol"]
        LOAD -->|"True In-Place UPSERT<br/>ON CONFLICT DO UPDATE"| FACT["fact_market_data"]
        FACT -->|"Dialect-Aware DDL View"| VIEW["vw_weekly_trends"]
        LOAD --> NEON["Neon Serverless PostgreSQL<br/>(AWS Singapore)"]
    end

    subgraph Orchestration ["4. Autonomous Cloud Orchestration"]
        CRON["GitHub Actions Cron<br/>00:05 UTC (05:35 AM IST)"] --> GHA[daily_etl.yml Runner]
        GHA --> RUN_ETL[run_etl.py]
        RUN_ETL --> PIPE[pipeline.py]
    end

    subgraph Observability ["5. 24/7 Cloud Sentinel Bot & Alerting"]
        PIPE -->|Webhook Heartbeat / Incident Alert| HOOK[Discord Channel]
        BOT_HOST["bot-hosting.net<br/>Isolated Linux Container"] --> BOT[bot/sentinel_bot.py]
        BOT -->|"11 Slash Commands"| DISCORD[Discord App / Server]
        DISCORD -->|"/health, /market, /cloud_db, /verify_etl, /resources"| BOT
        BOT -.->|"pg8000 + SSL + pool_pre_ping"| NEON
    end

    subgraph BI ["6. Interactive Business Intelligence"]
        FACT -.->|"Direct DB Import Connection"| PBI["Power BI Desktop"]
        DIM -.->|"Star Schema (1:N)"| PBI
        VIEW -.->|"Aggregated Trends"| PBI
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

The warehouse uses a dimensional star schema optimized for analytical query performance, strict referential integrity, and seamless BI reporting:

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
           │ UNIQUE(symbol_id,          │
           │        record_timestamp)   │
           └────────────────────────────┘
                          │
                          ▼ (Analytical View)
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

To prevent duplicate entries on re-runs, the pipeline executes a **True In-Place UPSERT**:

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
The pipeline runs seamlessly across GitHub Actions runners and containerized Linux environments through an intelligent auto-negotiation engine in `src/utils.py`:

- **Psycopg 3 (`psycopg[binary]`)**: Modern official driver for Python 3.11–3.14+ with C binary extensions. Used on GitHub Actions.
- **pg8000 (`pg8000`)**: Pure-Python PostgreSQL driver with custom SSL context. Used on bot-hosting.net containers where C compilers are unavailable.
- **Connection Resilience**: All engines are created with `pool_pre_ping=True` and `pool_recycle=300` to survive Neon Serverless sleep cycles.

```python
from src.utils import create_db_engine
# Auto-negotiates the optimal driver without manual dialect conversions
engine = create_db_engine(os.getenv("NEON_DB_URL"))
```

---

## 🤖 5. 24/7 Discord Sentinel Bot (`bot/sentinel_bot.py`)

Hosted 24/7 on an isolated Linux cloud container ([bot-hosting.net](https://bot-hosting.net/a)), the Sentinel Bot acts as a dedicated Command & Control Center for data pipeline observability. It connects to Neon PostgreSQL via `pg8000` with SSL and resilient connection pooling.

### Complete Slash Commands Suite (11 Commands)

| Command | Category | Description |
|---|---|---|
| `/status` | **Monitoring** | Pipeline health overview — date range, total rows, unique days, DB size. |
| `/market` | **Market Data** | Latest market snapshot — prices, 24h change, volume, market cap, 7d avg, volatility, daily return for all tracked assets. |
| `/health` | **Diagnostics** | Full-stack system health check — Neon DB status, Python runtime, environment secrets, CoinGecko API, webhook, and infrastructure telemetry. |
| `/cloud_db` | **Database** | Live Neon PostgreSQL deep-dive — connection latency (ms), active connections, storage size, table row counts, and engine version. |
| `/dbstats` | **Database** | Detailed per-table and per-symbol statistics — `dim_symbol` listing, `fact_market_data` row counts with date ranges per asset, and `vw_weekly_trends` aggregates. |
| `/history` | **Analytics** | Historical price & volatility trend for a chosen asset (BTC/ETH/SOL) — last 7 recorded days with daily return and 7d volatility. |
| `/verify_etl` | **Audit** | Verifies if today's scheduled ETL execution has completed and shows the prices loaded for each symbol. |
| `/test` | **Quality** | Runs a live 6-point integration smoke test (DB handshake, dimension integrity, fact integrity, CoinGecko/CoinPaprika/Binance API pings) with millisecond latencies. Includes a direct link to trigger the full 25-test Pytest suite on GitHub Actions. |
| `/run` | **Execution** | Triggers the ETL pipeline — dispatches GitHub Actions via REST API (if `GITHUB_TOKEN` is set), runs locally, or provides a 1-click manual trigger link. |
| `/resources` | **Navigation** | Centralized architecture hub — direct links to GitHub repo, GitHub Actions, Neon Console, Bot-Hosting panel, CoinGecko API, CoinPaprika API, Binance API, and Power BI. |
| `/help_pipe` | **Manual** | Interactive command reference listing all 11 slash commands with descriptions. |

---

## ⏰ 6. Cloud Automation & Scheduling

The cloud pipeline is configured to execute daily at **00:05 UTC (05:35 AM IST)** via GitHub Actions:
- **Optimal Crypto Market Timing:** Global cryptocurrency daily bars close at **00:00 UTC**. Running at 00:05 UTC ensures the final daily closing prices, daily returns, and trading volumes are 100% captured.
- **Zero Laptop Dependency:** Completely automated in the cloud without requiring a local machine to be turned on.
- **Bot-Hosting Renewal Reminder:** A separate GitHub Actions workflow (`bot_renewal_reminder.yml`) sends a Discord notification every 3 days reminding you to renew the free bot-hosting.net plan.

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

The warehouse is connected directly to **Microsoft Power BI Desktop** (`crypto_dash.pbix`) via Import mode from Neon PostgreSQL.

### Model View (Star Schema)
- `dim_symbol[symbol_id]` → `fact_market_data[symbol_id]`
  - **Cardinality:** 1 to Many (1:*)
  - **Cross filter direction:** Single (`dim_symbol` filters `fact_market_data`)
- `vw_weekly_trends`: Standalone aggregated analytical view.

### Column Formatting
| Column | Format | Decimals |
|--------|--------|----------|
| `price_usd` | Currency ($) | 2 |
| `volume_24h` | Currency ($) | 0 |
| `market_cap` | Currency ($) | 0 |
| `rolling_avg_7d` | Currency ($) | 2 |
| `rolling_avg_30d` | Currency ($) | 2 |
| `change_24h` | Percentage (%) | 2 |
| `daily_return` | Percentage (%) | 2 |
| `volatility_7d` | Percentage (%) | 2 |
| `record_timestamp` | Date | Short Date |

### Recommended Visualizations
1. **Executive KPI Cards**: Latest Price, 24h Return (conditional color), 7d Volatility.
2. **Asset Slicer**: Tile or pill slicer for `dim_symbol[symbol_code]` (BTC, ETH, SOL).
3. **Price & Rolling Averages Line Chart**: X-Axis: `record_timestamp`, Y-Axis: `price_usd`, `rolling_avg_7d`, `rolling_avg_30d`.
4. **Weekly Market Overview Table**: Aggregated metrics from `vw_weekly_trends`.

---

## 🧪 8. Automated Testing Suite (25 Tests Passing)

All core modules are tested with automated mock fixtures covering API rate limiting (HTTP 429), exponential jitter backoffs, server error retries (500/503), schema integrity, and Discord alert payloads:

```bash
$ pytest tests/ -v
============================= test session starts =============================
platform linux -- Python 3.11, pytest-7.4.0
collected 25 items

tests/test_alerts.py .....                                               [ 20%]
tests/test_extract.py ..........                                         [ 60%]
tests/test_transform.py ..........                                       [100%]

============================= 25 passed ==============================
```

Tests run automatically on every `git push` to `main` via the CI workflow (`.github/workflows/ci.yml`).

---

## 🛠️ 9. Quickstart & Installation Guide

### Prerequisites
- Python 3.11+
- Git
- Neon PostgreSQL Account (free tier at [neon.tech](https://neon.tech))

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

### 5. Launch Discord Sentinel Bot
```bash
python bot/sentinel_bot.py
```

---

## 📁 10. Project Structure

```
crypto-data-pipeline/
├── .github/workflows/
│   ├── ci.yml                    # CI: 25-test pytest suite on every push
│   ├── daily_etl.yml             # Daily ETL cron (00:05 UTC)
│   └── bot_renewal_reminder.yml  # 3-day Discord reminder for bot-hosting renewal
├── bot/
│   ├── __init__.py
│   └── sentinel_bot.py           # 24/7 Discord bot (11 slash commands)
├── src/
│   ├── __init__.py
│   ├── extract.py                # 3-tier API extractor (CoinGecko → CoinPaprika → Binance)
│   ├── transform.py              # Feature engineering (rolling avg, volatility, daily return)
│   ├── load.py                   # Star-schema loader with UPSERT
│   ├── alerts.py                 # Discord webhook success/failure notifications
│   ├── utils.py                  # Universal DB engine, logging setup
│   └── weekly_aggregate.py       # vw_weekly_trends view refresh
├── tests/
│   ├── test_alerts.py            # Alert payload tests
│   ├── test_extract.py           # API extraction & retry tests
│   └── test_transform.py         # Feature engineering & schema tests
├── scripts/
│   ├── backfill_market_data.py   # Historical data backfill utility
│   ├── migrate_sqlite_to_postgres.py  # SQLite → Neon migration
│   └── migrate_add_unique_constraint.py  # Schema migration
├── data/
│   ├── raw/                      # Raw JSON audit trail (timestamped)
│   └── transformed/              # Parquet backups (timestamped)
├── pipeline.py                   # Core ETL orchestrator
├── run_etl.py                    # Entry point for scheduled execution
├── crypto_dash.pbix              # Power BI dashboard file
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variable template
├── LICENSE                       # MIT License
└── README.md                     # This file
```

---

## 👤 Author & License

**Harsha Naik**  
- **GitHub:** [@HarshaNaik8](https://github.com/HarshaNaik8)  
- **LinkedIn:** [Harsha Naik](https://www.linkedin.com/in/harsha-naik-664694292/)

Licensed under the [MIT License](LICENSE).