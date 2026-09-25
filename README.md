# 🚀 Production-Grade Real-Time Crypto Market Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Database](https://img.shields.io/badge/SQLite-Star--Schema-003B57?style=flat&logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![Testing](https://img.shields.io/badge/Tests-20%2F20%20Passing-brightgreen?style=flat&logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![Power BI](https://img.shields.io/badge/Power_BI-ODBC_Direct-F2C811?style=flat&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Automation](https://img.shields.io/badge/Automation-Windows_Task_Scheduler-0078D4?style=flat&logo=windows&logoColor=white)](https://learn.microsoft.com/en-us/windows/win32/taskschd/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An enterprise-ready, end-to-end data engineering pipeline that automatically extracts, validates, transforms, stores, and visualizes live cryptocurrency market data (Bitcoin, Ethereum, Solana). Designed with financial engineering principles, idempotency, database-level integrity, resilience against API rate-limiting, automated orchestration, and interactive business intelligence reporting.

---

## 🏗️ 1. Architecture Flow

```mermaid
flowchart TD
    subgraph Ingestion ["1. Data Ingestion (Extract)"]
        CG[CoinGecko REST API] -->|GET /simple/price<br/>Backoff & 429 Jitter Retry| EXT[src/extract.py]
        EXT -->|Raw JSON Audit Trail| RAW[(data/raw/raw_YYYYMMDD_HHMM.json)]
    end

    subgraph Transformation ["2. Feature Engineering (Transform)"]
        EXT --> TRF[src/transform.py]
        DB_HIST[(fact_market_data)] -.->|Load Historical Context| TRF
        TRF -->|Rolling 7d/30d Avg, Daily Return, 7d Volatility| ENR[Enriched Features]
        ENR -->|Parquet Audit Archive| PRQ[(data/transformed/*.parquet)]
        ENR -->|Filter to Current Day Records| LOAD_READY[Clean Transformed Delta]
    end

    subgraph Storage ["3. Data Warehousing (Load)"]
        LOAD_READY --> LOAD[src/load.py]
        LOAD -->|Dynamic Dim Key Lookup| DIM[(dim_symbol)]
        LOAD -->|Atomic INSERT OR REPLACE<br/>UNIQUE Constraint Guard| FACT[(fact_market_data)]
        FACT -->|DDL View Creation| VIEW[(vw_weekly_trends)]
    end

    subgraph Orchestration ["4. Orchestration & Scheduling"]
        WTS[Windows Task Scheduler<br/>Daily 11:00 AM] --> RUN_BAT[run_pipeline.bat]
        RUN_BAT --> RUN_ETL[run_etl.py]
        RUN_ETL --> pipeline[pipeline.py]
    end

    subgraph BI ["5. Analytics & Dashboard"]
        VIEW -.->|ODBC DSN: CryptoDB| PBI[Power BI Desktop / Dashboard]
        FACT -.->|Star Schema (1:*)| PBI
        DIM -.->|Dimension Filtering| PBI
    end
```

---

## 🎯 2. Business Value & Financial Engineering

In institutional investment environments (e.g., Goldman Sachs, Morgan Stanley), decision-makers depend on reliable real-time and historical price series to evaluate market volatility, assess risk-adjusted returns, and rebalance assets. 

### Engineered Financial Metrics

1. **Daily Return ($R_t$)**:
   $$\text{Daily Return}_t = \left(\frac{P_t - P_{t-1}}{P_{t-1}}\right) \times 100$$
   *Captures percentage asset movement between consecutive trading periods.*

2. **Rolling Moving Averages (7-Day & 30-Day)**:
   $$\text{SMA}_{k,t} = \frac{1}{k}\sum_{i=0}^{k-1} P_{t-i} \quad \text{for } k \in \{7, 30\}$$
   *Smooths short-term price noise to highlight intermediate and monthly momentum.*

3. **7-Day Rolling Volatility ($\sigma_{7d}$)**:
   $$\sigma_{7d} = \sqrt{\frac{1}{n-1} \sum_{i=1}^{n} (R_i - \bar{R})^2} \quad \text{over 7-day rolling window}$$
   *Quantifies asset price turbulence and risk exposure.*

---

## 🗄️ 3. Data Warehouse Architecture (Star Schema)

The analytical store implements a dimensional model with explicit primary/foreign keys, database-level uniqueness, index optimization, and IEEE floating-point precision:

```
           ┌────────────────────────────┐
           │         dim_symbol         │
           ├────────────────────────────┤
           │ symbol_id (PK, AUTOINC)    │◄───────────┐
           │ symbol_code (VARCHAR, UNQ) │            │ (1:N)
           │ asset_name (VARCHAR)       │            │
           │ asset_type (VARCHAR)       │            │
           └────────────────────────────┘            │
                                                     │
           ┌────────────────────────────┐            │
           │      fact_market_data      │            │
           ├────────────────────────────┤            │
           │ fact_id (PK, AUTOINC)      │            │
           │ symbol_id (FK)             │────────────┘
           │ price_usd (REAL)           │
           │ market_cap (REAL)          │
           │ volume_24h (REAL)          │
           │ change_24h (REAL)          │
           │ rolling_avg_7d (REAL)      │
           │ rolling_avg_30d (REAL)     │
           │ daily_return (REAL)        │
           │ volatility_7d (REAL)       │
           │ record_timestamp (TEXT)    │
           ├────────────────────────────┤
           │ UNIQUE(symbol_id, timestamp│
           └────────────────────────────┘
                         │
                         ▼ (Aggregated View)
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

### Table Definitions & Schema DDL

```sql
-- Dimension: dim_symbol
CREATE TABLE dim_symbol (
    symbol_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_code VARCHAR(10) UNIQUE NOT NULL,
    asset_name  VARCHAR(50),
    asset_type  VARCHAR(20) DEFAULT 'crypto'
);

-- Fact: fact_market_data
CREATE TABLE fact_market_data (
    fact_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id        INTEGER NOT NULL,
    price_usd        REAL,
    market_cap       REAL,
    volume_24h       REAL,
    change_24h       REAL,
    rolling_avg_7d   REAL,
    rolling_avg_30d  REAL,
    daily_return     REAL,
    volatility_7d    REAL,
    record_timestamp TEXT NOT NULL,
    UNIQUE (symbol_id, record_timestamp),
    FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id)
);

CREATE UNIQUE INDEX idx_symbol_date ON fact_market_data(symbol_id, record_timestamp);
CREATE INDEX idx_timestamp ON fact_market_data(record_timestamp);
CREATE INDEX idx_symbol_id ON fact_market_data(symbol_id);
```

---

## ⚡ 4. Engineering Highlights & Hardening

| Feature | Child/Script Implementation | Production-Grade Implementation |
|---|---|---|
| **Rate Limit Handling** | Crashes or exits on HTTP 429 | Exponential backoff + full jitter, specifically distinguishing 429 & 5xx from true 4xx client errors |
| **Idempotency** | Application-level `SELECT` checks | Native database-level `UNIQUE(symbol_id, record_timestamp)` + atomic `INSERT OR REPLACE` |
| **Symbol Resolution** | Hardcoded dictionaries (`{'BITCOIN': 1}`) | Dynamic metadata queries against `dim_symbol` |
| **Historical Calculations** | Inaccurate rolling stats without past data | Pre-fetches past DB context before window calculations; returns only delta to warehouse loader |
| **Data Types** | Implicit SQLite dynamic typing (rounding issues in BI) | Explicit `REAL` schemas with type-safe `safe_float()` casting |
| **Batch Runner** | Interactive `pause` statements locking background tasks | Silent execution with exit code propagation (`exit /b %ERRORLEVEL%`) |
| **Testing** | Manual inspection | Automated Pytest test suite covering mock extractions, edge cases, and schema constraints |

---

## 📁 5. Repository Structure

```
crypto-pipeline/
├── src/
│   ├── __init__.py
│   ├── extract.py          # Resilient API extractor with backoff retry
│   ├── transform.py        # Context-aware financial feature engineering
│   ├── load.py             # Atomic database loader with star-schema enforcement
│   ├── weekly_aggregate.py # SQL view generator for aggregated trends
│   └── utils.py            # Centralized UTF-8 logging, DB helpers, and float casters
├── scripts/
│   ├── backfill_market_data.py          # OHLCV market chart backfill utility
│   └── migrate_add_unique_constraint.py # One-time DDL table migration script
├── tests/
│   ├── __init__.py
│   ├── test_extract.py     # 10 tests for backoff logic, retry behavior, response schemas
│   └── test_transform.py   # 10 tests for calculations, idempotency, and constraints
├── data/                   # (Gitignored) Raw JSON and transformed Parquet snapshots
├── logs/                   # (Gitignored) UTF-8 pipeline execution logs
├── pipeline.py             # Core ETL orchestrator function
├── run_etl.py              # Task Scheduler execution entry point
├── run_pipeline.bat        # Automated Windows batch execution script
├── crypto_pipeline.db      # SQLite database file (Star Schema)
├── crypto_dash.pbix        # Power BI interactive business dashboard
├── requirements.txt        # Pinned Python dependencies
├── .env.example            # Environment variables configuration template
├── .gitignore              # Production git exclusions
├── LICENSE                 # MIT License
└── README.md               # Complete project documentation
```

---

## 🛠️ 6. Quickstart & Installation

### Prerequisites
- Python 3.11+
- Windows 10/11 (for Windows Task Scheduler) or Linux/macOS (cron)
- Power BI Desktop (for dashboard) + SQLite ODBC Driver (64-bit)

### 1. Clone & Create Virtual Environment
```bash
git clone https://github.com/HarshaNaik8/crypto-data-pipeline.git
cd crypto-data-pipeline

python -m venv venv
# On Windows PowerShell:
.\venv\Scripts\Activate.ps1
# On Windows Command Prompt:
venv\Scripts\activate.bat
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
```bash
cp .env.example .env
```
Default configuration:
```ini
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
SYMBOLS=bitcoin,ethereum,solana
DB_CONNECTION_STRING=sqlite:///crypto_pipeline.db
LOG_LEVEL=INFO
```

### 4. Run Automated Test Suite
Ensure all 20 tests pass before running or scheduling:
```bash
pytest tests/ -v
```

---

## ⏰ 7. Automated Orchestration (Windows Task Scheduler)

The pipeline is designed to be triggered autonomously via Windows Task Scheduler.

1. Open **Task Scheduler** (`taskschd.msc`) on Windows.
2. Click **Create Task...** (not Basic Task) in the Actions pane:
   - **General Tab**:
     - Name: `CryptoDataPipeline`
     - Select: *Run whether user is logged on or not* (or *Run only when user is logged on*).
     - Check: *Run with highest privileges*.
   - **Triggers Tab**:
     - Click **New...**
     - Begin the task: *On a schedule*
     - Settings: **Daily**, Start at **11:00:00 AM**, Recur every **1** day.
   - **Actions Tab**:
     - Action: *Start a program*
     - Program/script: `D:\4th_year\project\crypto-pipeline\run_pipeline.bat`
     - Start in: `D:\4th_year\project\crypto-pipeline`
   - **Settings Tab**:
     - ✅ Check: *Run task as soon as possible after a scheduled start is missed* (Ensures catch-up if computer is powered off at 11:00 AM).
     - ✅ Check: *If the task fails, restart every: 10 minutes, Attempt to restart up to: 3 times*.
     - ✅ Check: *Allow task to be run on demand*.

---

## 📊 8. Business Intelligence: Power BI vs Tableau

### Why Power BI is the Best Choice for this Pipeline

| Feature | Microsoft Power BI (Recommended) | Tableau Desktop |
|---|---|---|
| **Direct SQLite Connection** | Native System DSN ODBC integration works seamlessly with direct refresh. | Requires custom JDBC or extracts; slower on embedded SQLite. |
| **Dimensional Modeling** | Full Star-Schema relationship engine (`1:*` cardinality, single-direction cross-filtering). | Requires physical/logical layer relationship mappings or joins. |
| **Financial Calculations** | DAX offers optimized time-intelligence and moving aggregations. | Table calculations and Level of Detail (LOD) can be rigid. |
| **Cost & Deployment** | Free Power BI Desktop; simple `.pbix` portability in Git repositories. | Expensive commercial licensing; `.twbx` packaging issues. |

---

## 📈 9. Step-by-Step BI Connection & Dashboard Setup

### Step 1: Create Windows ODBC System DSN
1. Download and install the official **SQLite3 ODBC Driver (64-bit)** (e.g., `sqliteodbc_w64.exe`).
2. Press `Win + R`, type `odbcad32.exe`, and select the **System DSN** tab.
3. Click **Add...** → Select **SQLite3 ODBC Driver** → Click **Finish**.
4. Configure DSN:
   - **Data Source Name:** `CryptoDB`
   - **Database Name:** Browse and select `D:\4th_year\project\crypto-pipeline\crypto_pipeline.db`
   - Leave options default and click **OK**.

### Step 2: Import into Power BI Desktop
1. Open **Power BI Desktop**.
2. Click **Get Data** → **ODBC** → Click **Connect**.
3. Select **DSN:** `CryptoDB`.
4. In the Navigator dialog, check:
   - `dim_symbol`
   - `fact_market_data`
   - `vw_weekly_trends`
5. Click **Load**.

### Step 3: Configure Star Schema Relationships
Navigate to the **Model View** tab in Power BI:
1. Connect `dim_symbol[symbol_id]` $\rightarrow$ `fact_market_data[symbol_id]`:
   - **Cardinality:** `1 to Many (1:*)`
   - **Cross filter direction:** `Single (dim_symbol filters fact_market_data)`
2. Ensure `vw_weekly_trends` is kept as a **standalone table** (Do not link it to fact table to avoid cyclic evaluation warnings).

### Step 4: Recommended Dashboard Visuals
- **Executive KPI Cards**:
  - Latest Price: `SELECTEDVALUE(fact_market_data[price_usd])`
  - 24h Price Change: `SELECTEDVALUE(fact_market_data[change_24h])`
  - 7-Day Volatility: `SELECTEDVALUE(fact_market_data[volatility_7d])`
- **Asset Slicer**:
  - Drop `dim_symbol[symbol_code]` (Bitcoin, Ethereum, Solana) into a button or dropdown slicer.
- **Price Trend Visual (Line Chart)**:
  - **X-Axis:** `fact_market_data[record_timestamp]`
  - **Y-Axis:** `price_usd`, `rolling_avg_7d`, `rolling_avg_30d`
- **Weekly Overview (Table/Matrix)**:
  - Display from `vw_weekly_trends`: `year_week`, `avg_price`, `avg_volume`, `price_range`.

---

## 🧪 10. Automated Testing Suite

The repository contains 20 comprehensive unit and integration tests located in `tests/`:

```bash
$ pytest tests/ -v
============================= test session starts =============================
tests/test_extract.py::TestShouldGiveUp::test_gives_up_on_400 PASSED     [  5%]
tests/test_extract.py::TestShouldGiveUp::test_gives_up_on_403 PASSED     [ 10%]
tests/test_extract.py::TestShouldGiveUp::test_retries_on_429 PASSED      [ 15%]
tests/test_extract.py::TestShouldGiveUp::test_retries_on_500 PASSED      [ 20%]
tests/test_extract.py::TestShouldGiveUp::test_retries_on_503 PASSED      [ 25%]
tests/test_extract.py::TestShouldGiveUp::test_retries_on_network_error PASSED [ 30%]
tests/test_extract.py::TestCoinGeckoExtractor::test_extract_all_returns_dataframe PASSED [ 35%]
tests/test_extract.py::TestCoinGeckoExtractor::test_extract_all_has_required_columns PASSED [ 40%]
tests/test_extract.py::TestCoinGeckoExtractor::test_symbol_is_uppercased PASSED [ 45%]
tests/test_extract.py::TestCoinGeckoExtractor::test_all_symbols_fail_raises PASSED [ 50%]
tests/test_transform.py::TestDataTransformer::test_basic_transform_returns_dataframe PASSED [ 55%]
tests/test_transform.py::TestDataTransformer::test_output_has_required_columns PASSED [ 60%]
tests/test_transform.py::TestDataTransformer::test_daily_return_is_nonzero_after_first_row PASSED [ 65%]
tests/test_transform.py::TestDataTransformer::test_no_negative_volatility PASSED [ 70%]
tests/test_transform.py::TestDataTransformer::test_empty_dataframe_raises PASSED [ 75%]
tests/test_transform.py::TestDataTransformer::test_symbol_upper_cased PASSED [ 80%]
tests/test_transform.py::TestDataTransformer::test_no_nan_in_critical_columns PASSED [ 85%]
tests/test_transform.py::TestLoadTransformIntegration::test_load_inserts_rows PASSED [ 90%]
tests/test_transform.py::TestLoadTransformIntegration::test_upsert_no_duplicates PASSED [ 95%]
tests/test_transform.py::TestLoadTransformIntegration::test_fact_has_unique_constraint PASSED [100%]
============================= 20 passed in 2.94s ==============================
```

---

## 👤 Author & License

**Harsha Naik**  
- GitHub: [@HarshaNaik8](https://github.com/HarshaNaik8)  
- LinkedIn: [Harsha Naik](https://www.linkedin.com/in/harsha-naik-664694292/)  

Licensed under the [MIT License](LICENSE).