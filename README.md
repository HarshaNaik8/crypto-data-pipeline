# 🚀 Real-Time Crypto Market Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![SQLite](https://img.shields.io/badge/SQLite-Database-green)
![PowerBI](https://img.shields.io/badge/PowerBI-Dashboard-yellow)
![Automation](https://img.shields.io/badge/Automation-Task_Scheduler-orange)

## 📊 Architecture Diagram

```mermaid
graph LR
    A[CoinGecko API] -->|HTTP GET Daily| B(Python ETL Script)
    B -->|Pandas Transform| C[Clean & Enriched Data]
    C -->|SQLAlchemy| D[(SQLite Database)]
    D -->|SQL View| E[Weekly Aggregates]
    D -->|ODBC| F[Power BI Dashboard]
    G[Windows Task Scheduler] -->|Triggers at 9 AM| B
    H[GitHub] -->|Stores Code| I[Recruiter / Portfolio]