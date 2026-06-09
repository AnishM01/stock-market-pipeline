# Stock Market Data Pipeline

An end-to-end batch data pipeline built on Azure and Databricks that ingests daily stock market data for 37 tickers across 6 sectors, transforms it through a medallion architecture, and produces 6 business-ready gold tables for financial analysis.

---

## Architecture

```
Yahoo Finance API
      ↓
Python Ingestion (Databricks Notebook)
      ↓
Bronze Layer — Raw Parquet files in Azure Data Lake Gen2
      ↓
Silver Layer — Cleaned Delta tables (PySpark + SQL)
      ↓
Gold Layer — Business metrics Delta tables (SQL)
      ↓
Databricks Workflow — Automated weekly execution
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Lake | Azure Data Lake Storage Gen2 |
| Data Processing | Azure Databricks |
| Query Engine | Apache Spark / Spark SQL |
| Table Format | Delta Lake |
| Data Catalog | Unity Catalog |
| Ingestion | Python, yfinance |
| Orchestration | Databricks Workflows |
| Version Control | Git / GitHub |

---

## Pipeline Layers

### Bronze — Raw Ingestion
- Pulls 1 year of daily OHLCV data for 37 tickers from Yahoo Finance API
- Lands raw Parquet files in ADLS Gen2 partitioned by ticker and date
- 9,287 rows across 37 tickers and 251 trading days

### Silver — Cleaned Data
- Casts data types (timestamp → date, long → integer)
- Rounds prices to 2 decimal places
- Adds sector classification (FAANG, Big Tech, Finance, Healthcare, Retail, Energy, ETF)
- Removes duplicates and validates no nulls in critical columns
- Written as managed Delta table in Unity Catalog

### Gold — Business Metrics
Six Delta tables built for financial analysis:

| Table | Description |
|---|---|
| `daily_prices` | Clean prices with daily % return per ticker |
| `moving_averages` | 7-day and 30-day rolling average closing prices |
| `sector_summary` | Daily aggregations by sector — avg price, avg return, total volume |
| `volatility` | 30-day rolling standard deviation of returns (risk metric) |
| `fifty_two_week` | 52-week high/low with % distance from each extreme |
| `volume_spikes` | Flags days where volume exceeds 2x the 30-day average |

---

## Tickers Tracked

**FAANG (5):** META, AAPL, AMZN, NFLX, GOOGL

**Big Tech (5):** MSFT, NVDA, AMD, INTC, TSLA

**Finance (8):** JPM, GS, BAC, MS, V, MA, AXP, BLK

**Healthcare (5):** JNJ, PFE, UNH, ABBV, MRK

**Retail (6):** WMT, TGT, NKE, MCD, SBUX, COST

**Energy (3):** XOM, CVX, COP

**ETFs (5):** SPY, QQQ, DIA, IWM, VTI

---

## Project Structure

```
stock-market-pipeline/
├── ingestion/
│   └── ingest_stocks.py        # Local ingestion script (retired — see notebooks)
├── notebooks/
│   ├── project_setup.ipynb     # Unity Catalog infrastructure setup
│   ├── ingest_stocks.ipynb     # Databricks ingestion notebook
│   ├── bronze_to_silver.ipynb  # PySpark transformation with data quality checks
│   └── silver_to_gold.ipynb    # SQL gold layer — 6 business metric tables
├── requirements.txt            # Python dependencies
└── README.md
```

---

## Key Concepts Demonstrated

- **Medallion Architecture** — Bronze/Silver/Gold layered data design pattern
- **Delta Lake** — ACID transactions, schema enforcement, time travel
- **Unity Catalog** — Centralized governance with external locations and storage credentials
- **Window Functions** — Rolling averages, LAG for daily returns, STDDEV for volatility
- **Data Quality** — Deduplication, null validation, schema enforcement
- **Workflow Orchestration** — Automated weekly pipeline with task dependencies
- **Dual Language** — PySpark for complex transformations, Spark SQL for aggregations

---

## Setup

### Prerequisites
- Azure subscription with ADLS Gen2 storage account
- Azure Databricks workspace (Premium tier for Unity Catalog)
- Databricks Access Connector with Storage Blob Data Contributor role

### Infrastructure
Run `notebooks/project_setup.ipynb` to create:
- External Location
- Unity Catalog (`stock_pipeline`)
- Schemas (bronze, silver, gold)
- External Volume

### Running the Pipeline
1. Run `notebooks/ingest_stocks.ipynb` to pull fresh data
2. Run `notebooks/bronze_to_silver.ipynb` to transform to silver
3. Run `notebooks/silver_to_gold.ipynb` to build gold tables

Or trigger the `stock-market-pipeline-weekly` Databricks Workflow for automated execution.

---
## Screenshots

### Automated Pipeline — Databricks Workflow
All three pipeline stages completing successfully in 15 minutes with full data lineage tracked by Unity Catalog.

![Workflow Run](screenshots/Screenshot%202026-06-09%20131556.png)

### Unity Catalog — Data Architecture
Stock pipeline catalog with bronze, silver, and gold schemas managed by Unity Catalog.

![Catalog Structure](screenshots/Screenshot%202026-06-09%20131710.png)

### Gold Layer — All 6 Tables
Six business-ready Delta tables produced by the pipeline with row counts.

![Gold Tables Summary](screenshots/Screenshot%202026-06-09%20132522.png)

### Daily Prices with Return %
Clean daily OHLCV data enriched with daily percentage return for each ticker.

![Daily Prices](screenshots/Screenshot%202026-06-09%20133243.png)

### Moving Averages
7-day and 30-day rolling average closing prices for trend analysis.

![Moving Averages](screenshots/Screenshot%202026-06-09%20133143.png)

### Sector Summary
Daily aggregations by sector showing average price, returns, and total volume.

![Sector Summary](screenshots/Screenshot%202026-06-09%20133016.png)

### Volatility — 30-Day Risk Metric
Rolling standard deviation of daily returns measuring stock risk profiles.

![Volatility](screenshots/Screenshot%202026-06-09%20133028.png)

### 52-Week High/Low
Yearly price extremes with percentage distance from highs and lows.

![52-Week High/Low](screenshots/Screenshot%202026-06-09%20132915.png)

### Volume Spike Detection
Anomaly detection flagging abnormal trading activity — WMT trading 11.65x normal volume.

![Volume Spikes](screenshots/Screenshot%202026-06-09%20132624.png)


## Author
Anish Matta | [LinkedIn](https://www.linkedin.com/in/anish-matta/) | [GitHub](https://github.com/AnishM01)
