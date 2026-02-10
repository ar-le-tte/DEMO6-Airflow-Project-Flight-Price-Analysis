# Flight Price Analysis Pipeline (Bangladesh)

A complete data engineering pipeline for processing (ingesting, validating, transforming, and analyzing) Bangladesh flight pricing data using `Apache Airflow`, `MySQL`, and `PostgreSQL`.

## Architecture Overview
```scss
Raw CSV
  ↓
Raw Validation  
  ↓
MySQL (Staging + Validation & Transformation)
  ├─ stg_flight_prices
  ├─ stg_flight_prices_valid
  └─ stg_flight_prices_invalid
        ↓
PostgreSQL (Analytics)
  ├─ analytics.flight_prices_valid
  ├─ analytics.flight_prices_invalid
  └─ analytics.kpi_*

```

### Project Structure
```text
.
├── dags/
│   └── flight_price_pipeline_dag.py      # Main Airflow DAG
│
├── src/
│   ├── contracts/
│   │   └── flight_prices_contract.json   # Validation contract (schema + rules)
│   ├── ingest_to_mysql.py                # CSV → MySQL staging
│   ├── raw_flights_validation.py         # Raw CSV validation
│   ├── validate_from_mysql.py             # Validation from MySQL staging
│   ├── load_mysql_to_postgres.py          # Load valid/invalid to Postgres
│   └── create_kpis.py                     # KPI population logic
│
├── sql/
│   ├── mysql_staging_ddl.sql              # MySQL staging schema
│   ├── mysql_validation_tables_ddl.sql    # MySQL valid/invalid tables
│   └── postgres_kpi_ddl.sql               # Postgres KPI tables
│
├── data/                                  # These are local (to store data)
│   ├── raw/
│   ├── tmp/
│   └── bad_records/
│
├── docs/                                 # Report & documentation 
│
├── logs/                                 # Airflow logs (generated)
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env                                  # Ignored (credentials and connection details)
├── .gitignore
└── README.md

```
## Dataset
This project uses the following data sourced from kaggle:
[Flight Price Dataset of Bangladesh](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh)

The dataset is to go in the `data/` directory under `raw/` locally.

## Prerequisites
- Docker Desktop (or Docker Engine + Docker Compose)
- WSL2 (if on Windows)
- Git

 