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

## Prerequisites
- Docker Desktop (or Docker Engine + Docker Compose)
- WSL2 (if on Windows)
- Git

 ## Set Up Instructions

 ### 1. Clone the Repo

 ```bash
 git clone https://github.com/ar-le-tte/DEMO6-Airflow-Project-Flight-Price-Analysis.git

cd DEMO6-Airflow-Project-Flight-Price-Analysis

 ```

 ### 2. Create Required Directories

 For the raw, and transformed data as well as airflow logs

 ```bash
 mkdir -p data/raw data/tmp data/bad_records logs dags sql src/contracts

 ```

 ### 3. Configure Environment Variables

 Create a `.env` in the project root with all the necessary credentials and connection details for `Airflow`, `MySQL`, `PostgreSQL`

 ### 4. Dataset
This project uses the following data sourced from kaggle:
[Flight Price Dataset of Bangladesh](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh)

The dataset is to go in the `data/` directory under `raw/` locally.

### 5. Docker Services
```bash
# Start all services
docker compose up -d

# Verify all containers are running
docker compose ps
```
### 6. Access Airflow UI
Open your browser to: [Airflow UI](http://localhost:8080)

**Login credentials:**

- Username: `flight`
- Password: `flight`

**Trigger the `flight_price_pipeline` DAG** 