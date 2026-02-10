# Flight Price Data Pipeline: Technical Report
## 1. Overview

This project implements an end-to-end data engineering pipeline for analyzing flight prices in Bangladesh using Apache Airflow.  
The pipeline ingests raw CSV data, validates and cleans it in MySQL, transfers curated datasets to PostgreSQL, and computes analytical KPIs for downstream analysis.

The focus of the implementation is **data correctness, reproducibility, and operational clarity**, rather than ad-hoc analysis.
---
## 2. Pipeline Architecture

### High-Level Architecture
The pipeline follows a medallion-inspired architecture with distinct staging and analytics layers:

```scss
Raw CSV Files → [Validation Layer]
    ↓
MySQL Staging Database
    ↓
[Quality Checks & Splitting]
    ↓
PostgreSQL Analytics Database
    ↓
[KPI Computation & Analysis]
```

### Key Design Choices

- **MySQL** is used for ingestion and validation because it is lightweight and well-suited for row-level data quality checks.
- **PostgreSQL** is used for analytics due to stronger support for analytical queries and schema organization.
- **Airflow** orchestrates every step to ensure deterministic execution and traceability.

---

## 3. Airflow DAG and Execution Flow

The pipeline is implemented as a single DAG with clearly ordered tasks to prevent partial or inconsistent runs.

### Execution Flow

The execution follows 9 tasks:

```text
validate_csv
→ create_mysql_staging_table
→ ingest_to_mysql
→ create_validation_tables
→ validate_from_mysql
→ pg_create_tables
→ mysql_to_pg
→ create_kpis_tables
→ populate_kpis
```

Each task only executes once its upstream dependency has completed successfully.

---

## 4. DAG Tasks Description

### 4.1 CSV Validation

**Task:** `validate_csv`

- Performs a lightweight structural check on the raw CSV.
- Ensures required columns exist before ingestion.
- Prevents malformed input from entering the pipeline.



### 4.2 MySQL Staging Table Creation

**Task:** `create_mysql_staging_table`

- Creates the staging table used for raw ingestion.
- Schema mirrors the CSV structure closely.
- No transformations are applied at this stage.



### 4.3 Raw Ingestion into MySQL

**Task:** `ingest_to_mysql`

- Loads the raw CSV into MySQL .
- Explicitly converts Pandas `NaN` values to `NULL` using:
  ```python
  df = df.astype(object).where(pd.notnull(df), None)
  ``` 
This step resolved earlier failures caused by silent NULL handling issues.
### 4.4 Validation Table Setup

**Task:** `create_validation_tables`

This task initializes the tables used for data quality separation in MySQL:

- **`stg_flight_prices_valid`**  
  Stores records that pass all validation checks and are considered trustworthy for analytics.

- **`stg_flight_prices_invalid`**  
  Stores records that fail one or more validation rules.

Both tables share an identical structural schema to ensure consistency.  
The invalid table includes an additional `__reasons` column, which captures detailed explanations for why a record was rejected during validation.



### 4.5 Data Validation in MySQL

**Task:** `validate_from_mysql`

This task applies row-level data quality rules to the ingested staging data and routes records into VALID or INVALID tables accordingly.

The validation logic enforces the following constraints:

- **Mandatory categorical fields** must be non-null and non-empty.
- **Numeric fields** must contain valid values and cannot be negative.
- **Datetime fields** must be present and successfully parsed.
- **Fare consistency** is checked by recomputing the total fare as the sum of base fare and tax/surcharge values.
```ini
computed_total_fare = base_fare_bdt + tax_surcharge_bdt
```

Records that violate any of these rules are flagged and preserved in the invalid table, ensuring transparency and auditability of data quality issues.

**Outcome**

- The VALID table contains fewer records than the original 57,000-row dataset. it has 54478 rows, because the other 2522 did not pass the total fare mismatch rule" 
- All rejected records are retained in the INVALID table with clear, human-readable explanations for each validation failure.



### 4.6 PostgreSQL Table Initialization

**Task:** `pg_create_tables`

This task prepares the analytics layer by creating PostgreSQL tables that reflect the structure of the MySQL VALID and INVALID staging tables.

Key characteristics include:

- Schema alignment between MySQL and PostgreSQL to preserve consistency.
- Explicit primary key constraints to ensure row-level uniqueness and traceability.



### 4.7 Data Transfer from MySQL to PostgreSQL

**Task:** `mysql_to_pg`

This task moves validated data from the MySQL staging layer into PostgreSQL for analytical processing.

Key implementation details:

- Both VALID and INVALID datasets are transferred.
- Batch inserts are performed using `psycopg2.execute_values` for efficiency.
- Destination tables are truncated before each load to guarantee idempotent execution.

This approach ensures:

- No duplicate records across runs.
- Low-latency bulk data transfer.
- Strong data consistency between source and destination systems.


### 4.8 KPI Table Definition

**Task:** `create_kpis_tables`

This task creates the KPI tables in PostgreSQL ahead of computation.

Design considerations:

- Primary keys are defined to prevent duplicate KPI entries.
- Table creation is intentionally separated from KPI population to improve maintainability and clarity of the pipeline.


### 4.9 KPI Computation and Population

**Task:** `populate_kpis`

This task computes and populates all KPI tables using data exclusively from the `analytics.flight_prices_valid` table.

By restricting KPI computation to validated records only, the pipeline guarantees that analytical results are derived from high-quality, trusted data.

---

## 5. KPI Definitions and Computation Logic

### 5.1 Average Fare by Airline

**Definition**

The mean total fare calculated for each airline across all validated bookings.

**Computation Logic**
```sql
AVG(total_fare_bdt)
```
- Group records by airline.
- Compute the average of `total_fare_bdt`.
- Store the result alongside the total booking count per airline.