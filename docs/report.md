# Flight Price Data Pipeline: Technical Report
## 1. Overview

This project implements an end-to-end data engineering pipeline for analyzing flight prices in Bangladesh using Apache Airflow.  
The pipeline ingests raw CSV data, validates and cleans it in MySQL, transfers curated datasets to PostgreSQL, and computes analytical KPIs for downstream analysis.

The focus of the implementation is **data correctness, reproducibility, and operational clarity**, rather than ad-hoc analysis.

## 2. Pipeline Architecture

### High-Level Architecture
The pipeline follows a medallion-inspired architecture with distinct staging and analytics layers:

```scss
Raw CSV Files -> [Validation Layer]
    ↓
MySQL Staging Database
    ↓
[Quality Checks & Splitting]
    ↓
PostgreSQL Analytics Database
    ↓
[KPI Computation & Analysis]
```
