# Test Cases 


## Test Case 1: MySQL Staging Ingestion

**Description:**  
Verifyinging that the raw CSV dataset is fully ingested into the `MySQL` staging table.


```sql
SELECT COUNT(*) FROM stg_flight_prices;
```
We injested 57,000 rows which match waht our raw data has.

## Test Case 2: Required Column Presence

```sql
DESCRIBE stg_flight_prices;
```

**Outcome:** Columns for airline, route codes, fares, dates, and metadata exist.

## Test Case 3: Validation Split (Valid vs Invalid Records)

Verifying that records are correctly split into VALID and INVALID tables during validation using rules like total fare mismatch.

```sql
SELECT COUNT(*) FROM stg_flight_prices;
SELECT COUNT(*) FROM stg_flight_prices_valid;
SELECT COUNT(*) FROM stg_flight_prices_invalid;
```

**Expected Outcome:**

- valid + invalid = staging
- Invalid records contain validation reasons.

**Actual Outcome:**

- Staging rows correctly split
- Invalid records populated with __reasons

## Test Case 4: Load VALID & INVALID Records into PostgreSQL

Verifying that validated data is transferred from `MySQL` to `PostgreSQL` analytics schema.

In `PostgrSQL`:

```sql
SELECT COUNT(*) FROM analytics.flight_prices_valid;
SELECT COUNT(*) FROM analytics.flight_prices_invalid;
```
**Expected Outcome:**

- PostgreSQL row counts match MySQL VALID and INVALID tables.
- VALID row count is less than 57,000 due to validation filtering.

**Actual Outcome:**

- Row counts match MySQL source tables exactly.

## Test Case 5: Data Consistency After Transfer
Ensure no unintended data mutation during MySQL → PostgreSQL transfer.

```sql
SELECT
  MIN(total_fare_bdt),
  MAX(total_fare_bdt)
FROM analytics.flight_prices_valid;
```
**Expected Outcome:**

- Values consistent with MySQL source.

**Actual Outcome:**

- Values preserved accurately.

## Test Case 6: Airflow DAG Execution
Verifying successful execution of all DAG tasks.

**Steps:**

- Open Airflow UI.
- Inspect DAG Graph view.

**Expected Outcome:**

- All tasks complete successfully.
- No retries or failures.

**Actual Outcome:**

- DAG executed end-to-end without errors.