from __future__ import annotations

import os
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values


# ---------- connections ----------
def mysql_conn():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
    )


def pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )


# ---------- Postgres DDL (mirrors your MySQL VALID + INVALID) ----------
DDL = """
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.flight_prices_valid (
    id                    INT PRIMARY KEY,
    airline               VARCHAR(100),
    source_code           VARCHAR(10),
    source_name           VARCHAR(255),
    destination_code      VARCHAR(10),
    destination_name      VARCHAR(255),
    departure_dt          TIMESTAMP,
    arrival_dt            TIMESTAMP,
    duration_hrs          NUMERIC(5,2),
    stopovers             INT NULL,
    aircraft_type         VARCHAR(100),
    travel_class          VARCHAR(50),
    booking_source        VARCHAR(100),
    base_fare_bdt         NUMERIC(12,2),
    tax_surcharge_bdt     NUMERIC(12,2),
    total_fare_bdt        NUMERIC(12,2),
    seasonality           VARCHAR(50),
    days_before_departure INT,
    ingested_at_utc       TIMESTAMP,

    computed_total_fare_bdt NUMERIC(12,2) NULL,
    fare_diff_bdt           NUMERIC(12,2) NULL,
    fare_mismatch_flag      SMALLINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS analytics.flight_prices_invalid (
    id                    INT PRIMARY KEY,
    airline               VARCHAR(100),
    source_code           VARCHAR(10),
    source_name           VARCHAR(255),
    destination_code      VARCHAR(10),
    destination_name      VARCHAR(255),
    departure_dt          TIMESTAMP,
    arrival_dt            TIMESTAMP,
    duration_hrs          NUMERIC(5,2),
    stopovers             INT NULL,
    aircraft_type         VARCHAR(100),
    travel_class          VARCHAR(50),
    booking_source        VARCHAR(100),
    base_fare_bdt         NUMERIC(12,2),
    tax_surcharge_bdt     NUMERIC(12,2),
    total_fare_bdt        NUMERIC(12,2),
    seasonality           VARCHAR(50),
    days_before_departure INT,
    ingested_at_utc       TIMESTAMP,

    computed_total_fare_bdt NUMERIC(12,2) NULL,
    fare_diff_bdt           NUMERIC(12,2) NULL,
    fare_mismatch_flag      SMALLINT NOT NULL DEFAULT 0,

    __reasons               TEXT
);
"""


def create_target_tables():
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()
    cur.close()
    conn.close()
    print("Tables created.")


# ---------- SQL extracts ----------
SRC_VALID_SQL = """
SELECT
  id, airline, source_code, source_name, destination_code, destination_name,
  departure_dt, arrival_dt, duration_hrs, stopovers,
  aircraft_type, travel_class, booking_source,
  base_fare_bdt, tax_surcharge_bdt, total_fare_bdt,
  seasonality, days_before_departure, ingested_at_utc,
  computed_total_fare_bdt, fare_diff_bdt, fare_mismatch_flag
FROM stg_flight_prices_valid
ORDER BY id
"""

SRC_INVALID_SQL = """
SELECT
  id, airline, source_code, source_name, destination_code, destination_name,
  departure_dt, arrival_dt, duration_hrs, stopovers,
  aircraft_type, travel_class, booking_source,
  base_fare_bdt, tax_surcharge_bdt, total_fare_bdt,
  seasonality, days_before_departure, ingested_at_utc,
  computed_total_fare_bdt, fare_diff_bdt, fare_mismatch_flag,
  __reasons
FROM stg_flight_prices_invalid
ORDER BY id
"""


def _copy_table(mysql_sql: str, pg_table: str, pg_columns: list[str], batch_size: int = 50000) -> int:
    mconn = mysql_conn()
    mcur = mconn.cursor()
    mcur.execute(mysql_sql)

    pconn = pg_conn()
    pcur = pconn.cursor()

    # replace on each run
    pcur.execute(f"TRUNCATE TABLE {pg_table};")
    pconn.commit()

    insert_sql = f"""
    INSERT INTO {pg_table} ({", ".join(pg_columns)})
    VALUES %s
    """

    total = 0
    while True:
        rows = mcur.fetchmany(batch_size)
        if not rows:
            break
        execute_values(pcur, insert_sql, rows, page_size=min(batch_size, 10000))
        pconn.commit()
        total += len(rows)
        print(f"[{pg_table}] committed total rows: {total}")

    mcur.close()
    mconn.close()
    pcur.close()
    pconn.close()
    return total


def copy_valid_and_invalid():
    valid_cols = [
        "id", "airline", "source_code", "source_name", "destination_code", "destination_name",
        "departure_dt", "arrival_dt", "duration_hrs", "stopovers",
        "aircraft_type", "travel_class", "booking_source",
        "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt",
        "seasonality", "days_before_departure", "ingested_at_utc",
        "computed_total_fare_bdt", "fare_diff_bdt", "fare_mismatch_flag",
    ]

    invalid_cols = valid_cols + ["__reasons"]

    n_valid = _copy_table(SRC_VALID_SQL, "analytics.flight_prices_valid", valid_cols)
    n_invalid = _copy_table(SRC_INVALID_SQL, "analytics.flight_prices_invalid", invalid_cols)

    print(f"Done. Postgres rows: valid={n_valid}, invalid={n_invalid}")


if __name__ == "__main__":
    create_target_tables()
    copy_valid_and_invalid()
