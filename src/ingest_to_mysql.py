#!/usr/bin/env python3
import math
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook


def ingest_csv_to_mysql(
    csv_path: str,
    mysql_conn_id: str = "mysql_staging",
    table: str = "stg_flight_prices",
    chunk_size: int = 5000,
) -> None:
    """
    Ingest validated CSV into MySQL staging table (idempotent).
    - Uses Airflow connection (mysql_conn_id)
    - Truncates staging table before load
    - Inserts in chunks
    - Reconciles row count
    """

    # 1) Read
    df = pd.read_csv(csv_path)

    # 2) Rename to clean column names (match your MySQL staging DDL)
    df = df.rename(columns={
        "Airline": "airline",
        "Source": "source_code",
        "Source Name": "source_name",
        "Destination": "destination_code",
        "Destination Name": "destination_name",
        "Departure Date & Time": "departure_dt",
        "Arrival Date & Time": "arrival_dt",
        "Duration (hrs)": "duration_hrs",
        "Stopovers": "stopovers",
        "Aircraft Type": "aircraft_type",
        "Class": "travel_class",
        "Booking Source": "booking_source",
        "Base Fare (BDT)": "base_fare_bdt",
        "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
        "Total Fare (BDT)": "total_fare_bdt",
        "Seasonality": "seasonality",
        "Days Before Departure": "days_before_departure",
    })

    # 3) Parse datetimes (match your validator’s output format; errors -> NaT)
    for col in ["departure_dt", "arrival_dt"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # 4) Numerics coercion
    numeric_cols = ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt", "duration_hrs"]
    for col in numeric_cols:
        df[col] = (
            df[col].astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
            .replace({"": None, "nan": None, "NaN": None, "None": None})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5) days_before_departure as int (keep nulls as 0 for now)
    df["days_before_departure"] = pd.to_numeric(df["days_before_departure"], errors="coerce").fillna(0).astype(int)

    # 6) Ensure stopovers is int/nullable if present (if it's text, leave it as-is and MySQL may reject)
    # If your validator already converted it to numbers, this will work:
    if "stopovers" in df.columns:
        df["stopovers"] = pd.to_numeric(df["stopovers"], errors="coerce")

    # Columns to load (must match MySQL DDL)
    load_cols = [
        "airline", "source_code", "source_name",
        "destination_code", "destination_name",
        "departure_dt", "arrival_dt",
        "duration_hrs", "stopovers",
        "aircraft_type", "travel_class", "booking_source",
        "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt",
        "seasonality", "days_before_departure",
    ]

    df = df[load_cols]

    # Convert pandas NaN/NaT -> Python None for DB insertion
    df = df.astype(object).where(pd.notnull(df), None)

    hook = MySqlHook(mysql_conn_id=mysql_conn_id)

    # 7) Truncating staging table
    hook.run(f"TRUNCATE TABLE {table};")

    # 8) Insert in chunks
    placeholders = ",".join(["%s"] * len(load_cols))
    insert_sql = f"""
        INSERT INTO {table} ({",".join(load_cols)})
        VALUES ({placeholders})
    """

    rows = list(df.itertuples(index=False, name=None))
    total_rows = len(rows)

    if total_rows == 0:
        raise ValueError("No rows to ingest (CSV is empty).")

    num_chunks = math.ceil(total_rows / chunk_size)

    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        for i in range(num_chunks):
            start = i * chunk_size
            end = min((i + 1) * chunk_size, total_rows)
            cur.executemany(insert_sql, rows[start:end])
            conn.commit()

        # 9) Row-count reconciliation
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        db_count = cur.fetchone()[0]

        if db_count != total_rows:
            raise RuntimeError(f"Row count mismatch: csv={total_rows} mysql={db_count}")

        print(f"OK | ingested_rows={db_count} into {table}")

    finally:
        cur.close()
        conn.close()
