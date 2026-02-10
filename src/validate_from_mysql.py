from __future__ import annotations

import json
from datetime import datetime
from typing import List

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook


DT_COLS = ["departure_dt", "arrival_dt"]
NUM_COLS = ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt", "duration_hrs", "stopovers"]
CAT_COLS = ["airline", "source_code", "destination_code", "travel_class", "booking_source"]
VALID_AIRPORTS = {'BZL', 'CGP', 'CXB', 'DAC', 'JSR', 'RJH', 'SPD', 'ZYL'}


# Required columns for the validation contract (staging table naming)
REQUIRED_COLS = ["airline", "source_code", "destination_code", "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]


def _add_reason(mask: pd.Series, reason: str, reasons: pd.Series) -> pd.Series:
    reasons = reasons.copy()
    reasons.loc[mask] = reasons.loc[mask].apply(
        lambda x: reason if (pd.isna(x) or x == "") else f"{x}; {reason}"
    )
    return reasons

def parse_stopovers(series: pd.Series) -> pd.Series:
    """Convert stopover text to numbers"""
    s = series.astype(str).str.strip()
    s = s.replace({
        "Direct": "0",
        "Non-stop": "0", 
        "Nonstop": "0",
    })
    # Extract numbers from "1 Stop", "2 Stopovers", etc.
    s = s.str.extract(r'(\d+)', expand=False)
    return pd.to_numeric(s, errors="coerce")
def _get_table_columns(hook: MySqlHook, table: str) -> List[str]:
    rows = hook.get_records(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
        """,
        parameters=(table,),
    )
    return [r[0] for r in rows]


def _assert_required_columns_exist(hook: MySqlHook, table: str, required: List[str]) -> None:
    cols = set(_get_table_columns(hook, table))
    missing = [c for c in required if c not in cols]
    if missing:
        raise ValueError(f"[VALIDATION] Missing required columns in `{table}`: {missing}")


def _ensure_total_fare_mismatch_columns(hook: MySqlHook, table: str) -> None:
    """Add mismatch columns if missing (idempotent via information_schema, no row loops)."""
    cols = set(_get_table_columns(hook, table))

    alter_parts = []
    if "computed_total_fare_bdt" not in cols:
        alter_parts.append("ADD COLUMN computed_total_fare_bdt DECIMAL(12,2) NULL")
    if "fare_diff_bdt" not in cols:
        alter_parts.append("ADD COLUMN fare_diff_bdt DECIMAL(12,2) NULL")
    if "fare_mismatch_flag" not in cols:
        alter_parts.append("ADD COLUMN fare_mismatch_flag TINYINT(1) NOT NULL DEFAULT 0")

    if alter_parts:
        hook.run(f"ALTER TABLE {table} " + ", ".join(alter_parts) + ";")


def _compute_total_fare_mismatch_sql(
    hook: MySqlHook,
    table: str,
    tolerance: float = 1.0,
) -> None:
    """Set-based computation (no looping)"""
    tol = float(tolerance)
    hook.run(
        f"""
        UPDATE {table}
        SET
          computed_total_fare_bdt = ROUND(COALESCE(base_fare_bdt,0) + COALESCE(tax_surcharge_bdt,0), 2),
          fare_diff_bdt = ROUND(COALESCE(total_fare_bdt,0) - (COALESCE(base_fare_bdt,0) + COALESCE(tax_surcharge_bdt,0)), 2),
          fare_mismatch_flag = CASE
            WHEN total_fare_bdt IS NULL OR base_fare_bdt IS NULL OR tax_surcharge_bdt IS NULL THEN 1
            WHEN ABS(COALESCE(total_fare_bdt,0) - (COALESCE(base_fare_bdt,0) + COALESCE(tax_surcharge_bdt,0))) > {tol} THEN 1
            ELSE 0
          END;
        """
    )


def validate_staging_table(
    mysql_conn_id: str = "mysql_staging",
    source_table: str = "stg_flight_prices",
    good_table: str = "stg_flight_prices_valid",
    bad_table: str = "stg_flight_prices_invalid",
    metrics_out: str = "/opt/airflow/data/tmp/validation_metrics.json",
    total_fare_check: bool = True,
    total_fare_tolerance: float = 1.0,
) -> None:
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)

    # ---- (1) Schema check: required columns exist (fail fast)
    _assert_required_columns_exist(hook, source_table, REQUIRED_COLS)

    # ---- (2) Ensure mismatch columns exist + compute them in SQL 
    if total_fare_check:
        _ensure_total_fare_mismatch_columns(hook, source_table)
        _compute_total_fare_mismatch_sql(hook, source_table, tolerance=total_fare_tolerance)

    # ---- Extract
    df = hook.get_pandas_df(f"SELECT * FROM {source_table};")
    if "stopovers" in df.columns:  ## To convert stopovers to numeric values
        df["stopovers"] = parse_stopovers(df["stopovers"])

    if df.empty:
        metrics = {
            "rows_total": 0,
            "rows_good": 0,
            "rows_bad": 0,
            "bad_reason_counts": {},
            "generated_at_utc": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
            "source_table": source_table,
            "good_table": good_table,
            "bad_table": bad_table,
            "total_fare_check": bool(total_fare_check),
            "total_fare_tolerance": float(total_fare_tolerance),
        }
        with open(metrics_out, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        return

    reasons = pd.Series([""] * len(df), index=df.index)

    # Ensuring the numeric columns are actually numeric
    for c in NUM_COLS:
        if c in df.columns:
            # Try to coerce to numeric, mark failures as bad
            original = df[c].copy()
            df[c] = pd.to_numeric(df[c], errors='coerce')
            bad = df[c].isna() & original.notna()
            reasons = _add_reason(bad, f"non_numeric_value:{c}", reasons)

    # ---- Rule 1: required categorical not empty/blank
    for c in CAT_COLS:
        if c in df.columns:
            bad = df[c].isna() | (df[c].astype(str).str.strip() == "")
            reasons = _add_reason(bad, f"empty_or_null:{c}", reasons)

    # ---- Rule 2: numeric must not be null 
    for c in NUM_COLS:
        if c in df.columns:
            bad = df[c].isna()
            reasons = _add_reason(bad, f"invalid_numeric:{c}", reasons)

    # ---- Rule 3: no negative fares
    for c in ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]:
        if c in df.columns:
            bad = df[c].notna() & (df[c] < 0)
            reasons = _add_reason(bad, f"negative_value:{c}", reasons)

    # ---- Rule 4: datetime must exist and be valid
    for c in DT_COLS:
        if c in df.columns:
            bad = df[c].isna()
            reasons = _add_reason(bad, f"invalid_datetime:{c}", reasons)
    # --- invalid source
    bad = ~df['source_code'].isin(VALID_AIRPORTS)
    reasons = _add_reason(bad, f"invalid_airport_code:source", reasons)

    # ---- Rule 5 total fare mismatch flag
    if total_fare_check and "fare_mismatch_flag" in df.columns:
        mismatch = df["fare_mismatch_flag"].fillna(0).astype(int) == 1
        reasons = _add_reason(mismatch, "total_fare_mismatch", reasons)

    # ---- Split
    is_bad = reasons.astype(str).str.strip() != ""
    bad_df = df.loc[is_bad].copy()
    good_df = df.loc[~is_bad].copy()

    if not bad_df.empty:
        bad_df["__reasons"] = reasons.loc[is_bad].values

    # ---- Load: truncate destination tables then insert
    hook.run(f"TRUNCATE TABLE {good_table};")
    hook.run(f"TRUNCATE TABLE {bad_table};")

    #convert NaN -> None properly for MySQL inserts
    good_df = good_df.astype(object).where(pd.notnull(good_df), None)
    bad_df = bad_df.astype(object).where(pd.notnull(bad_df), None)

    if not good_df.empty:
        hook.insert_rows(
            table=good_table,
            rows=good_df.itertuples(index=False, name=None),
            target_fields=list(good_df.columns),
            commit_every=1000,
        )

    if not bad_df.empty:
        hook.insert_rows(
            table=bad_table,
            rows=bad_df.itertuples(index=False, name=None),
            target_fields=list(bad_df.columns),
            commit_every=1000,
        )

    # ---- Metrics
    metrics = {
        "rows_total": int(len(df)),
        "rows_good": int(len(good_df)),
        "rows_bad": int(len(bad_df)),
        "bad_reason_counts": reasons.loc[is_bad].value_counts().to_dict(),
        "generated_at_utc": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
        "source_table": source_table,
        "good_table": good_table,
        "bad_table": bad_table,
        "total_fare_check": bool(total_fare_check),
        "total_fare_tolerance": float(total_fare_tolerance),
    }

    with open(metrics_out, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
