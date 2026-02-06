#!/usr/bin/env python3

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd


def load_contract(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dirs(*paths: str) -> None:
    for p in paths:
        Path(p).parent.mkdir(parents=True, exist_ok=True)


def now_stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def coerce_numeric(series: pd.Series) -> pd.Series:
    """
    Convert to numeric safely:
    - strips commas
    - coerces invalid values to NaN
    """
    s = series.astype(str).str.replace(",", "", regex=False).str.strip()
    # Treat common empties as NA
    s = s.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "NaN": pd.NA, "N/A": pd.NA})
    return pd.to_numeric(s, errors="coerce")


def parse_datetime(series: pd.Series) -> pd.Series:
    """
    Parse format: YYYY-MM-DD HH:MM:SS (e.g., 2025-11-17 06:25:00)
    Coerce bad values to NaT.
    """
    s = series.astype(str).str.strip()
    s = s.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "NaN": pd.NA, "N/A": pd.NA})
    return pd.to_datetime(s, errors="coerce")


def parse_stopovers(series: pd.Series) -> pd.Series:
    """
    Convert stopovers text to numbers:
    - "Direct" -> 0
    - "1 Stop" -> 1
    - "2 Stops" -> 2
    - etc.
    """
    s = series.astype(str).str.strip()
    
    # Map common patterns
    s = s.replace({
        "Direct": "0",
        "Non-stop": "0",
        "Nonstop": "0",
    })
    
    # Extract numbers from "1 Stop", "2 Stops", etc.
    s = s.str.extract(r'(\d+)', expand=False)
    
    # Convert to numeric
    return pd.to_numeric(s, errors="coerce")


def validate_required_columns(df: pd.DataFrame, required: List[str]) -> Tuple[bool, List[str]]:
    missing = [c for c in required if c not in df.columns]
    return (len(missing) == 0, missing)


def add_reason(bad_mask: pd.Series, reason: str, reasons: pd.Series) -> pd.Series:
    """
    Append a reason string to rows flagged as bad.
    """
    reasons = reasons.copy()
    reasons.loc[bad_mask] = reasons.loc[bad_mask].apply(
        lambda x: reason if (pd.isna(x) or x == "") else f"{x}; {reason}"
    )
    return reasons


def compute_metrics(df: pd.DataFrame, bad_df: pd.DataFrame, reasons: pd.Series) -> Dict:
    metrics = {
        "rows_total": int(len(df)),
        "rows_good": int(len(df) - len(bad_df)),
        "rows_bad": int(len(bad_df)),
        "bad_reason_counts": reasons.dropna().value_counts().to_dict(),
        "null_rates": {c: float(df[c].isna().mean()) for c in df.columns},
    }
    return metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to raw CSV")
    ap.add_argument("--contract", required=True, help="Path to JSON contract")
    ap.add_argument("--good_out", required=True, help="Output CSV for validated good records")
    ap.add_argument("--bad_out", required=True, help="Output CSV for quarantined bad records")
    ap.add_argument("--metrics_out", required=True, help="Output JSON metrics")
    args = ap.parse_args()

    contract = load_contract(args.contract)

    ensure_dirs(args.good_out, args.bad_out, args.metrics_out)

    # ---- Read raw
    df = pd.read_csv(args.input)

    # ---- Schema checks
    ok, missing = validate_required_columns(df, contract["required_columns"])
    if not ok:
        raise ValueError(f"Missing required columns: {missing}")

    # ---- Start bad reasons tracking
    reasons = pd.Series([""] * len(df))

    # ---- Normalize / coerce types
    # Handle Stopovers specially (convert text to numbers)
    if "Stopovers" in df.columns:
        df["Stopovers"] = parse_stopovers(df["Stopovers"])
    
    # Other numerics
    for col in contract.get("numeric_columns", []):
        if col != "Stopovers":  # Already handled
            df[col] = coerce_numeric(df[col])

    # Datetimes
    dt_cols = ["Departure Date & Time", "Arrival Date & Time"]
    for c in dt_cols:
        if c in df.columns:
            df[c] = parse_datetime(df[c])

    # ---- Rule checks
    # 1) Non-empty strings for key fields (excluding datetime columns)
    for col in contract.get("non_empty_string_columns", []):
        if col in dt_cols:
            continue
        bad = df[col].isna() | (df[col].astype(str).str.strip() == "")
        reasons = add_reason(bad, f"empty_or_null:{col}", reasons)

    # 2) Numeric columns must be valid numbers (not NaN)
    for col in contract.get("numeric_columns", []):
        bad = df[col].isna()
        reasons = add_reason(bad, f"invalid_numeric:{col}", reasons)

    # 3) No negative values
    for col in contract.get("rules", {}).get("no_negative_values", []):
        bad = df[col].notna() & (df[col] < 0)
        reasons = add_reason(bad, f"negative_value:{col}", reasons)

    # 4) Total Fare consistency / recompute
    rules = contract.get("rules", {})
    if rules.get("compute_total_fare_if_missing", False):
        tol = float(rules.get("total_fare_tolerance_bdt", 1.0))
        base = df["Base Fare (BDT)"]
        tax = df["Tax & Surcharge (BDT)"]
        total = df["Total Fare (BDT)"]

        computed = base + tax

        # If total is missing but base & tax exist => fill
        can_fill = total.isna() & base.notna() & tax.notna()
        df.loc[can_fill, "Total Fare (BDT)"] = computed.loc[can_fill]

        # If total exists but differs too much => flag (don't auto-fix silently)
        mismatch = (
            df["Total Fare (BDT)"].notna()
            & computed.notna()
            & ((df["Total Fare (BDT)"] - computed).abs() > tol)
        )
        reasons = add_reason(mismatch, "total_fare_mismatch", reasons)

    # 5) Datetime parse must succeed
    for c in dt_cols:
        if c in df.columns:
            bad = df[c].isna()
            reasons = add_reason(bad, f"invalid_datetime:{c}", reasons)

    # ---- Split good vs bad
    is_bad = reasons.astype(str).str.strip() != ""
    bad_df = df.loc[is_bad].copy()
    good_df = df.loc[~is_bad].copy()

    # Attach reasons to bad output
    if len(bad_df) > 0:
        bad_df["__reasons"] = reasons.loc[is_bad].values

    # ---- Write outputs
    good_df.to_csv(args.good_out, index=False)
    bad_df.to_csv(args.bad_out, index=False)

    metrics = compute_metrics(df, bad_df, reasons.loc[is_bad])
    metrics.update(
        {
            "input": args.input,
            "contract": args.contract,
            "good_out": args.good_out,
            "bad_out": args.bad_out,
            "generated_at_utc": now_stamp(),
        }
    )

    with open(args.metrics_out, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print(
        f"OK | total={metrics['rows_total']} good={metrics['rows_good']} bad={metrics['rows_bad']} "
        f"| metrics={args.metrics_out}"
    )


if __name__ == "__main__":
    main()