from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
}


with DAG(
    dag_id="flight_price_pipeline",
    default_args=DEFAULT_ARGS,
    description="Bangladesh flight price pipeline: validate -> stage -> kpis -> analytics",
    start_date=datetime(2026, 2, 1),
    schedule=None, 
    catchup=False,
    tags=["lab", "flight_prices"],
) as dag:

    validate_csv = BashOperator(
        task_id="validate_csv",
        bash_command=(
            "mkdir -p /opt/airflow/data/tmp /opt/airflow/data/bad_records && "
            "python /opt/airflow/src/validate_flights.py "
            "--input /opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv "
            "--contract /opt/airflow/src/contracts/flight_prices_contract.json "
            "--good_out /opt/airflow/data/tmp/validated_flights.csv "
            "--bad_out /opt/airflow/data/bad_records/bad_flights.csv "
            "--metrics_out /opt/airflow/data/tmp/metrics.json"
        ),
    )
