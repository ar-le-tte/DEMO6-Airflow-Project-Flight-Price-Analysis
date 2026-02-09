from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from src.ingest_to_mysql import ingest_csv_to_mysql


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
            "python /opt/airflow/src/raw_flights_validation.py "
            "--input /opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv "
            "--contract /opt/airflow/src/contracts/flight_prices_contract.json "
            "--good_out /opt/airflow/data/tmp/validated_flights.csv "
            "--bad_out /opt/airflow/data/bad_records/bad_flights.csv "
            "--metrics_out /opt/airflow/data/tmp/metrics.json"
        ),
    )
    create_mysql_staging_table = BashOperator(
        task_id="create_mysql_staging_table",
            bash_command=(
                "mysql "
                "-h mysql "
                "-u root "
                "-proot "
                "< /opt/airflow/sql/mysql_staging_ddl.sql"
            ),
        )
    ingest_to_mysql = PythonOperator(
    task_id="ingest_to_mysql",
    python_callable=ingest_csv_to_mysql,
    op_kwargs={
        "csv_path": "/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv",
        "mysql_conn_id": "mysql_staging",
        "table": "stg_flight_prices",
        "chunk_size": 5000,
    },
)

    validate_csv >> create_mysql_staging_table >> ingest_to_mysql


