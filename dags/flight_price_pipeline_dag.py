from __future__ import annotations
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from src.ingest_to_mysql import ingest_csv_to_mysql
from src.validate_from_mysql import validate_staging_table
from src.load_mysql_to_postgres import create_target_tables, copy_valid_and_invalid
from src.create_kpis import (create_kpi_avg_fare, create_kpi_seasonal, create_kpi_booking_count, create_kpi_top_routes)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "email": [os.getenv("ALERT_EMAIL")],
    "email_on_failure": True,          
    "email_on_retry": False,          
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
    create_validation_tables = BashOperator(
        task_id="create_validation_tables",
        bash_command=(
            "mysql "
            "-h mysql "
            "-u root "
            "-proot "
            "< /opt/airflow/sql/mysql_validation_tables_ddl.sql"
        ),
    )
    validate_from_mysql = PythonOperator(
        task_id="validate_from_mysql",
        python_callable=validate_staging_table,
        op_kwargs={
            "mysql_conn_id": "mysql_staging",
            "source_table": "stg_flight_prices",
            "good_table": "stg_flight_prices_valid",
            "bad_table": "stg_flight_prices_invalid",
            "metrics_out": "/opt/airflow/data/tmp/validation_metrics.json",
            "total_fare_check": True,
            "total_fare_tolerance": 1.0,
        },
    )
    pg_create_tables = PythonOperator(
        task_id="pg_create_tables",
        python_callable=create_target_tables,
    )

    mysql_to_pg = PythonOperator(
        task_id="mysql_to_pg_valid_invalid",
        python_callable=copy_valid_and_invalid,
    )
    create_kpis_tables = BashOperator(
        task_id="create_kpi_tables",
         bash_command=(
            "psql "
            "-h postgres "
            "-U ${POSTGRES_USER} "
            "-d ${POSTGRES_DB} "
            "-f /opt/airflow/sql/postgres_kpi_ddl.sql"
        ),
        env={
            "PGPASSWORD": os.getenv("POSTGRES_PASSWORD"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        },
 
    )
    calc_kpi_avg_fare = PythonOperator(
        task_id="calc_kpi_avg_fare",
        python_callable=create_kpi_avg_fare,
    )
    
    calc_kpi_seasonal = PythonOperator(
        task_id="calc_kpi_seasonal",
        python_callable=create_kpi_seasonal,
    )
    
    calc_kpi_booking_count = PythonOperator(
        task_id="calc_kpi_booking_count",
        python_callable=create_kpi_booking_count,
    )
    
    calc_kpi_top_routes = PythonOperator(
        task_id="calc_kpi_top_routes",
        python_callable=create_kpi_top_routes,
    )

    # Task grouping:

    ingestion = (
    validate_csv
    >> create_mysql_staging_table
    >> ingest_to_mysql
)

    validation = (
        create_validation_tables
        >> validate_from_mysql
    )

    warehouse_load = (
        pg_create_tables
        >> mysql_to_pg
    )

    kpis = (
        create_kpis_tables
        >> [calc_kpi_avg_fare, calc_kpi_seasonal, calc_kpi_booking_count, calc_kpi_top_routes]
    )

    ingestion >> validation >> warehouse_load >> kpis





