FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-mysql==5.6.1 \
    apache-airflow-providers-postgres==5.14.0 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9 \
    mysql-connector-python==9.1.0
