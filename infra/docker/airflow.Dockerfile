# Airflow 2.9 + providers dùng trong video #19–#21.
FROM apache/airflow:2.9.3-python3.11

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-postgres==5.11.2" \
    "apache-airflow-providers-amazon==8.26.0" \
    "apache-airflow-providers-apache-spark==4.9.0" \
    "apache-airflow-providers-openlineage==1.9.1" \
    "dbt-core>=1.8,<1.10" "dbt-duckdb>=1.8,<1.10" "dbt-postgres>=1.8,<1.10" \
    "great-expectations>=1.1,<2" \
    "duckdb>=1.1" "polars>=1.10" "pyarrow>=17" "boto3>=1.35" "s3fs>=2024.9" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" || \
    pip install --no-cache-dir \
    "apache-airflow-providers-postgres" "apache-airflow-providers-amazon" \
    "apache-airflow-providers-apache-spark" "apache-airflow-providers-openlineage" \
    "dbt-core>=1.8,<1.10" "dbt-duckdb>=1.8,<1.10" "dbt-postgres>=1.8,<1.10" \
    "great-expectations>=1.1,<2" "duckdb>=1.1" "polars>=1.10" "pyarrow>=17" "boto3>=1.35" "s3fs>=2024.9"
