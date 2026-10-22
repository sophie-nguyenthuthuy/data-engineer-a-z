# Airflow 2.9 + providers dùng trong video #19–#21.
# Bước 1: providers theo đúng constraints của Airflow 2.9.3 (không pin tay để khỏi lệch constraints).
# Bước 2: dbt, Great Expectations, DuckDB/Polars — cài sau, không constraints, rồi kiểm tra `airflow version` còn chạy.
# pyarrow < 19 vì constraints Airflow 2.9 giữ numpy 1.26 (pandas 2.1); pyarrow mới hơn đòi numpy >= 2.
FROM apache/airflow:2.9.3-python3.11

USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    "apache-airflow-providers-postgres" \
    "apache-airflow-providers-amazon" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-openlineage"

RUN pip install --no-cache-dir \
    "dbt-core>=1.9,<2" "dbt-duckdb>=1.9,<2" "dbt-postgres>=1.9,<2" \
    "great-expectations>=1.1,<2" \
    "duckdb>=1.1" "polars>=1.10" "pyarrow>=17,<19" "boto3>=1.35" \
    "psycopg[binary]>=3.2" "pytest>=8" && \
    airflow version
