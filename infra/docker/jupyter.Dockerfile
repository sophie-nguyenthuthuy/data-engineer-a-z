# Jupyter + PySpark 3.5 kernel + DuckDB/Polars cho lab phần 1–2 (chạy local, không cần cluster).
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.0

USER root
RUN cd /usr/local/spark/jars && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
USER ${NB_UID}

RUN pip install --no-cache-dir \
    "duckdb>=1.1" "polars>=1.10" "pandas>=2.2" "pyarrow>=17" \
    "psycopg[binary]>=3.2" "sqlalchemy>=2" "boto3>=1.35" \
    "confluent-kafka>=2.5" "fastavro>=1.9" "great-expectations>=1.1,<2" \
    "jupysql>=0.10" "matplotlib>=3.9"

COPY spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf
