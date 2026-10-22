# Spark 3.5 + Iceberg + hadoop-aws, dùng cho cả master lẫn worker (video #10, #11, #15, #17).
FROM bitnami/spark:3.5.3

USER root
ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG KAFKA_SQL_VERSION=3.5.3

RUN install_packages curl ca-certificates && \
    cd /opt/bitnami/spark/jars && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${KAFKA_SQL_VERSION}/spark-sql-kafka-0-10_2.12-${KAFKA_SQL_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${KAFKA_SQL_VERSION}/spark-token-provider-kafka-0-10_2.12-${KAFKA_SQL_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Default S3A → MinIO. Override via spark-submit --conf when pointing at real S3 (video #09).
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
USER 1001
