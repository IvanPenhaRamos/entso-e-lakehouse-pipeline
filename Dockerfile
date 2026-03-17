FROM apache/airflow:2.9.1
ENV PYSPARK_VERSION=3.5.1 \
    ICEBERG_VERSION=1.7.1 \
    NESSIE_VERSION=0.106.0 \
    HADOOP_VERSION=3.3.4 \
    AWS_SDK_VERSION=1.12.262
USER airflow
RUN pip install --no-cache-dir pyspark==$PYSPARK_VERSION
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless &&\
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN JARS_DIR=/home/airflow/.local/lib/python3.12/site-packages/pyspark/jars && \
    mkdir -p $JARS_DIR && \
    curl -O --output-dir $JARS_DIR \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar && \
    curl -O --output-dir $JARS_DIR \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.106.0/nessie-spark-extensions-3.5_2.12-0.106.0.jar && \
    curl -O --output-dir $JARS_DIR \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar &&\
    curl -O --output-dir $JARS_DIR \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    chown -R airflow:root $JARS_DIR
USER airflow