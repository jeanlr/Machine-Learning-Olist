FROM apache/airflow:2.8.1-python3.10

USER root

# =============================
# System + Java
# =============================
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    openjdk-17-jdk-headless \
    procps \
    curl \
    ca-certificates \
    && update-ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# =============================
# Install Spark
# =============================
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN curl -fL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# =============================
# Delta + AWS jars
# =============================
RUN mkdir -p $SPARK_HOME/jars && \

    # Delta Lake
    curl -L -o $SPARK_HOME/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \

    curl -L -o $SPARK_HOME/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \

    # AWS S3 Support
    curl -L -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \

    curl -L -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# =============================
# Python deps
# =============================
COPY requirements.txt /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# =============================
# PySpark + Delta
# =============================
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    boto3==1.34.0

# =============================
# Spark configs
# =============================
ENV PYSPARK_PYTHON=python3

ENV PYSPARK_SUBMIT_ARGS="\
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
pyspark-shell"

# =============================
# ENV
# =============================
ENV PYTHONPATH=/opt/airflow:/opt/airflow/scripts:$PYTHONPATH

WORKDIR /opt/airflow