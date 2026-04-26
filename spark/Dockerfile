# spark/Dockerfile
FROM apache/spark:3.5.0

USER root

# Install dependencies
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    curl \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python alias
RUN ln -s /usr/bin/python3 /usr/bin/python

# 🔥 Definir SPARK_HOME corretamente
ENV SPARK_HOME=/opt/spark

RUN mkdir -p $SPARK_HOME/jars

# 🔥 Adicionar JARs no classpath do Spark (CORRETO)
RUN curl -L -o $SPARK_HOME/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o $SPARK_HOME/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -L -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt

# Environment
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Configure Spark defaults for Delta Lake and MinIO
RUN mkdir -p $SPARK_HOME/conf && \
    echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.endpoint http://minio:9000" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.path.style.access true" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" >> $SPARK_HOME/conf/spark-defaults.conf

# Create working directory
WORKDIR /opt/spark

# Copy entrypoint script
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]