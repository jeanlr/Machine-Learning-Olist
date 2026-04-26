# jupyter/Dockerfile
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Java + curl (curl é importante aqui)
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 🔥 Define SPARK_HOME corretamente (alinhado com pyspark)
ENV SPARK_HOME=/opt/conda/lib/python3.11/site-packages/pyspark

RUN mkdir -p $SPARK_HOME/jars

# 🔥 Adiciona JARs diretamente no classpath do PySpark
RUN curl -L -o $SPARK_HOME/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o $SPARK_HOME/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -L -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Python deps
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# 🔥 Configuração limpa (SEM packages)
ENV PYSPARK_SUBMIT_ARGS="\
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
pyspark-shell"

USER jovyan
WORKDIR /home/jovyan