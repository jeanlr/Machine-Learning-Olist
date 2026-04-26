#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import expr, col, round, avg, max, min, sum, count, when, lit
from itertools import product
from datetime import datetime
import pytz
import os
import logging


# In[2]:


# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# In[3]:


def configure_spark():

    spark = (
        SparkSession.builder
        .appName("lakehouse-jupyter-abt")

        # conexão com cluster
        .master(os.getenv("SPARK_MASTER"))

        # 🚨 ESSENCIAL em Docker
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST"))
        .config("spark.driver.bindAddress", "0.0.0.0")

        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # S3 / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))

        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        # performance
        .config("spark.sql.adaptive.enabled", "true")

        # Delta fix
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# In[4]:


spark = configure_spark()
spark


# In[5]:


## parametros de data
data_exec_inicial = 201709
data_exec_final = 201808


# In[14]:


path_orders = "delta.`s3a://gold/book_orders`"
path_reviews = "delta.`s3a://gold/book_reviews`"
path_target = "delta.`s3a://gold/public_target_sellers`"


# In[11]:


df_orders = (
    spark.read
         .table(path_orders)
         .filter(
             (col("safra") >= data_exec_inicial) &
             (col("safra") <= data_exec_final)
         )
)


# In[15]:


df_reviews = (
    spark.read
         .table(path_reviews)
         .filter(
             (col("safra") >= data_exec_inicial) &
             (col("safra") <= data_exec_final)
         )
)


# In[17]:


df_target = (
    spark.read
         .table(path_target)
         .filter(
             (col("safra") >= data_exec_inicial) &
             (col("safra") <= data_exec_final)
         )
)


# In[20]:


df_temp_01 = (
    df_target.alias("b")
        .join(
            df_orders.alias("a"),
            on=["seller_id", "safra"],
            how="left"
        )
        .join(
            df_reviews.alias("c"),
            on=["seller_id", "safra"],
            how="left"
        )       
        .drop("rn", "DATPROC", "row_hash")        
)

df_temp_01.createOrReplaceTempView("df_temp_01")


# In[22]:


abt_treino = df_temp_01.filter(
    (F.col("safra") >= "201709") & (F.col("SAFRA") <= "201804")
)

abt_teste = df_temp_01.filter(
    (F.col("safra") >= "201805") & (F.col("SAFRA") <= "201808")
)


# In[23]:


abt_treino.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("delta.`s3a://gold/abt_treino`")

# Teste
abt_teste.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("delta.`s3a://gold/abt_teste`")


# In[24]:


spark.stop()

