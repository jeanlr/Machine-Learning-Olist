#!/usr/bin/env python
# coding: utf-8

# In[205]:


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


# In[206]:


# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# In[207]:


def configure_spark():

    spark = (
        SparkSession.builder
        .appName("lakehouse-jupyter-target")

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


# In[208]:


spark = configure_spark()
spark


# In[209]:


agora=datetime.now(pytz.timezone('America/Sao_Paulo'))
dthproc=agora.strftime("%Y%m%d%H%M%S")


# In[210]:


#201808, 201807, 201806, 201805, 201804, 201803, 201802, 201801, 201712, 201711, 201710, 201709
# converte YYYYMM -> date
data_exec_inicial = 201808
# converte YYYYMM -> date
data_dt = datetime.strptime(str(data_exec_inicial), "%Y%m")

# subtrai 12 meses
data_exec_final = int((data_dt + relativedelta(months=2)).strftime("%Y%m"))
data_exec_final


# In[211]:


logging.info("Iniciando criação do publico e target")

df_orders = (
    spark.read.table("delta.`s3a://silver/olist_orders`")
    .filter(F.col("order_status") == "DELIVERED")
)

df_orders.createOrReplaceTempView("df_orders")


# In[212]:


df_order_items = spark.read.table("delta.`s3a://silver/olist_order_items`")

df_order_items.createOrReplaceTempView("df_order_items")


# In[213]:


df_customers = spark.read.table("delta.`s3a://silver/olist_customers`")

df_customers.createOrReplaceTempView("df_customers")


# In[214]:


df_sellers = spark.read.table("delta.`s3a://silver/olist_sellers`")

df_sellers.createOrReplaceTempView("df_sellers")


# In[215]:


df_join = spark.sql("""
SELECT
    oi.seller_id,
    oi.price,
    o.safra
FROM df_orders o
LEFT JOIN df_order_items oi
    ON o.order_id = oi.order_id
LEFT JOIN df_sellers s
    ON oi.seller_id = s.seller_id
LEFT JOIN df_customers c
    ON o.customer_id = c.customer_id    
WHERE oi.seller_id IS NOT NULL
ORDER BY oi.seller_id, o.order_purchase_timestamp
""")

df_join.createOrReplaceTempView("df_join")


# In[216]:


df_orders_02 = spark.sql(f"""
WITH base_inicial AS (
    SELECT DISTINCT seller_id, safra
    FROM df_join
    WHERE safra = {data_exec_inicial}
),

compras_2m AS (
    SELECT DISTINCT seller_id
    FROM df_join
    WHERE (CAST(SUBSTRING(CAST(safra AS STRING), 1, 4) AS INT) * 12 +
           CAST(SUBSTRING(CAST(safra AS STRING), 5, 2) AS INT))
      >
          (CAST(SUBSTRING(CAST({data_exec_inicial} AS STRING), 1, 4) AS INT) * 12 +
           CAST(SUBSTRING(CAST({data_exec_inicial} AS STRING), 5, 2) AS INT))
      
      AND
        (CAST(SUBSTRING(CAST(safra AS STRING), 1, 4) AS INT) * 12 +
         CAST(SUBSTRING(CAST(safra AS STRING), 5, 2) AS INT))
      <=
        (CAST(SUBSTRING(CAST({data_exec_inicial} AS STRING), 1, 4) AS INT) * 12 +
         CAST(SUBSTRING(CAST({data_exec_inicial} AS STRING), 5, 2) AS INT)) + 2
)

SELECT 
    b.seller_id,
    b.safra,
    CASE 
        WHEN c.seller_id IS NOT NULL THEN 0
        ELSE 1
    END AS flag_sem_venda_2m
FROM base_inicial b
LEFT JOIN compras_2m c
    ON b.seller_id = c.seller_id
""")

df_orders_02.createOrReplaceTempView("df_orders_02")


# In[217]:


spark.sql("""
-- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
CREATE OR REPLACE TEMP VIEW stage_orders_final AS
SELECT
  *,
  sha2(concat_ws('||',
    coalesce(seller_id,''),
    coalesce(cast(safra as int),''),
    coalesce(cast(flag_sem_venda_2m as int),'')
    --coalesce(product_id,''),
    --coalesce(seller_id,''),
    --coalesce(date_format(shipping_limit_date,'yyyy-MM-dd'),''),
    --coalesce(cast(price as string),''),
    --coalesce(cast(freight_value as string),'')
  ), 256) AS row_hash
FROM df_orders_02;
""")


# In[218]:


spark.sql("""
        CREATE DATABASE IF NOT EXISTS gold
        LOCATION 's3a://gold'
""")


# In[219]:


from delta.tables import DeltaTable

path = "s3a://gold/public_target_sellers"

if not spark.catalog.tableExists("gold.public_target_sellers"):
    
    # Se já existe Delta no storage → só registra
    if DeltaTable.isDeltaTable(spark, path):
        spark.sql(f"""
            CREATE TABLE gold.public_target_sellers
            USING DELTA
            LOCATION '{path}'
        """)
    
    # Se não existe nada → cria do zero
    else:
        spark.sql(f"""
            CREATE TABLE gold.public_target_sellers
            USING DELTA
            PARTITIONED BY (safra)
            LOCATION '{path}'
            AS SELECT * FROM stage_orders_final WHERE 1=0
        """)


# In[220]:


spark.sql("""
MERGE INTO gold.public_target_sellers AS t
USING stage_orders_final AS s
ON t.seller_id = s.seller_id
AND t.safra = s.safra

WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN
UPDATE SET *

WHEN NOT MATCHED THEN
INSERT *
;
""")


# In[221]:


logging.info("Finalizando do publico e target")


# In[222]:


spark.stop()

