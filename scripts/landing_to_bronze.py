#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
import logging
import os


# In[ ]:


# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# In[ ]:


def configure_spark():

    spark = (
        SparkSession.builder
        .appName("lakehouse-jupyter")

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


# In[ ]:


def processing_orders_dataset():
    logging.info("Iniciando ingestão da tabela: landing_orders_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS bronze
        LOCATION 's3a://bronze'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.olist_orders (
          order_id STRING,
          customer_id STRING,
          order_status STRING,
          order_purchase_timestamp STRING,
          order_approved_at STRING,
          order_delivered_carrier_date STRING,
          order_delivered_customer_date STRING,
          order_estimated_delivery_date STRING,
          datproc TIMESTAMP
        )
        USING DELTA
        LOCATION 's3a://bronze/olist_orders'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW landing_orders_dataset
        USING csv
        OPTIONS (
          path 's3a://landing/olist_orders_dataset.csv',
          header 'true',
          sep ',',
          inferSchema 'false',
          mode 'PERMISSIVE',
          quote '"',
          escape '"',
          multiLine 'true'
        )
    """)

    spark.sql("""
        INSERT OVERWRITE TABLE bronze.olist_orders
        SELECT
          order_id,
          customer_id,
          order_status,
          order_purchase_timestamp,
          order_approved_at,
          order_delivered_carrier_date,
          order_delivered_customer_date,
          order_estimated_delivery_date,
          current_timestamp() AS datproc
        FROM landing_orders_dataset
    """)
    logging.info("Finalizado ingestão da tabela: landing_orders_dataset")


# In[ ]:


def processing_order_reviews_dataset():
    logging.info("Iniciando ingestão da tabela: landing_order_reviews_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS bronze
        LOCATION 's3a://bronze'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.olist_order_reviews (
          review_id STRING,
          order_id STRING,
          review_score STRING,
          review_comment_title STRING,
          review_comment_message STRING,
          review_creation_date STRING,
          review_answer_timestamp STRING,
          datproc TIMESTAMP
        )
        USING DELTA
        LOCATION 's3a://bronze/olist_order_reviews'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW landing_order_reviews_dataset
        USING csv
        OPTIONS (
          path 's3a://landing/olist_order_reviews_dataset.csv',
          header 'true',
          sep ',',
          inferSchema 'false',
          mode 'PERMISSIVE',
          quote '"',
          escape '"',
          multiLine 'true'
        )
    """)

    spark.sql("""
        INSERT OVERWRITE TABLE bronze.olist_order_reviews
        SELECT
          review_id,
          order_id,
          review_score,
          review_comment_title,
          review_comment_message,
          review_creation_date,
          review_answer_timestamp,
          current_timestamp() AS datproc
        FROM landing_order_reviews_dataset
    """)
    logging.info("Finalizado ingestão da tabela: landing_order_reviews_dataset")


# In[ ]:


def processing_sellers_dataset():
    logging.info("Iniciando ingestão da tabela: landing_sellers_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS bronze
        LOCATION 's3a://bronze'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.olist_sellers (
          seller_id STRING,
          seller_zip_code_prefix STRING,
          seller_city STRING,
          seller_state STRING,
          datproc TIMESTAMP
        )
        USING DELTA
        LOCATION 's3a://bronze/olist_sellers'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW landing_sellers_dataset
        USING csv
        OPTIONS (
          path 's3a://landing/olist_sellers_dataset.csv',
          header 'true',
          sep ',',
          inferSchema 'false',
          mode 'PERMISSIVE',
          quote '"',
          escape '"',
          multiLine 'true'
        )
    """)

    spark.sql("""
        INSERT OVERWRITE TABLE bronze.olist_sellers
        SELECT
          seller_id,
          seller_zip_code_prefix,
          seller_city,
          seller_state,
          current_timestamp() AS datproc
        FROM landing_sellers_dataset
    """)
    logging.info("Finalizado ingestão da tabela: landing_sellers_dataset")


# In[ ]:


def processing_products_dataset():
    logging.info("Iniciando ingestão da tabela: landing_products_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS bronze
        LOCATION 's3a://bronze'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.olist_products (
          product_id STRING,
          product_category_name STRING,
          product_name_lenght STRING,
          product_description_lenght STRING,
          product_photos_qty STRING,
          product_weight_g STRING,
          product_length_cm STRING,
          product_height_cm STRING,
          product_width_cm STRING,
          datproc TIMESTAMP
        )
        USING DELTA
        LOCATION 's3a://bronze/olist_products'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW landing_products_dataset
        USING csv
        OPTIONS (
          path 's3a://landing/olist_products_dataset.csv',
          header 'true',
          sep ',',
          inferSchema 'false',
          mode 'PERMISSIVE',
          quote '"',
          escape '"',
          multiLine 'true'
        )
    """)

    spark.sql("""
        INSERT OVERWRITE TABLE bronze.olist_products
        SELECT
          product_id,
          product_category_name,
          product_name_lenght,
          product_description_lenght,
          product_photos_qty,
          product_weight_g,
          product_length_cm,
          product_height_cm,
          product_width_cm,
          current_timestamp() AS datproc
        FROM landing_products_dataset
    """)
    logging.info("Finalizado ingestão da tabela: landing_products_dataset")


# In[ ]:


def processing_order_items_dataset():
    logging.info("Iniciando ingestão da tabela: landing_order_items_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS bronze
        LOCATION 's3a://bronze'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.olist_order_items (
          order_id STRING,
          order_item_id STRING,
          product_id STRING,
          seller_id STRING,
          shipping_limit_date STRING,
          price STRING,
          freight_value STRING,
          datproc TIMESTAMP
        )
        USING DELTA
        LOCATION 's3a://bronze/olist_order_items'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW landing_order_items_dataset
        USING csv
        OPTIONS (
          path 's3a://landing/olist_order_items_dataset.csv',
          header 'true',
          sep ',',
          inferSchema 'false',
          mode 'PERMISSIVE',
          quote '"',
          escape '"',
          multiLine 'true'
        )
    """)

    spark.sql("""
        INSERT OVERWRITE TABLE bronze.olist_order_items
        SELECT
          order_id,
          order_item_id,
          product_id,
          seller_id,
          shipping_limit_date,
          price,
          freight_value,
          current_timestamp() AS datproc
        FROM landing_order_items_dataset
    """)
    logging.info("Finalizado ingestão da tabela: landing_order_items_dataset")


# In[ ]:


def processing_customers_dataset():
    logging.info("Iniciando ingestão da tabela: landing_customers_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS bronze
        LOCATION 's3a://bronze'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.olist_customers (
          customer_id STRING,
          customer_unique_id STRING,
          customer_zip_code_prefix STRING,
          customer_city STRING,
          customer_state STRING,
          datproc TIMESTAMP
        )
        USING DELTA
        LOCATION 's3a://bronze/olist_customers'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW landing_customers_dataset
        USING csv
        OPTIONS (
          path 's3a://landing/olist_customers_dataset.csv',
          header 'true',
          sep ',',
          inferSchema 'false',
          mode 'PERMISSIVE',
          quote '"',
          escape '"',
          multiLine 'true'
        )
    """)

    spark.sql("""
        INSERT OVERWRITE TABLE bronze.olist_customers
        SELECT
          customer_id,
          customer_unique_id,
          customer_zip_code_prefix,
          customer_city,
          customer_state,
          current_timestamp() AS datproc
        FROM landing_customers_dataset
    """)
    logging.info("Finalizado ingestão da tabela: landing_customers_dataset")


# In[ ]:


if __name__ == "__main__":
    spark = configure_spark()
    processing_orders_dataset()
    processing_order_reviews_dataset()
    processing_sellers_dataset()
    processing_products_dataset()
    processing_order_items_dataset()
    processing_customers_dataset()
    spark.stop()

