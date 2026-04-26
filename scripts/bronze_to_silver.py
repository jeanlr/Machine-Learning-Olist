#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import logging
import os


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
        .appName("lakehouse-jupyter-silver")

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


def processing_orders_dataset():
    logging.info("Iniciando ingestão da tabela: landing_orders_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS silver
        LOCATION 's3a://silver'
    """)


    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.olist_orders (
          order_id STRING,
          customer_id STRING,
          order_status STRING,
          order_purchase_timestamp TIMESTAMP,
          order_approved_at TIMESTAMP,
          order_delivered_carrier_date TIMESTAMP,
          order_delivered_customer_date TIMESTAMP,
          order_estimated_delivery_date TIMESTAMP,
          safra INT,
          datproc TIMESTAMP,
          row_hash STRING
        ) 
        USING DELTA
        LOCATION 's3a://silver/olist_orders'
    """)

    spark.sql("""
        -- 1) Stage: normaliza e parseia dates para timestamp
        CREATE OR REPLACE TEMP VIEW stage_orders AS
        SELECT
            order_id,
            customer_id,
            UPPER(TRIM(order_status)) AS order_status,
        
            -- timestamp de referência (suporta múltiplos formatos)
            COALESCE(
                try_to_timestamp(order_purchase_timestamp, 'yyyy-MM-dd HH:mm:ss'),
                try_to_timestamp(order_purchase_timestamp, 'yyyy/MM/dd HH:mm:ss'),
                try_to_timestamp(order_purchase_timestamp, 'dd/MM/yyyy HH:mm:ss'),
                try_to_timestamp(order_purchase_timestamp, 'dd-MM-yyyy HH:mm:ss'),
                try_to_timestamp(order_purchase_timestamp, 'yyyy-MM-dd'),
                try_to_timestamp(order_purchase_timestamp, 'yyyy/MM/dd'),
                try_to_timestamp(order_purchase_timestamp, 'dd/MM/yyyy'),
                try_to_timestamp(order_purchase_timestamp, 'dd-MM-yyyy')
            ) AS order_purchase_timestamp,
        
            COALESCE(
                try_to_timestamp(order_approved_at, 'yyyy-MM-dd HH:mm:ss'),
                try_to_timestamp(order_approved_at, 'yyyy/MM/dd HH:mm:ss'),
                try_to_timestamp(order_approved_at, 'dd/MM/yyyy HH:mm:ss'),
                try_to_timestamp(order_approved_at, 'dd-MM-yyyy HH:mm:ss'),
                try_to_timestamp(order_approved_at, 'yyyy-MM-dd'),
                try_to_timestamp(order_approved_at, 'yyyy/MM/dd'),
                try_to_timestamp(order_approved_at, 'dd/MM/yyyy'),
                try_to_timestamp(order_approved_at, 'dd-MM-yyyy')
            ) AS order_approved_at,
        
            COALESCE(
                try_to_timestamp(order_delivered_carrier_date, 'yyyy-MM-dd HH:mm:ss'),
                try_to_timestamp(order_delivered_carrier_date, 'yyyy/MM/dd HH:mm:ss'),
                try_to_timestamp(order_delivered_carrier_date, 'dd/MM/yyyy HH:mm:ss'),
                try_to_timestamp(order_delivered_carrier_date, 'dd-MM-yyyy HH:mm:ss'),
                try_to_timestamp(order_delivered_carrier_date, 'yyyy-MM-dd'),
                try_to_timestamp(order_delivered_carrier_date, 'yyyy/MM/dd'),
                try_to_timestamp(order_delivered_carrier_date, 'dd/MM/yyyy'),
                try_to_timestamp(order_delivered_carrier_date, 'dd-MM-yyyy')
            ) AS order_delivered_carrier_date,
        
            COALESCE(
                try_to_timestamp(order_delivered_customer_date, 'yyyy-MM-dd HH:mm:ss'),
                try_to_timestamp(order_delivered_customer_date, 'yyyy/MM/dd HH:mm:ss'),
                try_to_timestamp(order_delivered_customer_date, 'dd/MM/yyyy HH:mm:ss'),
                try_to_timestamp(order_delivered_customer_date, 'dd-MM-yyyy HH:mm:ss'),
                try_to_timestamp(order_delivered_customer_date, 'yyyy-MM-dd'),
                try_to_timestamp(order_delivered_customer_date, 'yyyy/MM/dd'),
                try_to_timestamp(order_delivered_customer_date, 'dd/MM/yyyy'),
                try_to_timestamp(order_delivered_customer_date, 'dd-MM-yyyy')
            ) AS order_delivered_customer_date,
        
            COALESCE(
                try_to_timestamp(order_estimated_delivery_date, 'yyyy-MM-dd HH:mm:ss'),
                try_to_timestamp(order_estimated_delivery_date, 'yyyy/MM/dd HH:mm:ss'),
                try_to_timestamp(order_estimated_delivery_date, 'dd/MM/yyyy HH:mm:ss'),
                try_to_timestamp(order_estimated_delivery_date, 'dd-MM-yyyy HH:mm:ss'),
                try_to_timestamp(order_estimated_delivery_date, 'yyyy-MM-dd'),
                try_to_timestamp(order_estimated_delivery_date, 'yyyy/MM/dd'),
                try_to_timestamp(order_estimated_delivery_date, 'dd/MM/yyyy'),
                try_to_timestamp(order_estimated_delivery_date, 'dd-MM-yyyy')
            ) AS order_estimated_delivery_date,
            
            CAST(date_format(order_purchase_timestamp, 'yyyyMM') AS INT) AS safra,
        
            datproc
        
        FROM delta.`s3a://bronze/olist_orders`
        WHERE order_id IS NOT NULL;
    """)

    spark.sql("""
        -- 2) Janela incremental (watermark de 90 dias)
        CREATE OR REPLACE TEMP VIEW stage_orders_win AS
        SELECT *
        FROM stage_orders
        WHERE datproc >= date_sub(current_timestamp(), 90);
    """)


    spark.sql("""
        -- 3) Dedup: mantém 1 linha por order_id (mais recente por order_purchase_timestamp)
        CREATE OR REPLACE TEMP VIEW stage_orders_dedup AS
        SELECT
          order_id,
          customer_id,
          order_status,
          order_purchase_timestamp,
          order_approved_at,
          order_delivered_carrier_date,
          order_delivered_customer_date,
          order_estimated_delivery_date,
          safra,
          datproc
        FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY order_id
              ORDER BY order_purchase_timestamp DESC NULLS LAST,
                       customer_id DESC            -- desempate determinístico
            ) AS rn
          FROM stage_orders_win s
          WHERE order_purchase_timestamp IS NOT NULL
        ) z
        WHERE rn = 1;
    """)

    spark.sql("""
        -- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
        CREATE OR REPLACE TEMP VIEW stage_orders_final AS
        SELECT
          order_id,
          customer_id,
          order_status,
          order_purchase_timestamp,
          order_approved_at,
          order_delivered_carrier_date,
          order_delivered_customer_date,
          order_estimated_delivery_date,
          safra,
          datproc,
          sha2(concat_ws('||',
            coalesce(order_id,''),
            coalesce(date_format(order_purchase_timestamp,'yyyy-MM-dd'),''),
            coalesce(order_status,'')
          ), 256) AS row_hash
        FROM stage_orders_dedup;
    """)    

    spark.sql("""
        -- 5) MERGE idempotente: só atualiza quando o hash difere
        MERGE INTO silver.olist_orders AS t
        USING stage_orders_final AS s
        ON t.order_id = s.order_id
        WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
          t.order_id = s.order_id,
          t.customer_id  = s.customer_id,
          t.order_status = s.order_status,
          t.order_purchase_timestamp = s.order_purchase_timestamp,
          t.order_approved_at = s.order_approved_at,
          t.order_delivered_carrier_date = s.order_delivered_carrier_date,
          t.order_delivered_customer_date = s.order_delivered_customer_date,
          t.order_estimated_delivery_date = s.order_estimated_delivery_date,
          t.safra     = s.safra,
          t.datproc     = s.datproc,
          t.row_hash     = s.row_hash
        WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date, safra, datproc, row_hash)
        VALUES (s.order_id, s.customer_id, s.order_status, s.order_purchase_timestamp, s.order_approved_at, s.order_delivered_carrier_date, s.order_delivered_customer_date, s.order_estimated_delivery_date, s.safra, s.datproc, s.row_hash);
            """)
    logging.info("Finalizado ingestão da tabela: landing_orders_dataset")


# In[5]:


def processing_order_reviews_dataset():
    logging.info("Iniciando ingestão da tabela: landing_order_reviews_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS silver
        LOCATION 's3a://silver'
    """)


    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.olist_order_reviews (
          review_id STRING,
          order_id STRING,
          review_score INT,
          review_comment_title STRING,
          review_comment_message STRING,
          review_creation_date DATE,
          review_answer_timestamp TIMESTAMP,
          safra INT,
          datproc TIMESTAMP,
          row_hash STRING
        )
        USING DELTA
        LOCATION 's3a://silver/olist_order_reviews'
    """)

    spark.sql("""
        -- 1) Stage: normaliza e parseia dates para timestamp
        CREATE OR REPLACE TEMP VIEW stage_order_reviews AS
        SELECT
          review_id,
          order_id,
          try_cast(TRIM(review_score) AS INT) AS review_score,
          review_comment_title,
          review_comment_message,
          -- timestamp de referência (suporta múltiplos formatos)
          to_date(COALESCE(
              try_to_timestamp(review_creation_date , 'yyyy-MM-dd HH:mm:ss'),
              try_to_timestamp(review_creation_date , 'yyyy/MM/dd HH:mm:ss'),
              try_to_timestamp(review_creation_date , 'dd/MM/yyyy HH:mm:ss'),
              try_to_timestamp(review_creation_date , 'dd-MM-yyyy HH:mm:ss'),
              try_to_timestamp(review_creation_date , 'yyyy-MM-dd'),
              try_to_timestamp(review_creation_date , 'yyyy/MM/dd'),
              try_to_timestamp(review_creation_date , 'dd/MM/yyyy'),
              try_to_timestamp(review_creation_date , 'dd-MM-yyyy')
          )) AS review_creation_date,
        
        
          COALESCE(
            try_to_timestamp(review_answer_timestamp, 'yyyy-MM-dd HH:mm:ss'),
            try_to_timestamp(review_answer_timestamp, 'yyyy/MM/dd HH:mm:ss'),
            try_to_timestamp(review_answer_timestamp, 'dd/MM/yyyy HH:mm:ss'),
            try_to_timestamp(review_answer_timestamp, 'dd-MM-yyyy HH:mm:ss'),
            try_to_timestamp(review_answer_timestamp, 'yyyy-MM-dd'),
            try_to_timestamp(review_answer_timestamp, 'yyyy/MM/dd'),
            try_to_timestamp(review_answer_timestamp, 'dd/MM/yyyy'),
            try_to_timestamp(review_answer_timestamp, 'dd-MM-yyyy')
          ) AS review_answer_timestamp,
        
          CAST(date_format(review_creation_date, 'yyyyMM') AS INT) AS safra,
        
          datproc
        FROM delta.`s3a://bronze/olist_order_reviews`
        WHERE review_id IS NOT NULL;
    """)

    spark.sql("""
        -- 2) Janela incremental (watermark de 90 dias)
        CREATE OR REPLACE TEMP VIEW stage_order_reviews_win AS
        SELECT *
        FROM stage_order_reviews
        WHERE datproc >= date_sub(current_timestamp(), 90);
    """)


    spark.sql("""
        -- 3) Dedup: mantém 1 linha por order_id (mais recente por review_creation_date)
        CREATE OR REPLACE TEMP VIEW stage_order_reviews_dedup AS
        SELECT
          review_id,
          order_id,
          review_score,
          review_comment_title,
          review_comment_message,
          review_creation_date,
          review_answer_timestamp,
          safra,
          datproc
        FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY review_id
              ORDER BY review_creation_date DESC NULLS LAST,
                       order_id DESC            -- desempate determinístico
            ) AS rn
          FROM stage_order_reviews_win s
          WHERE review_creation_date IS NOT NULL
        ) z
        WHERE rn = 1;
    """)

    spark.sql("""
        -- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
        CREATE OR REPLACE TEMP VIEW stage_order_reviews_final AS
        SELECT
          review_id,
          order_id,
          review_score,
          review_comment_title,
          review_comment_message,
          review_creation_date,
          review_answer_timestamp,
          safra,
          datproc,
          sha2(concat_ws('||',
            coalesce(review_id,''),
            coalesce(date_format(review_creation_date,'yyyy-MM-dd'),''),
            coalesce(cast(review_score as string),'')
          ), 256) AS row_hash
        FROM stage_order_reviews_dedup;
    """)    

    spark.sql("""
        -- 5) MERGE idempotente: só atualiza quando o hash difere
        MERGE INTO silver.olist_order_reviews AS t
        USING stage_order_reviews_final AS s
        ON t.review_id = s.review_id
        WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
          t.review_id = s.review_id,
          t.order_id  = s.order_id,
          t.review_score = s.review_score,
          t.review_comment_title = s.review_comment_title,
          t.review_comment_message = s.review_comment_message,
          t.review_creation_date = s.review_creation_date,
          t.review_answer_timestamp = s.review_answer_timestamp,
          t.safra = s.safra,
          t.datproc     = s.datproc,
          t.row_hash     = s.row_hash
        WHEN NOT MATCHED THEN INSERT (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp, safra, datproc, row_hash)
        VALUES (s.review_id, s.order_id, s.review_score, s.review_comment_title, s.review_comment_message, s.review_creation_date, s.review_answer_timestamp, s.safra, s.datproc, s.row_hash);
            """)
    logging.info("Finalizado ingestão da tabela: landing_order_reviews_dataset")


# In[6]:


def processing_sellers_dataset():
    logging.info("Iniciando ingestão da tabela: landing_sellers_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS silver
        LOCATION 's3a://silver'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.olist_sellers (
          seller_id STRING,
          seller_zip_code_prefix STRING,
          seller_city STRING,
          seller_state STRING,
          datproc TIMESTAMP,
          row_hash STRING
        )
        USING DELTA
        LOCATION 's3a://silver/olist_sellers'
    """)

    spark.sql("""
        -- 1) Stage: normaliza e parseia dates para timestamp
        CREATE OR REPLACE TEMP VIEW stage_olist_sellers AS
        SELECT
          seller_id,
          seller_zip_code_prefix,
          UPPER(TRIM(seller_city)) AS seller_city,
          seller_state,  
          datproc
        FROM delta.`s3a://bronze/olist_sellers`
        WHERE seller_id IS NOT NULL;
    """)

    spark.sql("""
        -- 2) Janela incremental (watermark de 90 dias)
        CREATE OR REPLACE TEMP VIEW stage_olist_sellers_win AS
        SELECT *
        FROM stage_olist_sellers
        WHERE datproc >= date_sub(current_timestamp(), 90);
    """)


    spark.sql("""
        -- 3) Dedup: mantém 1 linha por order_id (mais recente por datproc)
        CREATE OR REPLACE TEMP VIEW stage_olist_sellers_dedup AS
        SELECT
          seller_id,
          seller_zip_code_prefix,
          seller_city,
          seller_state,
          datproc
        FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY seller_id
              ORDER BY datproc DESC NULLS LAST
            ) AS rn
          FROM stage_olist_sellers_win s
          WHERE seller_id IS NOT NULL
        ) z
        WHERE rn = 1;
    """)

    spark.sql("""
        -- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
        CREATE OR REPLACE TEMP VIEW stage_order_sellers_final AS
        SELECT
          seller_id,
          seller_zip_code_prefix,
          seller_city,
          seller_state,
          datproc,
          sha2(concat_ws('||',
            coalesce(seller_id,''),
            coalesce(seller_zip_code_prefix,''),
            coalesce(seller_city,''),
            coalesce(seller_state,'')
          ), 256) AS row_hash
        FROM stage_olist_sellers_dedup;
    """)    

    spark.sql("""
        -- 5) MERGE idempotente: só atualiza quando o hash difere
        MERGE INTO silver.olist_sellers AS t
        USING stage_order_sellers_final AS s
        ON t.seller_id = s.seller_id
        WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
          t.seller_id = s.seller_id,
          t.seller_zip_code_prefix  = s.seller_zip_code_prefix,
          t.seller_city = s.seller_city,
          t.seller_state = s.seller_state,
          t.datproc     = s.datproc,
          t.row_hash     = s.row_hash
        WHEN NOT MATCHED THEN INSERT (seller_id, seller_zip_code_prefix, seller_city, seller_state, datproc, row_hash)
        VALUES (s.seller_id, s.seller_zip_code_prefix, s.seller_city, s.seller_state, s.datproc, s.row_hash);
            """)
    logging.info("Finalizado ingestão da tabela: landing_sellers_dataset")


# In[10]:


def processing_products_dataset():
    logging.info("Iniciando ingestão da tabela: landing_products_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS silver
        LOCATION 's3a://silver'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.olist_products (
          product_id STRING,
          product_category_name STRING,
          product_name_lenght INT,
          product_description_lenght INT,
          product_photos_qty INT,
          product_weight_g INT,
          product_length_cm INT,
          product_height_cm INT,
          product_width_cm INT,
          datproc TIMESTAMP,
          row_hash STRING
        )
        USING DELTA
        LOCATION 's3a://silver/olist_products'
    """)

    spark.sql("""
        -- 1) Stage: normaliza e parseia dates para timestamp
        CREATE OR REPLACE TEMP VIEW stage_olist_products AS
        SELECT
          product_id,
          product_category_name,
          try_cast(TRIM(product_name_lenght) AS INT) AS product_name_lenght,
          try_cast(TRIM(product_description_lenght) AS INT) AS product_description_lenght,
          try_cast(TRIM(product_photos_qty) AS INT) AS product_photos_qty,
          try_cast(TRIM(product_weight_g) AS INT) AS product_weight_g,
          try_cast(TRIM(product_length_cm) AS INT) AS product_length_cm,
          try_cast(TRIM(product_height_cm) AS INT) AS product_height_cm,
          try_cast(TRIM(product_width_cm) AS INT) AS product_width_cm,
          datproc
        FROM delta.`s3a://bronze/olist_products`
        WHERE product_id IS NOT NULL;
    """)

    spark.sql("""
        -- 2) Janela incremental (watermark de 90 dias)
        CREATE OR REPLACE TEMP VIEW stage_olist_products_win AS
        SELECT *
        FROM stage_olist_products
        WHERE datproc >= date_sub(current_timestamp(), 90);
    """)


    spark.sql("""
        -- 3) Dedup: mantém 1 linha por order_id (mais recente por review_creation_date)
        CREATE OR REPLACE TEMP VIEW stage_olist_products_dedup AS
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
          datproc
        FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY product_id
              ORDER BY product_category_name DESC NULLS LAST
            ) AS rn
          FROM stage_olist_products_win s
        ) z
        WHERE rn = 1;
    """)

    spark.sql("""
        -- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
        CREATE OR REPLACE TEMP VIEW stage_olist_products_final AS
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
          datproc,
          sha2(concat_ws('||',
            coalesce(product_id,''),
            coalesce(product_category_name,''),
            coalesce(cast(product_name_lenght as string),''),
            coalesce(cast(product_description_lenght as string),''),
            coalesce(cast(product_photos_qty as string),''),
            coalesce(cast(product_weight_g as string),''),
            coalesce(cast(product_length_cm as string),''),
            coalesce(cast(product_height_cm as string),''),
            coalesce(cast(product_width_cm as string),'')
          ), 256) AS row_hash
        FROM stage_olist_products_dedup;
    """)    

    spark.sql("""
        -- 5) MERGE idempotente: só atualiza quando o hash difere
        MERGE INTO silver.olist_products AS t
        USING stage_olist_products_final AS s
        ON t.product_id = s.product_id
        WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
          t.product_id = s.product_id,
          t.product_category_name  = s.product_category_name,
          t.product_name_lenght = s.product_name_lenght,
          t.product_description_lenght = s.product_description_lenght,
          t.product_photos_qty = s.product_photos_qty,
          t.product_weight_g = s.product_weight_g,
          t.product_length_cm = s.product_length_cm,
          t.product_height_cm = s.product_height_cm,
          t.product_width_cm = s.product_width_cm,
          t.datproc     = s.datproc,
          t.row_hash     = s.row_hash
        WHEN NOT MATCHED THEN INSERT (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm, datproc, row_hash)
        VALUES (s.product_id, s.product_category_name, s.product_name_lenght, s.product_description_lenght, s.product_photos_qty, s.product_weight_g, s.product_length_cm, s.product_height_cm, s.product_width_cm, s.datproc, s.row_hash);
                    """)
    logging.info("Finalizado ingestão da tabela: landing_products_dataset")


# In[12]:


def processing_order_items_dataset():
    logging.info("Iniciando ingestão da tabela: landing_order_items_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS silver
        LOCATION 's3a://silver'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.olist_order_items (
          order_id STRING,
          order_item_id INT,
          product_id STRING,
          seller_id STRING,
          shipping_limit_date TIMESTAMP,
          price DECIMAL(18,2),
          freight_value DECIMAL(18,2),
          datproc TIMESTAMP,
          row_hash STRING
        ) USING DELTA
        LOCATION 's3a://silver/olist_order_items'
    """)

    spark.sql("""
        -- 1) Stage: normaliza e parseia dates para timestamp
        CREATE OR REPLACE TEMP VIEW stage_olist_order_items AS
        SELECT
          order_id,
          try_cast(order_item_id AS INT) AS order_item_id,
          product_id,
          seller_id,
          -- timestamp de referência (suporta múltiplos formatos)
          COALESCE(
            try_to_timestamp(shipping_limit_date, 'yyyy-MM-dd HH:mm:ss'),
            try_to_timestamp(shipping_limit_date, 'yyyy/MM/dd HH:mm:ss'),
            try_to_timestamp(shipping_limit_date, 'dd/MM/yyyy HH:mm:ss'),
            try_to_timestamp(shipping_limit_date, 'dd-MM-yyyy HH:mm:ss'),
            try_to_timestamp(shipping_limit_date, 'yyyy-MM-dd'),
            try_to_timestamp(shipping_limit_date, 'yyyy/MM/dd'),
            try_to_timestamp(shipping_limit_date, 'dd/MM/yyyy'),
            try_to_timestamp(shipping_limit_date, 'dd-MM-yyyy')
          ) AS shipping_limit_date,
        
          try_cast(price AS DECIMAL) AS price,
          try_cast(freight_value AS DECIMAL) AS freight_value,
        
          datproc
        FROM delta.`s3a://bronze/olist_order_items`
        WHERE order_id IS NOT NULL;
    """)

    spark.sql("""
        -- 2) Janela incremental (watermark de 90 dias)
        CREATE OR REPLACE TEMP VIEW stage_olist_order_items_win AS
        SELECT *
        FROM stage_olist_order_items
        WHERE datproc >= date_sub(current_timestamp(), 90);
    """)


    spark.sql("""
        -- 3) Dedup: mantém 1 linha por order_id (mais recente por order_purchase_timestamp)
        CREATE OR REPLACE TEMP VIEW stage_olist_order_items_dedup AS
        SELECT
          order_id,
          order_item_id,
          product_id,
          seller_id,
          shipping_limit_date,
          price,
          freight_value,
          datproc
        FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY order_id
              ORDER BY order_item_id DESC NULLS LAST,
                       product_id DESC            -- desempate determinístico
            ) AS rn
          FROM stage_olist_order_items_win s
          WHERE shipping_limit_date IS NOT NULL
        ) z
        WHERE rn = 1;
    """)

    spark.sql("""
        -- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
        CREATE OR REPLACE TEMP VIEW stage_olist_order_items_final AS
        SELECT
          order_id,
          order_item_id,
          product_id,
          seller_id,
          shipping_limit_date,
          price,
          freight_value,
          datproc,
          sha2(concat_ws('||',
            coalesce(order_id,''),
            coalesce(cast(order_item_id as string),''),
            coalesce(product_id,''),
            coalesce(seller_id,''),
            coalesce(date_format(shipping_limit_date,'yyyy-MM-dd'),''),
            coalesce(cast(price as string),''),
            coalesce(cast(freight_value as string),'')
          ), 256) AS row_hash
        FROM stage_olist_order_items_dedup;
    """)    

    spark.sql("""
        -- 5) MERGE idempotente: só atualiza quando o hash difere
        MERGE INTO silver.olist_order_items AS t
        USING stage_olist_order_items_final AS s
        ON t.order_id = s.order_id
        WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
          t.order_id = s.order_id,
          t.order_item_id = s.order_item_id,
          t.product_id = s.product_id,
          t.seller_id = s.seller_id,
          t.shipping_limit_date = s.shipping_limit_date,
          t.price = s.price,
          t.freight_value = s.freight_value,
          t.datproc = s.datproc,
          t.row_hash = s.row_hash
        WHEN NOT MATCHED THEN INSERT (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value, datproc, row_hash)
        VALUES (s.order_id, s.order_item_id, s.product_id, s.seller_id, s.shipping_limit_date, s.price, s.freight_value, s.datproc, s.row_hash);  
    """)
    logging.info("Finalizado ingestão da tabela: landing_order_items_dataset")


# In[14]:


def processing_customers_dataset():
    logging.info("Iniciando ingestão da tabela: landing_customers_dataset")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS silver
        LOCATION 's3a://silver'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.olist_customers (
          customer_id STRING,
          customer_unique_id STRING,
          customer_zip_code_prefix STRING,
          customer_city STRING,
          customer_state STRING,
          datproc TIMESTAMP,
          row_hash STRING
        ) USING DELTA
        LOCATION 's3a://silver/olist_customers'
    """)

    spark.sql("""
        -- 1) Stage: normaliza e parseia dates para timestamp
        CREATE OR REPLACE TEMP VIEW stage_olist_customers AS
        SELECT
          customer_id,
          customer_unique_id,
          customer_zip_code_prefix,
          UPPER(TRIM(customer_city)) AS customer_city,
          customer_state,
          datproc
        FROM delta.`s3a://bronze/olist_customers`
        WHERE customer_id IS NOT NULL;
    """)

    spark.sql("""
        -- 2) Janela incremental (watermark de 90 dias)
        CREATE OR REPLACE TEMP VIEW stage_olist_customers_win AS
        SELECT *
        FROM stage_olist_customers
        WHERE datproc >= date_sub(current_timestamp(), 90);
    """)


    spark.sql("""
        -- 3) Dedup: mantém 1 linha por order_id (mais recente por order_purchase_timestamp)
        CREATE OR REPLACE TEMP VIEW stage_olist_customers_dedup AS
        SELECT
          customer_id,
          customer_unique_id,
          customer_zip_code_prefix,
          customer_city,
          customer_state,
          datproc
        FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY customer_id
              ORDER BY customer_unique_id DESC NULLS LAST
            ) AS rn
          FROM stage_olist_customers_win s
        ) z
        WHERE rn = 1;
    """)

    spark.sql("""
        -- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
        CREATE OR REPLACE TEMP VIEW stage_olist_customers_final AS
        SELECT
          customer_id,
          customer_unique_id,
          customer_zip_code_prefix,
          customer_city,
          customer_state,
          datproc,
          sha2(concat_ws('||',
            coalesce(customer_id,''),
            coalesce(customer_unique_id,''),
            coalesce(customer_zip_code_prefix,''),
            coalesce(customer_zip_code_prefix,'')
          ), 256) AS row_hash
        FROM stage_olist_customers_dedup;
    """)    

    spark.sql("""
        -- 5) MERGE idempotente: só atualiza quando o hash difere
        MERGE INTO silver.olist_customers AS t
        USING stage_olist_customers_final AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN UPDATE SET
          t.customer_id = s.customer_id,
          t.customer_unique_id = s.customer_unique_id,
          t.customer_zip_code_prefix = s.customer_zip_code_prefix,
          t.customer_city = s.customer_city,
          t.customer_state = s.customer_state,
          t.datproc = s.datproc,
          t.row_hash = s.row_hash
        WHEN NOT MATCHED THEN INSERT (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, datproc, row_hash)
        VALUES (s.customer_id, s.customer_unique_id, s.customer_zip_code_prefix, s.customer_city, s.customer_state, s.datproc, s.row_hash);    
    """)
    logging.info("Finalizado ingestão da tabela: landing_customers_dataset")


# In[16]:


if __name__ == "__main__":
    spark = configure_spark()
    processing_orders_dataset()
    processing_order_reviews_dataset()
    processing_sellers_dataset()
    processing_products_dataset()
    processing_order_items_dataset()
    processing_customers_dataset()
    spark.stop()

