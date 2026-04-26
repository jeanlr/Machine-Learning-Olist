#!/usr/bin/env python
# coding: utf-8

# In[245]:


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


# In[246]:


# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# In[247]:


def configure_spark():

    spark = (
        SparkSession.builder
        .appName("lakehouse-jupyter-book-reviews")

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


# In[248]:


agora=datetime.now(pytz.timezone('America/Sao_Paulo'))
dthproc=agora.strftime("%Y%m%d%H%M%S")


# In[249]:


#executar todas as datas abaixo em (  data_exec_inicial  ) 
#201808, 201807, 201806, 201805, 201804, 201803, 201802, 201801, 201712, 201711, 201710, 201709
# converte YYYYMM -> date
data_exec_inicial = 201808
# converte YYYYMM -> date
data_dt = datetime.strptime(str(data_exec_inicial), "%Y%m")

# subtrai 12 meses
data_exec_final = int((data_dt - relativedelta(months=12)).strftime("%Y%m"))
data_exec_final


# In[250]:


spark = configure_spark()
spark


# In[251]:


logging.info("Iniciando criação do book: pedidos")

df_reviews = spark.read.table("delta.`s3a://silver/olist_order_reviews`")

df_reviews.createOrReplaceTempView("df_reviews")


# In[252]:


df_order_items = spark.read.table("delta.`s3a://silver/olist_order_items`")

df_order_items.createOrReplaceTempView("df_order_items")


# In[253]:


df_sellers = spark.read.table("delta.`s3a://silver/olist_sellers`")

df_sellers.createOrReplaceTempView("df_sellers")


# In[254]:


df_join = spark.sql("""
SELECT
    oi.seller_id,
    r.review_score,
    r.review_creation_date,
    r.safra
FROM df_reviews r
LEFT JOIN df_order_items oi
    ON r.order_id = oi.order_id
LEFT JOIN df_sellers s
    ON oi.seller_id = s.seller_id
WHERE oi.seller_id IS NOT NULL
ORDER BY oi.seller_id, r.review_creation_date 
""")

df_join.createOrReplaceTempView("df_join")


# In[255]:


df_reviews_01 = spark.sql(f"""
    SELECT DISTINCT seller_id, safra FROM df_join WHERE safra = {data_exec_inicial}
""")

df_reviews_01.createOrReplaceTempView("df_reviews_01")


# In[256]:


df_reviews_02 = spark.sql(f"""
    SELECT * FROM df_join WHERE safra BETWEEN {data_exec_final} AND {data_exec_inicial}
""")

df_reviews_02.createOrReplaceTempView("df_reviews_02")


# In[257]:


df_reviews_02.createOrReplaceTempView("df_transacoes")

df_temp_01 = spark.sql("""
WITH base AS (
    SELECT
        *,
        TO_DATE(CONCAT(safra, '01'), 'yyyyMMdd') AS data_dt
    FROM df_transacoes
)

SELECT
    *,
    CASE
        WHEN data_dt BETWEEN
             ADD_MONTHS(MAX(data_dt) OVER (PARTITION BY seller_id), -1)
             AND MAX(data_dt) OVER (PARTITION BY seller_id)
        THEN 1 ELSE 0
    END AS u1m,

    CASE
        WHEN data_dt BETWEEN
             ADD_MONTHS(MAX(data_dt) OVER (PARTITION BY seller_id), -3)
             AND MAX(data_dt) OVER (PARTITION BY seller_id)
        THEN 1 ELSE 0
    END AS u3m,


    CASE
        WHEN data_dt BETWEEN
             ADD_MONTHS(MAX(data_dt) OVER (PARTITION BY seller_id), -6)
             AND MAX(data_dt) OVER (PARTITION BY seller_id)
        THEN 1 ELSE 0
    END AS u6m,

    CASE
        WHEN data_dt BETWEEN
             ADD_MONTHS(MAX(data_dt) OVER (PARTITION BY seller_id), -9)
             AND MAX(data_dt) OVER (PARTITION BY seller_id)
        THEN 1 ELSE 0
    END AS u9m,    

    CASE
        WHEN data_dt BETWEEN
             ADD_MONTHS(MAX(data_dt) OVER (PARTITION BY seller_id), -12)
             AND MAX(data_dt) OVER (PARTITION BY seller_id)
        THEN 1 ELSE 0
    END AS u12m

FROM base
ORDER BY seller_id, safra
""")

df_temp_01.createOrReplaceTempView("df_temp_01")


# In[258]:


# Definição das variáveis
coluna_chave = "seller_id"
colunas_flags = ['u1m', 'u3m', 'u6m', 'u9m', 'u12m']

# Lista de colunas de valores
colunas_valores = [
    'review_score'
]

# Configuração dos indicadores
"""indicadores_config = {
    'IND_PDD': {'alias': 'PDD', 'valores': ['S']},
    'IND_WO': {'alias': 'WO', 'valores': ['R']},
    'IND_PCCR': {'alias': 'PCCR', 'valores': ['W']},
    'IND_ACA': {'alias': 'ACA', 'valores': ['N']},
    'IND_PRIMEIRA_FAT': {'alias': 'PRIM_FAT', 'valores': ['S']},
    'IND_FRAUDE': {'alias': 'FRAUDE', 'valores': ['N']}
}"""



def gerar_sql_dinamico():
    #selects = ["seller_id", "seller_region"]
    selects = ["seller_id"]
    
    # Agregações básicas
    for flag in colunas_flags:
        for valor in colunas_valores:
            selects.append(f"round(avg(case when {flag} = 1 then {valor} else NULL end), 2) as vl_med_{flag}_{valor}_reviews")
            selects.append(f"round(max(case when {flag} = 1 then {valor} else NULL end), 2) as vl_max_{flag}_{valor}_reviews")
            selects.append(f"round(min(case when {flag} = 1 then {valor} else NULL end), 2) as vl_min_{flag}_{valor}_reviews")
            selects.append(f"round(stddev(case when {flag} = 1 then {valor} else NULL end), 2) as vl_std_{flag}_{valor}_reviews")
            selects.append(f"round(count(case when {flag} = 1 then {valor} else NULL end), 2) as vl_qtd_{flag}_{valor}_reviews")
    # Agregações com indicadores
    """for indicador, info in indicadores_config.items():
        for valor_indicador in info['valores']:
            for flag in colunas_flags:
                for valor in colunas_valores:
                    alias = info['alias']
                    selects.append(f"round(avg(case when {flag} = 1 and {indicador} = '{valor_indicador}' then {valor} else NULL end), 2) as vl_med_{flag}_{alias}_{valor_indicador}_{valor}_reviews")
                    #selects.append(f"round(max(case when {flag} = 1 and {indicador} = '{valor_indicador}' then {valor} else NULL end), 2) as vl_max_{flag}_{alias}_{valor_indicador}_{valor}_reviews")
                    #selects.append(f"round(count(case when {flag} = 1 and {indicador} = '{valor_indicador}' then {valor} else NULL end), 2) as vl_qtd_{flag}_{alias}_{valor_indicador}_{valor}_reviews")"""
    
    sql_query = f"""
    SELECT
        {', '.join(selects)}
    FROM df_temp_01
    GROUP BY seller_id
    ORDER BY seller_id
    """
    
    return sql_query

# Executar SQL dinâmico
sql_dinamico = gerar_sql_dinamico()

df_temp_02 = spark.sql(sql_dinamico)

df_temp_02.createOrReplaceTempView("df_temp_02")
print(f"Shape: {df_temp_02.count()} linhas, {len(df_temp_02.columns)} colunas")


# In[259]:


def add_temporal_ratio_columns(
    df,
    base_prefix="vl_med",
    ratio_prefix="raz_med",
    windows=("u1m", "u3m", "u6m", "u9m", "u12m"),
    suffix="_reviews"
):
    """
    Função que mantém todas as colunas originais do DataFrame
    e adiciona novas colunas de razão temporal entre janelas consecutivas.

    Exemplo de razão criada:
    RAZ_MED_U1M_U3M_FAT_ATRASO = VL_MED_U1M_FAT_ATRASO / VL_MED_U3M_FAT_ATRASO
    """

    # Cria uma lista com todas as colunas originais do DataFrame
    # Isso garante que nenhuma coluna existente será removida
    base_cols = [F.col(c) for c in df.columns]

    # Lista que armazenará as expressões das novas colunas de razão
    ratio_exprs = []

    # Conjunto com os nomes das colunas do DataFrame
    # Usado para checar rapidamente se uma coluna existe
    df_cols = set(df.columns)

    # Gera pares de janelas consecutivas
    # Exemplo: (U1M, U3M), (U3M, U6M), ...
    window_pairs = list(zip(windows[:-1], windows[1:]))

    # Percorre cada par de janelas (numerador e denominador)
    for num_win, den_win in window_pairs:

        # Percorre todas as colunas do DataFrame
        for col_num in df.columns:

            # Verifica se a coluna pertence à janela do numerador
            # e segue o padrão: VL_MED__
            if not col_num.startswith(f"{base_prefix}_{num_win}_"):
                continue

            # Deriva o nome da coluna do denominador
            # Substitui a janela do numerador pela janela do denominador
            col_den = col_num.replace(
                f"{base_prefix}_{num_win}_",
                f"{base_prefix}_{den_win}_"
            )

            # Se a coluna do denominador não existir, ignora
            if col_den not in df_cols:
                continue

            # Extrai o nome da feature base
            # Remove prefixo (VL_MED__) e o sufixo (_ATRASO)
            feature = (
                col_num
                .replace(f"{base_prefix}_{num_win}_", "")
                .replace(suffix, "")
            )

            # Define o nome da nova coluna de razão temporal
            # Exemplo: RAZ_MED_U1M_U3M_FAT_ATRASO
            ratio_name = (
                f"{ratio_prefix}_{num_win}_{den_win}_"
                f"{feature}{suffix}"
            )

            # Cria a expressão da razão temporal
            # Realiza a divisão apenas quando o denominador é diferente de zero
            # Caso contrário, retorna NULL
            ratio_exprs.append(
                F.when(
                    F.col(col_den) != 0,
                    F.col(col_num) / F.col(col_den)
                ).alias(ratio_name)
            )

    # Retorna o DataFrame mantendo todas as colunas originais
    # e adicionando as novas colunas de razão temporal
    return df.select(*base_cols, *ratio_exprs)


# Aplica a função ao DataFrame anterior, gerando um novo DataFrame enriquecido
df_temp_03 = add_temporal_ratio_columns(df_temp_02)


df_temp_03.createOrReplaceTempView("df_temp_03")

print(f"Shape: {df_temp_03.count()} linhas, {len(df_temp_03.columns)} colunas")


# In[260]:


df_temp_04 = df_reviews_01.alias("t1") \
    .join(df_temp_03.alias("t2"), "seller_id", "left") \
    .withColumn("safra", lit(data_exec_inicial)) \
    .withColumn("datproc", lit(dthproc)) \
    .drop("review_creation_date") \
    .drop("review_score")

df_temp_04.createOrReplaceTempView("df_temp_04")


# In[261]:


spark.sql("""
-- 4) Calcula hash para idempotência (evita UPDATE sem mudança real)
CREATE OR REPLACE TEMP VIEW stage_reviews_final AS
SELECT
  *,
  sha2(concat_ws('||',
    coalesce(seller_id,''),
    coalesce(cast(safra as int),''),
    coalesce(cast(vl_med_u1m_review_score_reviews as int),'')
    --coalesce(product_id,''),
    --coalesce(seller_id,''),
    --coalesce(date_format(shipping_limit_date,'yyyy-MM-dd'),''),
    --coalesce(cast(price as string),''),
    --coalesce(cast(freight_value as string),'')
  ), 256) AS row_hash
FROM df_temp_04;
""")


# In[262]:


spark.sql("""
        CREATE DATABASE IF NOT EXISTS gold
        LOCATION 's3a://gold'
""")


# In[263]:


from delta.tables import DeltaTable

path = "s3a://gold/book_reviews"

if not spark.catalog.tableExists("gold.book_reviews"):
    
    # Se já existe Delta no storage → só registra
    if DeltaTable.isDeltaTable(spark, path):
        spark.sql(f"""
            CREATE TABLE gold.book_reviews
            USING DELTA
            LOCATION '{path}'
        """)
    
    # Se não existe nada → cria do zero
    else:
        spark.sql(f"""
            CREATE TABLE gold.book_reviews
            USING DELTA
            PARTITIONED BY (safra)
            LOCATION '{path}'
            AS SELECT * FROM stage_reviews_final WHERE 1=0
        """)


# In[264]:


spark.sql("""
MERGE INTO gold.book_reviews AS t
USING stage_reviews_final AS s
ON t.seller_id = s.seller_id
AND t.safra = s.safra

WHEN MATCHED AND (t.row_hash IS NULL OR t.row_hash <> s.row_hash) THEN
UPDATE SET *

WHEN NOT MATCHED THEN
INSERT *
;
""")


# In[265]:


logging.info("Finalizando criação do book: pedidos")


# In[266]:


spark.stop()

