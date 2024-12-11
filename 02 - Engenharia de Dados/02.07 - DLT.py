# Databricks notebook source
# MAGIC %md # 02.07 - Delta Live Tables

# COMMAND ----------

# MAGIC %md ## 1. Preparando o ambiente

# COMMAND ----------

catalog = spark.conf.get("pipelines.catalog")
database = spark.conf.get("pipelines.schema")
source_path = spark.conf.get("source_path")

# COMMAND ----------

# MAGIC %md ## 2. Camada Bronze

# COMMAND ----------

import dlt

@dlt.table()
def dlt_sales_bronze():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("locale", "BR")
    .load(source_path))

# COMMAND ----------

# MAGIC %md ## 3. Camada Silver

# COMMAND ----------

from pyspark.sql.functions import col, expr

dlt.create_streaming_table(
  name=f"{catalog}.{database}.dlt_sales_silver",
  cluster_by=["date_key", "product_key", "store_key"],
  table_properties={"delta.enableChangeDataFeed" : "true"},
  expect_all={
    "valid_sales_id": "sales_id IS NOT NULL", 
    "valid_market_key": "market_key IN ('US', 'BR')"
  }
)

dlt.apply_changes(
  target = f"{catalog}.{database}.dlt_sales_silver",
  source = "dlt_sales_bronze",
  keys = ["sales_id"],
  sequence_by = col("date_key"),
  # apply_as_deletes = expr("operation = 'DELETE'"),
  # apply_as_truncates = expr("operation = 'TRUNCATE'"),
  except_column_list=["_rescued_data"],
  stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md ## 4. Camada Gold

# COMMAND ----------

# MAGIC %md ### 4.1. Dimensão de Produtos

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

@dlt.view()
def dlt_dim_product_feed():
  return (spark.readStream
    .option("readChangeFeed", "true")
    .option("withEventTimeOrder", "true")
    .table(f"{catalog}.{database}.dlt_sales_silver")
    .withColumn("ts", col("date_key").cast("timestamp"))
    .withWatermark("ts", "10 seconds")
    .select("product_id", "supplier", "product", "upc", "date_key")
  )
  
dlt.create_streaming_table(
  name="dlt_dim_product",
  cluster_by=["product_id"],
  expect_all={
    "valid_product_id": "product_id IS NOT NULL"
  },
  schema="""
    product_id BIGINT NOT NULL COMMENT 'Product identifier',
    supplier STRING COMMENT 'Product supplier',
    product STRING COMMENT 'Product name',
    upc STRING COMMENT 'UPC / GTIN-13 code of the product. 13 digits. No check digit.',
    CONSTRAINT dlt_dim_product_pk PRIMARY KEY (product_id)
  """,
  comment="Product dimension table"
)

dlt.apply_changes(
  target = "dlt_dim_product",
  source = "dlt_dim_product_feed",
  keys = ["product_id"],
  sequence_by = col("date_key"),
  # apply_as_deletes = expr("operation = 'DELETE'"),
  # apply_as_truncates = expr("operation = 'TRUNCATE'"),
  except_column_list=["date_key"],
  stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md ### 4.2. Dimensão de Lojas

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

@dlt.view()
def dlt_dim_store_feed():
  return (spark.readStream
    .option("readChangeFeed", "true")
    .option("withEventTimeOrder", "true")
    .table(f"{catalog}.{database}.dlt_sales_silver")
    .withColumn("ts", col("date_key").cast("timestamp"))
    .withWatermark("ts", "10 seconds")
    .select("store_id", "retailer", "store", "store_type", "store_zip", "store_lat_long", "date_key")
  )
  
dlt.create_streaming_table(
  name="dlt_dim_store",
  cluster_by=["store_id"],
  expect_all={
    "valid_store_id": "store_id IS NOT NULL"
  },
  schema="""
    store_id BIGINT NOT NULL COMMENT 'Store identifier',
    retailer STRING COMMENT 'Retailer name',
    store STRING COMMENT 'Store name',
    store_type STRING COMMENT 'Type of store',
    store_zip STRING COMMENT 'Store zip/postal code',
    store_lat_long STRING COMMENT 'Store latitude and longitude',
    CONSTRAINT dlt_dim_store_pk PRIMARY KEY (store_id)
  """,
  comment="Store dimension table"
)

dlt.apply_changes(
  target = "dlt_dim_store",
  source = "dlt_dim_store_feed",
  keys = ["store_id"],
  sequence_by = col("date_key"),
  # apply_as_deletes = expr("operation = 'DELETE'"),
  # apply_as_truncates = expr("operation = 'TRUNCATE'"),
  except_column_list=["date_key"],
  stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md ### 4.3. Fato de Vendas

# COMMAND ----------

@dlt.view()
def dlt_sales_gold_feed():
  return (spark.readStream
    .option("readChangeFeed", "true")
    .table(f"{catalog}.{database}.dlt_sales_silver")
    .select("sales_id", "product_id", "store_id", "date_key", "sales_quantity", "sales_amount")
  )
  
dlt.create_streaming_table(
  name="dlt_sales_gold",
  cluster_by=["date_key", "product_id", "store_id"],
  expect_all={
    "valid_sales_id": "sales_id IS NOT NULL"
  },
  schema=f"""
    sales_id STRING NOT NULL COMMENT 'Store identifier',
    product_id BIGINT COMMENT 'Product identifier',
    store_id BIGINT COMMENT 'Store identifier',
    date_key DATE COMMENT 'Date of sale',
    sales_quantity DECIMAL(10,0) COMMENT 'Quantity sold (units)',
    sales_amount DOUBLE COMMENT 'Amount sold (USD)',
    CONSTRAINT dlt_sales_gold_pk PRIMARY KEY (sales_id),
    CONSTRAINT dlt_sales_product_fk FOREIGN KEY (product_id) REFERENCES {catalog}.{database}.dim_product(product_id),
    CONSTRAINT dlt_sales_store_fk FOREIGN KEY (store_id) REFERENCES {catalog}.{database}.dim_store(store_id)
  """,
  comment="Sales fact table"
)

dlt.apply_changes(
  target = "dlt_sales_gold",
  source = "dlt_sales_gold_feed",
  keys = ["sales_id"],
  sequence_by = col("date_key"),
  # apply_as_deletes = expr("operation = 'DELETE'"),
  # apply_as_truncates = expr("operation = 'TRUNCATE'"),
  stored_as_scd_type = 1
)
