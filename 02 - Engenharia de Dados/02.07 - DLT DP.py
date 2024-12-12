# Databricks notebook source
# MAGIC %md-sandbox # 02.07 - Delta Live Tables
# MAGIC
# MAGIC DLT makes Data Engineering accessible for all. Just declare your transformations in SQL or Python, and DLT will handle the Data Engineering complexity for you.
# MAGIC
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-1.png" width="700"/>
# MAGIC
# MAGIC **Accelerate ETL development** <br/>
# MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC
# MAGIC **Remove operational complexity** <br/>
# MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC
# MAGIC **Trust your data** <br/>
# MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC
# MAGIC **Simplify batch and streaming** <br/>
# MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC
# MAGIC ## Our Delta Live Table pipeline
# MAGIC
# MAGIC We'll be using as input a raw dataset containing information on our customers Loan and historical transactions. 
# MAGIC
# MAGIC Our goal is to ingest this data in near real time and build table for our Analyst team while ensuring data quality.
# MAGIC
# MAGIC **Your DLT Pipeline is ready!** Your pipeline was started using this notebook and is <a dbdemos-pipeline-id="dlt-loans" href="/#joblist/pipelines/460f840c-9ecc-4d19-a661-f60fd3a88297">available here</a>.
# MAGIC

# COMMAND ----------

# MAGIC %md ## 1. Preparando o ambiente

# COMMAND ----------

catalog = spark.conf.get("pipelines.catalog")
database = spark.conf.get("pipelines.schema")
source_path = spark.conf.get("source_path")

# COMMAND ----------

# MAGIC %md-sandbox ## 2. Camada Bronze
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-2.png" width="600"/>
# MAGIC
# MAGIC Our raw data is being sent to a blob storage. 
# MAGIC
# MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files. 
# MAGIC
# MAGIC Autoloader is available in SQL using the `cloud_files` function and can be used with a variety of format (json, csv, avro...):
# MAGIC
# MAGIC For more detail on Autoloader, you can see `dbdemos.install('auto-loader')`
# MAGIC
# MAGIC #### STREAMING LIVE TABLE 
# MAGIC Defining tables as `STREAMING` will guarantee that you only consume new incoming data. Without `STREAMING`, you will scan and ingest all the data available at once. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-incremental-data.html) for more details

# COMMAND ----------

import dlt

@dlt.table()
def dlt_sales_bronze():
  return (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("locale", "BR")
    .load(source_path))

# COMMAND ----------

# MAGIC %md-sandbox ## 3. Camada Silver
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-3.png" width="600"/>
# MAGIC
# MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the `LIVE` spacename. 
# MAGIC
# MAGIC To consume only increment from the Bronze layer like `BZ_raw_txs`, we'll be using the `stream` keyworkd: `stream(LIVE.BZ_raw_txs)`
# MAGIC
# MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
# MAGIC
# MAGIC #### Expectations
# MAGIC By defining expectations (`CONSTRAINT <name> EXPECT <condition>`), you can enforce and track your data quality. See the [documentation](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html) for more details

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

# MAGIC %md-sandbox ## 4. Camada Gold
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/dlt-golden-demo-loan-4.png" width="600"/>
# MAGIC
# MAGIC Our last step is to materialize the Gold Layer.
# MAGIC
# MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using `cluster_by`, and DLT will handle the rest.

# COMMAND ----------

# MAGIC %md ### 4.1. Dimensão de Produtos

# COMMAND ----------

@dlt.view()
def dlt_dim_product_feed():
  return (spark.readStream
    .option("readChangeFeed", "true")
    .table(f"{catalog}.{database}.dlt_sales_silver")
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
  except_column_list=["date_key"],
  stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md ### 4.2. Dimensão de Lojas

# COMMAND ----------

@dlt.view()
def dlt_dim_store_feed():
  return (spark.readStream
    .option("readChangeFeed", "true")
    .table(f"{catalog}.{database}.dlt_sales_silver")
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
  stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md ## 5. Criando um pipeline
# MAGIC
# MAGIC - No menu principal à esquerda, clique em **Workflows**
# MAGIC - Clique em **Pipelines**
# MAGIC - Clique em **Create pipeline**
# MAGIC - **Configure** o pipeline:
# MAGIC     - **General:**
# MAGIC         - **Serverless:** ativado
# MAGIC         - **Pipeline mode:** Triggered
# MAGIC     - **Source code:** 
# MAGIC         - **Paths:** selecione o notebook `02.07 - DLT`
# MAGIC     - **Destination:** 
# MAGIC         - **Direct publishing mode:** ativado
# MAGIC         - **Storage options:** Unity Catalog
# MAGIC         - **Catalog:** mesmo catálogo que estamos utilizando
# MAGIC         - **Schema:** mesmo database que estamos utilizando
# MAGIC     - **Advanced:**
# MAGIC         - **Configuration:**
# MAGIC             - source_path : {caminho-dados}
# MAGIC         - **Channel:** Preview
# MAGIC - Clique em **Create**

# COMMAND ----------

# MAGIC %md ## 6. Executando um pipeline
# MAGIC
# MAGIC - **Valide** que o modo de execução está em `Development`
# MAGIC - Clique em **Start**
# MAGIC - **Acompanhe** a execução do pipeline
# MAGIC     - Note que o DLT paraleliza automaticamente a execução das tarefas quando possível
