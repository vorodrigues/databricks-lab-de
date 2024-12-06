# Databricks notebook source
# MAGIC %md # 02.02 - Engenharia de Dados

# COMMAND ----------

# MAGIC %md ## 1. Preparando o ambiente

# COMMAND ----------

# MAGIC %md ### 1.1. Adicionando parâmetros
# MAGIC
# MAGIC - **Execute** a célula abaixo
# MAGIC - **Customize** os valores dos parâmetros

# COMMAND ----------

dbutils.widgets.text("catalog", "my_catalog", "Catalog")
dbutils.widgets.text("database", "my_database", "Database")
dbutils.widgets.text("source_path", "/path/to/my/folder", "Source Path")
dbutils.widgets.text("project_path", "/path/to/my/folder", "Project Path")

# COMMAND ----------

# MAGIC %md ### 1.2. Carregando parâmetros
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")
source_path = dbutils.widgets.get("source_path")
project_path = dbutils.widgets.get("project_path")

# COMMAND ----------

# MAGIC %md ### 1.3. Criando catálogos e databases
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

# COMMAND ----------

# MAGIC %md ## 2. Visualizando a fonte de dados
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

display(spark.read.parquet(source_path).limit(10))

# COMMAND ----------

# MAGIC %md ## 3. Bronze: Ingestão incremental dos dados

# COMMAND ----------

# MAGIC %md ### Auto Loader
# MAGIC
# MAGIC Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage.
# MAGIC
# MAGIC Auto Loader provides a Structured Streaming source called cloudFiles. Given an input directory path on the cloud file storage, the cloudFiles source automatically processes new files as they arrive, with the option of also processing existing files in that directory.
# MAGIC
# MAGIC ![](https://databricks.com/wp-content/uploads/2020/02/autoloader.png)

# COMMAND ----------

# MAGIC %md ### Auto Optimize e File Size Tuning
# MAGIC
# MAGIC Auto Optimize is an optional set of features that automatically compact small files during individual writes to a Delta table. Paying a small cost during writes offers significant benefits for tables that are queried actively. It is particularly useful in the following scenarios:
# MAGIC
# MAGIC   - Streaming use cases where latency in the order of minutes is acceptable
# MAGIC   - MERGE INTO is the preferred method of writing into Delta Lake
# MAGIC   - CREATE TABLE AS SELECT or INSERT INTO are commonly used operations
# MAGIC
# MAGIC > **NOTE: Auto Optimize and File Size Tuning are enabled by default on all Managed Tables**
# MAGIC
# MAGIC ![](https://docs.databricks.com/_images/optimized-writes.png)

# COMMAND ----------

# MAGIC %md ### 3.1. Tabela Bronze
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

# Ingest data using Auto Loader.
bronzeDF = (spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.schemaLocation", project_path+"/schemas/bronze")
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  .option("cloudFiles.inferColumnTypes", True)
  .option("locale", "BR")
  .load(source_path))

# Write Stream as Delta Table
(bronzeDF.writeStream
  .option("checkpointLocation", project_path+"/checkpoints/bronze")
  .trigger(availableNow=True)
  .toTable(f"{catalog}.{database}.sales_bronze")
  .awaitTermination())

# COMMAND ----------

# MAGIC %md ## 4. Silver: Limpeza e padronização dos dados

# COMMAND ----------

# MAGIC %md ### Schema Enforcement ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC
# MAGIC Schema enforcement helps keep our tables clean and tidy so that we can trust the data we have stored in Delta Lake. The writes above were blocked because the schema of the new data did not match the schema of table (see the exception details).
# MAGIC
# MAGIC > **NOTE: Schema enforcement is enabled by default on all Delta Tables**

# COMMAND ----------

# MAGIC %md ### Constraints ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC Constraints help us avoid bad data to flow through our pipeline and to help us identify potential issues with our data.

# COMMAND ----------

# MAGIC %md ### Liquid Clustering ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC
# MAGIC Delta Lake liquid clustering replaces table partitioning and ZORDER to simplify data layout decisions and optimize query performance. Liquid clustering provides flexibility to redefine clustering keys without rewriting existing data, allowing data layout to evolve alongside analytic needs over time.

# COMMAND ----------

# MAGIC %md ### 4.1. Criando a tabela silver
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

if not spark.catalog.tableExists(f"{catalog}.{database}.sales"):

  df_silver = (spark.read
    .table(f"{catalog}.{database}.sales_bronze")
    .dropDuplicates(["sales_id"])
    .limit(0))

  (df_silver.write
    .option("delta.enableChangeDataFeed", "true")
    .clusterBy("date_key", "product_key", "store_key")
    .saveAsTable(f"{catalog}.{database}.sales_silver"))
  
  spark.sql(f"ALTER TABLE {catalog}.{database}.sales_silver CHANGE COLUMN sales_id SET NOT NULL")
  spark.sql(f"ALTER TABLE {catalog}.{database}.sales_silver ADD CONSTRAINT quantity CHECK (market_key IN ('US', 'BR'))")

# COMMAND ----------

# MAGIC %md ### Change Data Feed ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC
# MAGIC Change data feed allows Databricks to track row-level changes between versions of a Delta table. When enabled on a Delta table, the runtime records change events for all the data written into the table. This includes the row data along with metadata indicating whether the specified row was inserted, deleted, or updated.

# COMMAND ----------

# MAGIC %md ### DML Support ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC
# MAGIC Delta Lake brings full DML support to data lakes: `DELETE`, `UPDATE`, `MERGE INTO`
# MAGIC
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif" alt='Merge process' width=600/>
# MAGIC
# MAGIC
# MAGIC #### INSERT, UPDATE, DELETE or TRUNCATE with Delta Lake
# MAGIC
# MAGIC 2-step process: 
# MAGIC 1. Identify changes made to the source
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# MAGIC %md ### 4.2. Ingestão incremental da tabela silver
# MAGIC
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

def merge_delta(microbatch, table, merge_keys, ts_key = None):

  on_clause = " AND ".join([f"t.{key} = s.{key}" for key in merge_keys])

  # Deduplica registros dentro do microbatch e mantém somente o mais recente
  if ts_key:
    microbatch = microbatch.orderBy(ts_key, ascending=False).dropDuplicates(merge_keys)
  
  # Caso a tabela já exista, os dados serão atualizados com MERGE
  microbatch.createOrReplaceTempView("microbatch")
  microbatch.sparkSession.sql(f"""
    MERGE INTO {table} t
    USING microbatch s
    ON {on_clause}
    -- WHEN MATCHED AND s.op_code = 'd' THEN DELETE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

df_silver = spark.readStream.table(f"{catalog}.{database}.sales_bronze")

(df_silver.writeStream
  .outputMode("update")
  .option("checkpointLocation", project_path+"/checkpoints/silver")
  .trigger(availableNow=True)
  .foreachBatch(lambda microbatch, x: merge_delta(microbatch, f"{catalog}.{database}.sales_silver", ["sales_id"], "date_key"))
  .start()
  .awaitTermination())

# COMMAND ----------

# MAGIC %md ## 5. Gold: Visões de negócio

# COMMAND ----------

# MAGIC %md ### Primary / Foreign Keys
# MAGIC
# MAGIC Primary Keys (PKs) and Foreign Keys (FKs) are essential elements in relational databases, acting as fundamental building blocks for data modeling. They provide information about the data relationships in the schema to users, tools and applications; and enable optimizations that leverage constraints to speed up queries.
# MAGIC
# MAGIC And you can visualize this information and the relationships between tables with the **Entity Relationship Diagram** in Catalog Explorer. Below is an example of a table purchases referencing two tables, users and products:
# MAGIC
# MAGIC <img src="https://www.databricks.com/sites/default/files/inline-images/db-960-blog-img-2.png?v=1720429389">

# COMMAND ----------

# MAGIC %md ### Comments / Tags
# MAGIC
# MAGIC Comments can help you and other users find and manage the data and AI assets you need by providing a metadata field for annotating your securable objects.
# MAGIC
# MAGIC Tags are attributes that include keys and optional values that you can use to organize and categorize securable objects in Unity Catalog. Using tags also simplifies the search and discovery of tables and views using the workspace search functionality.

# COMMAND ----------

# MAGIC %md ### 5.1. Dimensão de Produtos
# MAGIC
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

if not spark.catalog.tableExists(f"{catalog}.{database}.dim_product"):
  
  spark.sql(f"""
    CREATE TABLE {catalog}.{database}.dim_product (
      product_id BIGINT NOT NULL COMMENT 'Product identifier',
      supplier STRING COMMENT 'Product supplier',
      product STRING COMMENT 'Product name',
      upc STRING COMMENT 'UPC / GTIN-13 code of the product. 13 digits. No check digit.',
      CONSTRAINT dim_product_pk PRIMARY KEY (product_id)
    )
    COMMENT 'Product dimension table'
    CLUSTER BY (product_id)
  """)

# COMMAND ----------

prod_df = (spark.readStream
  .option("readChangeFeed", "true")
  .option("withEventTimeOrder", "true")
  .table(f"{catalog}.{database}.sales_silver")
  .withColumn("ts", col("date_key").cast("timestamp"))
  .withWatermark("ts", "10 seconds")
  .select("product_id", "supplier", "product", "upc")
  .distinct()
)

(prod_df.writeStream
  .outputMode("update")
  .option("checkpointLocation", project_path+"/checkpoints/dim_product")
  .trigger(availableNow=True)
  .foreachBatch(lambda microbatch, x: merge_delta(microbatch, f"{catalog}.{database}.dim_product", ["product_id"]))
  .start()
  .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vr_demo.handson_de.dim_product

# COMMAND ----------

# MAGIC %md ### 5.2. Dimensão de Lojas
# MAGIC
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

if not spark.catalog.tableExists(f"{catalog}.{database}.dim_store"):
  
  spark.sql(f"""
    CREATE TABLE {catalog}.{database}.dim_store (
      store_id BIGINT NOT NULL COMMENT 'Store identifier',
      retailer STRING COMMENT 'Retailer name',
      store STRING COMMENT 'Store name',
      store_type STRING COMMENT 'Type of store',
      store_zip STRING COMMENT 'Store zip/postal code',
      store_lat_long STRING COMMENT 'Store latitude and longitude',
      CONSTRAINT dim_store_pk PRIMARY KEY (store_id)
    )
    COMMENT 'Store dimension table'
    CLUSTER BY (store_id)
  """)

# COMMAND ----------

store_df = (spark.readStream
  .option("readChangeFeed", "true")
  .option("withEventTimeOrder", "true")
  .table(f"{catalog}.{database}.sales_silver")
  .withColumn("ts", col("date_key").cast("timestamp"))
  .withWatermark("ts", "10 seconds")
  .select("store_id", "retailer", "store", "store_type", "store_zip", "store_lat_long")
  .distinct()
)

(store_df.writeStream
  .outputMode("update")
  .option("checkpointLocation", project_path+"/checkpoints/dim_store")
  .trigger(availableNow=True)
  .foreachBatch(lambda microbatch, x: merge_delta(microbatch, f"{catalog}.{database}.dim_store", ["store_id"]))
  .start()
  .awaitTermination())

# COMMAND ----------

# MAGIC %md ### 5.3. Fato de Vendas
# MAGIC
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

if not spark.catalog.tableExists(f"{catalog}.{database}.sales_gold"):
  
  spark.sql(f"""
    CREATE TABLE {catalog}.{database}.sales_gold (
      sales_id BIGINT NOT NULL COMMENT 'Store identifier',
      product_id BIGINT COMMENT 'Product identifier',
      store_id BIGINT COMMENT 'Store identifier',
      date_key DATE COMMENT 'Date of sale',
      sales_quantity DECIMAL(10,0) COMMENT 'Quantity sold (units)',
      sales_amount DOUBLE COMMENT 'Amount sold (USD)',
      CONSTRAINT sales_gold_pk PRIMARY KEY (sales_id),
      CONSTRAINT sales_product_fk FOREIGN KEY (product_id) REFERENCES {catalog}.{database}.dim_product(product_id),
      CONSTRAINT sales_store_fk FOREIGN KEY (store_id) REFERENCES {catalog}.{database}.dim_store(store_id)
    )
    COMMENT 'Sales fact table'
    CLUSTER BY (store_id)
  """)

# COMMAND ----------

gold_df = (spark.readStream
  .option("readChangeFeed", "true")
  .table(f"{catalog}.{database}.sales_silver")
  .select("sales_id", "product_id", "store_id", "date_key", "sales_quantity", "sales_amount")
)

(gold_df.writeStream
  .outputMode("update")
  .option("checkpointLocation", project_path+"/checkpoints/sales_gold")
  .trigger(availableNow=True)
  .foreachBatch(lambda microbatch, x: merge_delta(microbatch, f"{catalog}.{database}.sales_gold", ["sales_id"], "date_key"))
  .start()
  .awaitTermination())

# COMMAND ----------

# MAGIC %md ## 6. Otimização dos dados

# COMMAND ----------

# MAGIC %md ### Vacuum ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC
# MAGIC VACUUM removes all files from the table directory that are not managed by Delta, as well as data files that are no longer in the latest state of the transaction log for the table and are older than a retention threshold. 
# MAGIC
# MAGIC `VACUUM table_name`

# COMMAND ----------

# MAGIC %md ### Optimize ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC
# MAGIC Optimizes the layout of Delta Lake data. Optionally optimize a subset of data or collocate data by column. If you do not specify collocation and the table is not defined with liquid clustering, bin-packing optimization is performed.
# MAGIC
# MAGIC `OPTIMIZE table_name`

# COMMAND ----------

# MAGIC %md ### Analyze ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png)
# MAGIC
# MAGIC The ANALYZE TABLE statement collects estimated statistics about a specific table or all tables in a specified schema. These statistics are used by the query optimizer to generate an optimal query plan.
# MAGIC
# MAGIC `ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS`<br>
# MAGIC `ANALYZE TABLES IN schema_name COMPUTE STATISTICS`

# COMMAND ----------

# MAGIC %md ### Predictive Optimization
# MAGIC
# MAGIC Predictive optimization removes the need to manually manage maintenance operations for Unity Catalog managed tables on Databricks.
# MAGIC
# MAGIC With predictive optimization enabled, Databricks automatically identifies tables that would benefit from maintenance operations and runs them for the user. Maintenance operations are only run as necessary, eliminating both unnecessary runs for maintenance operations and the burden associated with tracking and troubleshooting performance.
# MAGIC
# MAGIC <br><img src="https://www.databricks.com/sites/default/files/2024-05/db-976-blog-img-og.png?v=1717158571" width="60%"><br><br>

# COMMAND ----------

# MAGIC %md ### 6.1. Otimizando dados do nosso banco de dados
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

spark.sql(f"ALTER DATABASE {catalog}.{database} ENABLE PREDICTIVE OPTIMIZATION")

# COMMAND ----------

# MAGIC %md ## 7. Governança dos dados

# COMMAND ----------

# MAGIC %md ### 7.1. Controle de acesso (ACLs)
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

spark.sql(f"GRANT USAGE ON CATALOG {catalog} TO `<user/group/service_principal>`")
spark.sql(f"GRANT USAGE ON DATABASE {catalog}.{database} TO `<user/group/service_principal>`")
spark.sql(f"GRANT SELECT ON DATABASE {catalog}.{database}.sales_gold TO `<user/group/service_principal>`")
spark.sql(f"GRANT SELECT ON DATABASE {catalog}.{database}.dim_product TO `<user/group/service_principal>`")
spark.sql(f"GRANT SELECT ON DATABASE {catalog}.{database}.dim_store TO `<user/group/service_principal>`")

# COMMAND ----------

# MAGIC %md ### 7.2. Column-Level Masking
# MAGIC
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {catalog}.{database}.mask_column_with_hash(col STRING)
  RETURN CASE WHEN is_member('<group>') THEN col ELSE SHA2(col, 256) END
""")

# COMMAND ----------

spark.sql(f"ALTER TABLE {catalog}.{database}.sales_gold ALTER COLUMN sales_id SET MASK {catalog}.{database}.mask_column_with_hash")

# COMMAND ----------

# MAGIC %md ### 7.3. Row-Level Security
# MAGIC
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

spark.sql(f"""
  CREATE FUNCTION {catalog}.{database}.supplier_row_filter(col STRING)
  RETURN IF(is_member('<group>'), true, col='SUPPLIER 08')
""")

# COMMAND ----------

spark.sql(f"ALTER TABLE {catalog}.{database}.dim_product SET ROW FILTER {catalog}.{database}.supplier_row_filter ON (supplier)")
