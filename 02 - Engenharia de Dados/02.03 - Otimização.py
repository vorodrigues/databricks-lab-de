# Databricks notebook source
# MAGIC %md # 02.03 - Otimização

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

# COMMAND ----------

# MAGIC %md ### 1.2. Carregando parâmetros
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")

# COMMAND ----------

# MAGIC %md ## 2. Otimização

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

# MAGIC %md ### 2.1. Otimizando o nosso banco de dados
# MAGIC
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

spark.sql(f"ALTER DATABASE {catalog}.{database} ENABLE PREDICTIVE OPTIMIZATION")
