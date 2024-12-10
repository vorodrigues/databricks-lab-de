# Databricks notebook source
# MAGIC %md # 02.00 - Setup

# COMMAND ----------

catalog = "my_catalog"
database = "my_database"
project_path = "/path/to/my/folder"
source_table = "my_crisp_sales_table"

# COMMAND ----------

# spark.sql(f"drop database {catalog}.{schema} cascade")
# dbutils.fs.rm(project_path+"/schemas", True)
# dbutils.fs.rm(project_path+"/checkpoints", True)

# COMMAND ----------

# dbutils.fs.rm(project_path+"/sales/parquet", True)

# COMMAND ----------

spark.table(source_table).limit(int(50e6)).write.mode("overwrite").parquet(project_path+"/sales/parquet")
