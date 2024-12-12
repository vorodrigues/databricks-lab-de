# Databricks notebook source
# MAGIC %md # 02.08 - Conexões JDBC

# COMMAND ----------

# MAGIC %md ## 1. Preparando o ambiente

# COMMAND ----------

# MAGIC %md ### 1.1. Carregando valores dos secrets

# COMMAND ----------

scope = "my-scope"
hostname = dbutils.secrets.get(scope, "hostname")
port = dbutils.secrets.get(scope, "port")
database = dbutils.secrets.get(scope, "database")
username = dbutils.secrets.get(scope, "username")
password = dbutils.secrets.get(scope, "password")

# COMMAND ----------

# MAGIC %md ### 1.1. Definindo as variáveis
# MAGIC
# MAGIC - **Execute** a célula abaixo
# MAGIC - **Customize** os valores dos variáveis

# COMMAND ----------

url = f"jdbc:mysql://{hostname}:{port}/{database}?user={username}&password={password}"
read_table = "read-table"
write_table = "write-table"
databricks_table = "catalog.database.table"

# COMMAND ----------

# MAGIC %md ## 2. Leitura

# COMMAND ----------

# MAGIC %md ### 2.1. Lendo uma tabela

# COMMAND ----------

df = (spark.read 
  .format("jdbc") 
  .option("url", url) 
  .option("dbtable", read_table)
  .load()
)

display(df)

# COMMAND ----------

# MAGIC %md ### 2.2. Push down de consultas

# COMMAND ----------

df = (spark.read 
  .format("jdbc") 
  .option("url", url) 
  .option("dbtable", f"(select date_key, sum(sales_amount) as sales_amount from {read_table} group by date_key) as q") 
  .load()
)

display(df)

# COMMAND ----------

# MAGIC %md ### 2.3. Controlando o número de linhas lidas simultaneamente

# COMMAND ----------

df = (spark.read 
  .format("jdbc") 
  .option("url", url) 
  .option("dbtable", read_table)
  .option("fetchSize", "10000")
  .load()
)

display(df)

# COMMAND ----------

# MAGIC %md ### 2.4. Controlando o paralelismo da leitura

# COMMAND ----------

min = (spark.read 
  .format("jdbc") 
  .option("url", url) 
  .option("dbtable", f"(select min(sales_id) as sales_id from {read_table}) as q") 
  .load()
).collect()[0]["sales_id"]

max = (spark.read 
  .format("jdbc") 
  .option("url", url) 
  .option("dbtable", f"(select max(sales_id) as sales_id from {read_table}) as q") 
  .load()
).collect()[0]["sales_id"]

print(f"min: {min}, max: {max}")

# COMMAND ----------

df = (spark.read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", read_table)
  .option("fetchSize", "10000")
  # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
  .option("partitionColumn", "sales_id")
  # lowest value to pull data for with the partitionColumn
  .option("lowerBound", min)
  # max value to pull data for with the partitionColumn
  .option("upperBound", max)
  # number of partitions to distribute the data into. Do not set this very large (~hundreds)
  .option("numPartitions", 8)
  .load()
)

display(df)

# COMMAND ----------

# MAGIC %md ## 3. Escrita

# COMMAND ----------

# MAGIC %md ### 3.1. Escrevendo para uma tabela

# COMMAND ----------

df = spark.table(databricks_table).limit(int(1e6))

(df.write
  .format("jdbc")
  .option("url", url)
  .option("dbtable", write_table)
  .mode("overwrite")
  .save()
)

# COMMAND ----------

# MAGIC %md ### 3.2. Controlando o número de linhas escritas simultaneamente

# COMMAND ----------

(df.write
  .format("jdbc")
  .option("url", url)
  .option("dbtable", write_table)
  .option("batchSize", "10000")
  .mode("overwrite")
  .save()
)

# COMMAND ----------

# MAGIC %md ### 3.3. Controlando o paralelismo da escrita

# COMMAND ----------

(df.write
  .format("jdbc")
  .option("url", url)
  .option("dbtable", write_table)
  .option("batchSize", "100000")
  .option("numPartitions", 10)
  .mode("overwrite")
  .save()
)
