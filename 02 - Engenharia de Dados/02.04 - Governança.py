# Databricks notebook source
# MAGIC %md # 02.04. Governança

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

# MAGIC %md ## 2. Governança

# COMMAND ----------

# MAGIC %md ### 2.1. Controle de acesso (ACLs)
# MAGIC
# MAGIC - **Customize** a variável `principal`
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

principal = "<my_principal>" # can be a user, group, or service principal
spark.sql(f"GRANT USAGE ON CATALOG {catalog} TO `{principal}`")
spark.sql(f"GRANT USAGE ON DATABASE {catalog}.{database} TO `{principal}`")
spark.sql(f"GRANT SELECT ON TABLE {catalog}.{database}.sales_gold TO `{principal}`")
spark.sql(f"GRANT SELECT ON TABLE {catalog}.{database}.dim_product TO `{principal}`")
spark.sql(f"GRANT SELECT ON TABLE {catalog}.{database}.dim_store TO `{principal}`")

# COMMAND ----------

# MAGIC %md ### 2.2. Column-Level Masking
# MAGIC
# MAGIC - **Customize** a variável `clm_principal`
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

clm_principal = "my_principal"
spark.sql(f"""
  CREATE OR REPLACE FUNCTION {catalog}.{database}.mask_column_with_hash(col STRING)
  RETURN CASE WHEN is_member('{clm_principal}') THEN col ELSE SHA2(col, 256) END
""")

# COMMAND ----------

spark.sql(f"ALTER TABLE {catalog}.{database}.sales_gold ALTER COLUMN sales_id SET MASK {catalog}.{database}.mask_column_with_hash")

# COMMAND ----------

# MAGIC %md ### 2.3. Row-Level Security
# MAGIC
# MAGIC - **Customize** a variável `rls_principal`
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

rls_principal = "my_principal"
spark.sql(f"""
  CREATE OR REPLACE FUNCTION {catalog}.{database}.supplier_row_filter(col STRING)
  RETURN IF(is_member('{rls_principal}'), true, col='SUPPLIER 08')
""")

# COMMAND ----------

spark.sql(f"ALTER TABLE {catalog}.{database}.dim_product SET ROW FILTER {catalog}.{database}.supplier_row_filter ON (supplier)")
