# Databricks notebook source
# MAGIC %md # 02.1 - Introdução

# COMMAND ----------

# MAGIC %md ## 1. Configuração do Notebook
# MAGIC

# COMMAND ----------

# MAGIC %md ### 1.1. Git Folder
# MAGIC
# MAGIC - Configure suas **credenciais** do Git [[AWS](https://docs.databricks.com/en/repos/repos-setup.html#add-or-edit-git-credentials-in-databricks)]
# MAGIC - Conecte um **repositório** [[AWS](https://docs.databricks.com/en/repos/git-operations-with-repos.html#clone-a-repo-connected-to-a-remote-git-repository)]

# COMMAND ----------

# MAGIC %md ### 1.2. Linguagens
# MAGIC
# MAGIC - No canto superior esquerdo, selecione a linguagem de programação **padrão** desejada
# MAGIC - Em cada célula, no seu canto superior direito, selecione a linguagem de programação **local** quando necessário

# COMMAND ----------

# MAGIC %md ### 1.3. Compute
# MAGIC
# MAGIC - No canto superior direito, selecione o **compute** desejado

# COMMAND ----------

# MAGIC %md ### 1.4. Navegação
# MAGIC
# MAGIC - Explore a interface do notebook para se familiarizar com os demais recursos como:
# MAGIC     - Tabela de conteúdos
# MAGIC     - Explorador do workspace
# MAGIC     - Navegador do catálogo
# MAGIC     - Comentários
# MAGIC     - Versionamento
# MAGIC     - Agendamento
# MAGIC     - Permissões
# MAGIC     - Etc...

# COMMAND ----------

# MAGIC %md ### 1.5. Parameters
# MAGIC
# MAGIC - Clique em **Edit > Add Parameter**
# MAGIC - Em **Parameter name**, digite `text`
# MAGIC - Em **Default parameter value**, digite `Hello World!` (ou algo mais criativo)
# MAGIC - **Execute** a célula abaixo

# COMMAND ----------

text = dbutils.widgets.get('text')
print(text)

# COMMAND ----------

# MAGIC %md ### 1.6. Secrets
# MAGIC
# MAGIC - Customize o **`scope_name`** com as suas iniciais
# MAGIC - **Execute** as células abaixo

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

scope_name = 'my_data_source'
key_name = 'access_token'

w.secrets.create_scope(scope=scope_name)
w.secrets.put_secret(scope=scope_name, key=key_name, string_value='my-token')

# COMMAND ----------

token = dbutils.secrets.get(scope=scope_name, key=key_name)
print(token)

# COMMAND ----------

w.secrets.delete_scope(scope=scope_name)

# COMMAND ----------

# MAGIC %md ## 2. Usando o Databricks Assistant

# COMMAND ----------

# MAGIC %md ### 2.1. Geração de código
# MAGIC
# MAGIC - Entre no modo de **edição** na célula abaixo
# MAGIC - Pressione **`Ctrl+I`** para ativar o assistente
# MAGIC - Faça o seguinte **pedido** ao assistente:
# MAGIC
# MAGIC ```
# MAGIC Leia um diretório do S3 contendo arquivos Parquet
# MAGIC ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### 2.2. Transcrição
# MAGIC
# MAGIC - Entre no modo de **edição** na célula abaixo
# MAGIC - Pressione **`Ctrl+I`** para ativar o assistente
# MAGIC - Faça o seguinte **pedido** ao assistente:
# MAGIC
# MAGIC ```
# MAGIC Converta para PySpark
# MAGIC ```

# COMMAND ----------

CREATE PROCEDURE remove_emp (employee_id NUMBER) AS
   tot_emps NUMBER;
   BEGIN
      DELETE FROM employees
      WHERE employees.employee_id = remove_emp.employee_id;
   tot_emps := tot_emps - 1;
   END;

# COMMAND ----------

# MAGIC %md ### 2.3. Diagnóstico
# MAGIC
# MAGIC - **Execute** a célula abaixo
# MAGIC - Caso o assistente não sugira uma correção automaticamente, clique em **`Diagnose error`**

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_trunc('day', pickup_time) as pickup_day, count(*) as cnt from samples.nyctaxi.trips group by all

# COMMAND ----------

# MAGIC %md Além disso, o assistente pode ser utilizado para:
# MAGIC
# MAGIC - Otimizar o código (`/optimize`)
# MAGIC - Explicar o código (`/explain`)
# MAGIC - Documentação automática (`/doc`)
# MAGIC - Formatação automática (`/prettify`)
# MAGIC - Entre outros...
