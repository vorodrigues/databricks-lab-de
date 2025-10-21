# Databricks notebook source
# MAGIC %md # 02.05 - Orquestração e Debugging

# COMMAND ----------

# MAGIC %md ## 1. Criando um Workflow
# MAGIC
# MAGIC - No menu principal à esquerda, clique em **Workflows**
# MAGIC - Clique em **Create job**
# MAGIC - Preencha as informações da tarefa:
# MAGIC     - **Task Name:** ingestao
# MAGIC     - **Path:** Selecione o notebook `02.02 - Ingestão e Trasformação`
# MAGIC     - **Compute:** Selecione o mesmo cluster que utilizamos no exercício anterior
# MAGIC     - Clique em **Create task**
# MAGIC - Clique no nome do job para **renomeá-lo**

# COMMAND ----------

# MAGIC %md ## 2. Adicionando novas tarefas
# MAGIC
# MAGIC **Otimização**
# MAGIC - Clique em **Add task**
# MAGIC - Clique em **Notebook**
# MAGIC - Preencha as informações da tarefa:
# MAGIC     - **Task name:** otimizacao
# MAGIC     - **Path:** Selecione o notebook `02.03 - Otimização`
# MAGIC     - **Compute:** Selecione o mesmo cluster que utilizamos no exercício anterior
# MAGIC     - Clique em **Create task**
# MAGIC
# MAGIC **Governança**
# MAGIC - Clique na tarefa `ingestao`
# MAGIC - Clique em **Add task**
# MAGIC - Clique em **Notebook**
# MAGIC - Preencha as informações da tarefa:
# MAGIC     - **Task name:** governanca
# MAGIC     - **Path:** Selecione o notebook `02.04 - Governança`
# MAGIC     - **Compute:** Selecione o mesmo cluster que utilizamos no exercício anterior
# MAGIC     - Clique em **Create task**
# MAGIC
# MAGIC **Task Values**
# MAGIC - Clique na tarefa `otimizacao`
# MAGIC - Clique em **Add task**
# MAGIC - Clique em **Notebook**
# MAGIC - Preencha as informações da tarefa:
# MAGIC     - **Task name:** task_values
# MAGIC     - **Path:** Selecione o notebook `02.06 - Task Values`
# MAGIC     - **Compute:** Selecione o mesmo cluster que utilizamos no exercício anterior
# MAGIC     - **Depends on:** Adicione a tarefa `governanca`
# MAGIC     - Clique em **Create task**

# COMMAND ----------

# MAGIC %md ## 2. Agendando um Workflow
# MAGIC
# MAGIC **Triggers:**
# MAGIC - No painel à direita, clique em **Add trigger**
# MAGIC - Em **Trigger Type**, selecione **Scheduled**
# MAGIC - Mantenha a configuração padrão: a cada 1 dia
# MAGIC - Por se tratar de um workshop, em **Trigger Status**, clique em **Paused**
# MAGIC - Clique em **Save**
# MAGIC
# MAGIC **Notificações:**
# MAGIC - No painel à direita, clique em **Edit notifications**
# MAGIC - Clique em **Add notification**
# MAGIC - Em **destination**, selecione seu email
# MAGIC - Clique em **Save**
# MAGIC
# MAGIC **Limites de tempo:**
# MAGIC - No painel à direita, clique em **Add metric thresholds**
# MAGIC - Em **Metric**, selecione **Run duration**
# MAGIC - Em **Warning threshold**, configure para **1 horas**
# MAGIC - Em **Timeout threshold**, configure para **2 horas**
# MAGIC - Clique em **Save**
# MAGIC
# MAGIC **Tags:**
# MAGIC - No painel à direita, clique em **Add tag** e adicione as configurações abaixo:
# MAGIC     - project : sales_report
# MAGIC
# MAGIC **Parâmetros:**
# MAGIC - No painel à direita, clique em **Edit parameters** e adicione as configurações abaixo:
# MAGIC     - catalog : {seu-catalogo}
# MAGIC     - database : {seu-database}
# MAGIC     - source_path : {caminho-dados}
# MAGIC     - project_path : {caminho-projeto}
# MAGIC
# MAGIC > **NOTA:** Observe que os parâmetros definidos no job são repassados a todas as tarefas
# MAGIC
# MAGIC > **NOTA:** Para produção, recomendamos fortemente a definição de um Service Principal como owner do job e que os times de desenvolvimento tenham apenas permissão de visualização / execução no mesmo

# COMMAND ----------

# MAGIC %md ## 3. Executando um Workflow
# MAGIC
# MAGIC - No canto superior direito, clique em **Run now**
# MAGIC - No canto superior esquerdo, clique em **Runs**
# MAGIC - Clique no **nome da execução** que foi iniciada
# MAGIC - Acompanhe a execução do notebook e observe os recursos para debugging
# MAGIC
# MAGIC > **NOTA:** Observem que o pipeline desenvolvido é idempotente: deverá completar com sucesso e produzir dados corretamente em todas as execuções (caso não haja nenhuma alteração, nada será feito; caso haja alguma alteração, ela será inserida, atualizada, deletada ou truncada conforme a fonte)

# COMMAND ----------

# MAGIC %md ## 4. Debugging e recuperação de jobs

# COMMAND ----------

# MAGIC %md ### 4.1. Inserindo um registro inválido
# MAGIC
# MAGIC - **Substitua** o valor da variável `source_path`
# MAGIC - **Execute** a célula abaixo para adicionar um novo arquivo com schema inválido
# MAGIC - **Execute** o job criado anteriormente
# MAGIC     - Deverá ocorrer um erro na camada silver

# COMMAND ----------

from pyspark.sql.functions import lit

source_path = "s3://one-env/vr/handson_de/sales/parquet"
dest_path = source_path+"/new"

(spark.read.parquet(source_path).limit(1)
  .withColumn("sales_id", lit(None).cast("string"))
  .write.mode("append").parquet(dest_path))

display(dbutils.fs.ls(dest_path))

# COMMAND ----------

# MAGIC %md ### 4.2. Reparo da execução
# MAGIC
# MAGIC Conforme podemos observar na mensagem de erro, quando uma coluna é alterada ou uma nova coluna é incluída, o Auto Loader irá atualizar o esquema da tabela e interromper a execução. Para reparar o pipeline, basta reexecutá-lo. Caso não deseje receber este tipo de erro, basta definir uma política de retry nesta tarefa!
# MAGIC
# MAGIC Agora que já sabemos qual é o erro, podemos aproveitar para corrigir o código da camada silver e evitar que ele se propague.
# MAGIC
# MAGIC - No notebook `02.02 - Ingestão e Transformação`, **adicione** este código à leitura da camada bronze: `.where("sales_id is not null")`
# MAGIC - Use a célula acima para encontrar o caminho completo do novo arquivo gerado e **atualize** a variável `new_file`
# MAGIC - **Execute** a célula abaixo para corrigir o dado
# MAGIC - Na tela da execução, clique em **Repair run** no canto superior direito
# MAGIC - As tarefas que não foram executadas com successo aparecerão selecionadas. Ajuste a seleção caso necessário
# MAGIC - Clique em **Repair run**
# MAGIC
# MAGIC > **NOTA:** caso deseje capturar novas ocorrências de ID nulo, o `where` pode ser removido após o reparo do pipeline

# COMMAND ----------

new_file = "/path/to/my/folder"

(spark.read.parquet(new_file)
  .withColumn("sales_id", lit("1234"))
  .write.mode("append").parquet(dest_path))

dbutils.fs.rm(new_file)
