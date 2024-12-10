# Databricks notebook source
# MAGIC %md # 02.06 - Task Values

# COMMAND ----------

# MAGIC %md ## 1. Recebendo variáveis de outras tarefas em um Workflow

# COMMAND ----------

from datetime import datetime

# Get values from another task in a Workflow
start_time_str = dbutils.jobs.taskValues.get(taskKey="ingestao", key="start_time")

# Prepare the start and end times
start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S.%f")
end_time = datetime.now()

# Calculate the execution time in hours
diff = end_time - start_time
diff_hours = diff.total_seconds() / 3600

# Print the results
print(f"Start: {start_time}")
print(f"End: {end_time}")
print(f"Execution time: {diff_hours} hours")
