# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using SAS tokens
# MAGIC ####1. Set the spark config using SAS tokens
# MAGIC ####2. List files from demo container
# MAGIC ####3. Read data from circuit.csv file

# COMMAND ----------

formula1_demo_SAS_token = dbutils.secrets.get(scope="formula1-scope", key="formula1-demo-SAS-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlby63.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlby63.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlby63.dfs.core.windows.net", formula1_demo_SAS_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlby63.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlby63.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

