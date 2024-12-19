# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using access keys
# MAGIC ####1. Set the spark config fs.azure.account.key
# MAGIC ####2. List files from demo container
# MAGIC ####3. Read data from circuit.csv file

# COMMAND ----------

formula1_dl_account_key = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-dl-account-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlby63.dfs.core.windows.net", 
               formula1_dl_account_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlby63.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlby63.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

