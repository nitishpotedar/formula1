# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Cluster Scoped Credentials
# MAGIC ####1. Set the spark config fs.azure.account.key in the cluster
# MAGIC ####2. List files from demo container
# MAGIC ####3. Read data from circuit.csv file

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlby63.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlby63.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

