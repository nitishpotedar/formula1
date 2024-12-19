# Databricks notebook source
# MAGIC %md
# MAGIC ###Explore DBFS Root
# MAGIC ####1. List all the folders in the DBFS root
# MAGIC ####2. Interact with DBFS File browser
# MAGIC ####3. Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

