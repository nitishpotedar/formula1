# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Service Principal
# MAGIC ####Steps to follow
# MAGIC 1. Get the client ID, tenant ID and client secret from the key vault
# MAGIC 2. Set Spark Conf ith App/Client ID, Directory/Tenant ID & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to the mount (list all mounts, unmount)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-principal-clientID")
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-principal-tenantID")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-prinicpal-client-secret")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlby63.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlby63/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlby63/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlby63/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dlby63/demo')

# COMMAND ----------

