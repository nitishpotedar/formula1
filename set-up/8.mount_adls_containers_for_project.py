# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Service Principal
# MAGIC ####Steps to follow
# MAGIC 1. Get the client ID, tenant ID and client secret from the key vault
# MAGIC 2. Set Spark Conf ith App/Client ID, Directory/Tenant ID & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to the mount (list all mounts, unmount)

# COMMAND ----------

def mount_adls(storage_account_name, container_name):

  client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-principal-clientID")
  tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-principal-tenantID")
  client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-prinicpal-client-secret")

  configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
  
  if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs
  )

  display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls('formula1dlby63', 'raw')

# COMMAND ----------

mount_adls('formula1dlby63', 'processed')

# COMMAND ----------

mount_adls('formula1dlby63', 'presentation')

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlby63/demo/circuits.csv"))

# COMMAND ----------

