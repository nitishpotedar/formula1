# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using Service Principal
# MAGIC ####1. Register Azure AD application/Service Principal
# MAGIC - app registration, pin it to the dashboard and copy the client & tenant IDs
# MAGIC ####2. Generate a secret/password for the Application
# MAGIC ####3. Set Spark Conf ith App/Client ID, Directory/Tenant ID & Secret
# MAGIC ####4. Assign role "Storage Blob Data Contributor" to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-principal-clientID")
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-principal-tenantID")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1-service-prinicpal-client-secret")



# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlby63.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlby63.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlby63.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlby63.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlby63.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlby63.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlby63.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

