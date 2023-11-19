# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

storage_account = "adlsformula1dl"
container = "demo"
client_id = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-client-id")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-client-secret")
tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-tenant-id")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account}/{container}",
  extra_configs = configs)

# COMMAND ----------

# test if spark configuration is successful
display(spark.read.csv(f"/mnt/{storage_account}/{container}/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())
