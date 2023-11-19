# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
  # get secrets from key vault
  client_id = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-client-id")
  client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-client-secret")
  tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-tenant-id")

  # set spark configurations
  configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

  # unmount and mount again if already exists
  if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

  # mount the storage account container (optionally, you can add <directory-name> to the source URI of your mount point)
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)
  
  # display mounts post mounting to validate
  display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('adlsformula1dl', 'raw')

# COMMAND ----------

mount_adls('adlsformula1dl', 'processed')

# COMMAND ----------

mount_adls('adlsformula1dl', 'presentation')

# COMMAND ----------


