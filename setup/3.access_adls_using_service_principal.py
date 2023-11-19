# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application/Service Principal (Azure AD has been renamed to Microsoft Entra ID)
# MAGIC 2. Generate a sercret/password for the Application
# MAGIC 3. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

storage_account = "adlsformula1dl"
client_id = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-client-id")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-client-secret")
tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# test if spark configuration is successful
display(dbutils.fs.ls("abfss://demo@adlsformula1dl.dfs.core.windows.net/"))

# COMMAND ----------


