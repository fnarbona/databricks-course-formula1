# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

storage_account = "adlsformula1dl"
sas_token_credentials = dbutils.secrets.get(scope="formula1-scope", key="formula1dl-sas-token")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", sas_token_credentials)

# COMMAND ----------

# test if spark configuration is successful
display(dbutils.fs.ls("abfss://demo@adlsformula1dl.dfs.core.windows.net/"))

# COMMAND ----------


