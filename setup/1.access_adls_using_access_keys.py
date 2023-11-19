# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Key
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

storage_account = "adlsformula1dl"
access_credentials = dbutils.secrets.get(scope="formula1-scope", key="formula1dl-access-key")

spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", access_credentials)

# COMMAND ----------

# test if spark configuration is successful
display(dbutils.fs.ls("abfss://demo@adlsformula1dl.dfs.core.windows.net/"))

# COMMAND ----------


