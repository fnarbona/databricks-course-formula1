# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/adlsformula1dl/raw

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json("/mnt/adlsformula1dl/raw/constructors.json")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("/mnt/adlsformula1dl/processed/constructors")

# COMMAND ----------


