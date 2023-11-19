# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/adlsformula1dl/raw

# COMMAND ----------

qualifying_schema = "constructorId INT, driverId INT, number INT, position INT, q1 STRING, q2 STRING, q3 STRING, qualifyId INT, raceId INT"

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).json('/mnt/adlsformula1dl/raw/qualifying', multiLine=True)

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/adlsformula1dl/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/adlsformula1dl/processed/qualifying"))

# COMMAND ----------


