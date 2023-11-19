# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/adlsformula1dl/raw

# COMMAND ----------

results_df = spark.read.json('/mnt/adlsformula1dl/raw/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

results_final_df = results_df.drop(col("statusId")) \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("resultId", "result_id") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_final_df.write.mode("overwrite").parquet("/mnt/adlsformula1dl/processed/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/adlsformula1dl/processed/results"))

# COMMAND ----------


