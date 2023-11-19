# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/adlsformula1dl/raw

# COMMAND ----------

pit_stops_df = spark.read.json('/mnt/adlsformula1dl/raw/pit_stops.json', multiLine=True)

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/adlsformula1dl/processed/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/adlsformula1dl/processed/pit_stops"))
