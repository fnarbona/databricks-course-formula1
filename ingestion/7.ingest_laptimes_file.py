# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/adlsformula1dl/raw

# COMMAND ----------

lap_times_schema = "race_id INT, driver_id INT, lap INT, position INT, time STRING, milliseconds INT"

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv('/mnt/adlsformula1dl/raw/lap_times')

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/adlsformula1dl/processed/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/adlsformula1dl/processed/lap_times"))

# COMMAND ----------


