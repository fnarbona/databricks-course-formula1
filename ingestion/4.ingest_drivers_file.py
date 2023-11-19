# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/adlsformula1dl/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True), StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False), 
    StructField("driverRef", StringType(), True),
    StructField("number", StringType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema, True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json("/mnt/adlsformula1dl/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_final_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("name", concat(col("name.forename"), lit (" "), col("name.surname"))) \
    .drop("url")

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/adlsformula1dl/processed/drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/adlsformula1dl/processed/drivers"))
