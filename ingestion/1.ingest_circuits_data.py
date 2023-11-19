# Databricks notebook source
# see current dbfs volumes
# display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adlsformula1dl/processed

# COMMAND ----------

# imports for creating schemas
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# create the schema
circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# read the csv with headers and enforced schema
circuits_df = spark.read.option("schema", circuits_schema).csv("dbfs:/mnt/adlsformula1dl/raw/circuits.csv", header=True, enforceSchema=True)

# COMMAND ----------

# validate schema is applied
circuits_df.printSchema()

# COMMAND ----------

# select specific columns
circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# validate columns selected
circuits_selected_df.columns

# COMMAND ----------

df_renamed_cols = circuits_selected_df.withColumnRenamed("circuitsId", "circuits_id") \
                    .withColumnRenamed("circuitRef", "circuit_ref") \
                    .withColumnRenamed("lat", "latitude") \
                    .withColumnRenamed("lng", "longitude") \
                    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# validate columns were renamed
df_renamed_cols.columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = df_renamed_cols.withColumn("ingestion_date", current_timestamp())
display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/adlsformula1dl/processed/circuits")

# COMMAND ----------

df_from_parquet = spark.read.parquet("/mnt/adlsformula1dl/processed/circuits")
display(df_from_parquet)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adlsformula1dl/processed/circuits
