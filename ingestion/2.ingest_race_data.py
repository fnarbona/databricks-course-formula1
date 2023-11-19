# Databricks notebook source
# %fs
# ls /mnt/adlsformula1dl/raw

# COMMAND ----------

# imports for creating schemas
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

# create the schema
races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# read the csv with headers and enforced schema
data_file_path = "/mnt/adlsformula1dl/raw/races.csv"
races_df = spark.read.option("schema", races_schema).csv(data_file_path, header=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, col, lit, concat

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(" "), col('time')), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias("race_id"), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode("overwrite").parquet("/mnt/adlsformula1dl/processed/races")

# COMMAND ----------

df_from_parquet = spark.read.parquet("/mnt/adlsformula1dl/processed/races")
display(df_from_parquet)

# COMMAND ----------


