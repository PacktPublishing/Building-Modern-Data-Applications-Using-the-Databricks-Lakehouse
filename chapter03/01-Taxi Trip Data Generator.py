# Databricks notebook source
dbutils.widgets.text("wNumberOfFiles", "100", "Number of new files to generate")

# COMMAND ----------

# MAGIC %pip install dbldatagen==0.4.0

# COMMAND ----------

def generate_taxi_trip_data():
  """Generates random taxi trip data"""
  import dbldatagen as dg
  from pyspark.sql.types import IntegerType, StringType, FloatType, DateType

  ds = (
      dg.DataGenerator(spark, name="data_quality_taxi_trip_dataset", rows=100000, partitions=8)
      .withColumn("trip_id", IntegerType(), minValue=1000000, maxValue=2000000)
      .withColumn("taxi_number", IntegerType(), uniqueValues=10000, random=True)
      .withColumn("passenger_count", IntegerType(), minValue=1, maxValue=4)
      .withColumn("trip_amount", FloatType(), minValue=10.0, maxValue=1000.0)
      .withColumn("trip_distance", FloatType(), minValue=0.1, maxValue=1000.0)
      .withColumn("trip_date", DateType(), uniqueValues=300, random=True))

  return ds.build()

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/chp_03/taxi_data")
dbutils.fs.mkdirs("/tmp/chp_03/taxi_data_chkpnt")

# COMMAND ----------

import random

max_num_files = dbutils.widgets.get("wNumberOfFiles")
for i in range(int(max_num_files)):
  df = generate_taxi_trip_data()
  file_name = f"/tmp/chp_03/taxi_data/taxi_data_{random.randint(1, 1000000)}.json"
  df.write.mode("append").json(file_name)
  print(f"Wrote trip data to: '{file_name}'")

# COMMAND ----------

df = (
  spark.read.json("/tmp/chp_03/taxi_data/taxi_data_*.json")
)
df.display()

# COMMAND ----------

# Optional - Cleanup random generated data
#dbutils.fs.rm("/tmp/chp_03/taxi_data/", True)
#dbutils.fs.rm("/tmp/chp_03/taxi_data_chkpnt", recurse=True)
#spark.sql("DROP SCHEMA IF EXISTS hive_metastore.chp_03 CASCADE")
