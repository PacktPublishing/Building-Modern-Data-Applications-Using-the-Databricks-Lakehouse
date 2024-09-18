# Databricks notebook source
dbutils.widgets.text("wNumberOfFiles", "10", "Number of new files to generate")

# COMMAND ----------

# MAGIC %pip install dbldatagen==0.4.0

# COMMAND ----------

# Global vars - the Catalog, Schema, and Table name for this exercise
# You can change these values to match your environment
CATALOG_NAME = "building_modern_dapps"
SCHEMA_NAME = "iot_device_readings"

# COMMAND ----------

def generate_iot_device_readings():
  """Generates synthetics thermostat readings"""
  import dbldatagen as dg
  from pyspark.sql.types import IntegerType, FloatType, TimestampType

  ds = (
      dg.DataGenerator(spark, name="iot_device_dataset", rows=10000, partitions=4)
      .withColumn("device_id", IntegerType(), minValue=1000000, maxValue=2000000)
      .withColumn("temperature", FloatType(), minValue=10.0, maxValue=1000.0)
      .withColumn("humidity", FloatType(), minValue=0.1, maxValue=1000.0)
      .withColumn("battery_level", FloatType(), minValue=-50.0, maxValue=150.0)
      .withColumn("reading_ts", TimestampType(), random=False)
  )

  return ds.build()

# COMMAND ----------

# Create raw landing zone and checkpoint directories
dbutils.fs.mkdirs("/tmp/chp_10/iot_device_data")
dbutils.fs.mkdirs("/tmp/chp_10/iot_device_data_chkpnt")

# Ensure that the Catalog and Schema exist for this exercise
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

import random

max_num_files = dbutils.widgets.get("wNumberOfFiles")
for i in range(int(max_num_files)):
  df = generate_iot_device_readings()
  file_name = f"/tmp/chp_10/iot_device_data/iot_device_data_{random.randint(1, 1000000)}.json"
  df.write.mode("append").json(file_name)
  print(f"Wrote IoT device data to: '{file_name}'")

# COMMAND ----------

df = (
  spark.read.json("/tmp/chp_10/iot_device_data/iot_device_data_*.json")
)
df.display()

# COMMAND ----------

# Optional - Cleanup random generated data
# dbutils.fs.rm("/tmp/chp_10/iot_device_data/", True)
# dbutils.fs.rm("/tmp/chp_10/iot_device_data_chkpnt", recurse=True)
# dbutils.fs.rm("/tmp/chp_10/", True)
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME} CASCADE")


# COMMAND ----------
