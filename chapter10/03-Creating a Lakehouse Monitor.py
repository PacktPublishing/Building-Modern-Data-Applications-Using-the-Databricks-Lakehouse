# Databricks notebook source
# MAGIC %pip install dbldatagen==0.4.0

# COMMAND ----------

# Global vars - the Catalog, Schema, and Table name for this exercise
# You can change these values to match your environment
CATALOG_NAME = "chp10"
SCHEMA_NAME = "monitor_demo"
TABLE_NAME = "iot_readings"
FULLY_QUALIFIED_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

# COMMAND ----------

# Ensure that the Catalog and Schema exist for this exercise
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating Smart Thermostat Data
# MAGIC In this exercise, we'll use the Databricks Labs project `dbldatagen` to generate a synthetic dataset that mimics smart thermostats.

# COMMAND ----------

def generate_smart_thermostat_readings():
  """Generates synthetics thermostat readings"""
  import dbldatagen as dg
  from pyspark.sql.types import IntegerType, FloatType, TimestampType

  ds = (
      dg.DataGenerator(spark, name="smart_thermostat_dataset", rows=10000, partitions=4)
      .withColumn("device_id", IntegerType(), minValue=1000000, maxValue=2000000)
      .withColumn("temperature", FloatType(), minValue=10.0, maxValue=1000.0)
      .withColumn("humidity", FloatType(), minValue=0.1, maxValue=1000.0)
      .withColumn("battery_level", FloatType(), minValue=-50.0, maxValue=150.0)
      .withColumn("reading_ts", TimestampType(), random=False)
  )

  return ds.build()

# COMMAND ----------

# Generate the data using dbldatagen  
df = generate_smart_thermostat_readings()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving the Delta table to UC
# MAGIC Finally, we’ll save the newly created dataset as a Delta table in Unity Catalog. 

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(FULLY_QUALIFIED_TABLE_NAME))

# COMMAND ----------
