# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating a Lakehouse Monitor
# MAGIC The following notebook is used to generate a sample dataset from which we can attach a Lakehouse Monitor to and capture metrics.

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from dbldatagen import generate_data

# COMMAND ----------

# Define the schema for the thermostat readings
schema = {
   "timestamp": "timestamp",
   "device_id": "string",
   "temperature": "double",
   "humidity": "double",
   "battery_level": "double"
}

# Generate the data using dbldatagen
data = generate_data(schema, rows=1000)


# Create a Spark DataFrame from the generated data
df = spark.createDataFrame(data)


# Add the current timestamp to the dataframe
df = df.withColumn("timestamp", current_timestamp())
