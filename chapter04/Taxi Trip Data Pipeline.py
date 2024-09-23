# Databricks notebook source
# MAGIC %md
# MAGIC ## Connect to your cloud storage location
# MAGIC
# MAGIC You can connect to various cloud storage locations. In the example below, we are connecting to an Azure Data Lake Storage (ADLS) Gen2 storage account. To connect to an alternate storage account, please consult the Databricks documentation.
# MAGIC
# MAGIC https://docs.databricks.com/en/connect/storage/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Important Note
# MAGIC In order for this data pipeline to work properly, the connection details and storage location should be identical to the settings in the accompanying notebook `Random Taxi Trip Data Generator`.

# COMMAND ----------

# Set a SAS token for authenticating with ABFSS and writing the sample NYC taxi trip data
storage_account = "yourstorageaccount"
storage_credential = dbutils.secrets.get("yourscopename", "yoursecretname")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", storage_credential)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the Databricks FileSystem
# MAGIC Alternatively, you can simplify the configuration setup by using the Databricks FileSystem (DBFS) which does not need additional authentication.

# COMMAND ----------

# Next, we can test the cloud storage path to see if authentication is successful
container_name = "yellowtaxi"
storage_path = "/tmp/chp_04/taxi_trip_data"

# Path to the raw landing zone
raw_landing_zone = f"{storage_path}/raw-zone"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline definition

# COMMAND ----------

import dlt
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
import pyspark.sql.functions as F
import random

# COMMAND ----------

@dlt.table(
  name="random_trip_data_raw",
  comment="The raw taxi trip data ingested from a landing zone.",
  table_properties={
    "quality": "bronze"
  }
)
def random_trip_data_raw():
  raw_trip_data_schema = StructType([
    StructField('Id', IntegerType(), True),
    StructField('driver_id', IntegerType(), True),
    StructField('Trip_Pickup_DateTime', TimestampType(), True),
    StructField('Trip_Dropoff_DateTime', TimestampType(), True),
    StructField('Passenger_Count', IntegerType(), True),
    StructField('Trip_Distance', DoubleType(), True), 
    StructField('Start_Lon', DoubleType(), True), 
    StructField('Start_Lat', DoubleType(), True), 
    StructField('Rate_Code', StringType(), True), 
    StructField('store_and_forward', IntegerType(), True), 
    StructField('End_Lon', DoubleType(), True), 
    StructField('End_Lat', DoubleType(), True), 
    StructField('Payment_Type', StringType(), True), 
    StructField('Fare_Amt', DoubleType(), True), 
    StructField('surcharge', DoubleType(), True), 
    StructField('mta_tax', StringType(), True), 
    StructField('Tip_Amt', DoubleType(), True), 
    StructField('Tolls_Amt', DoubleType(), True), 
    StructField('Total_Amt', DoubleType(), True)
  ])
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(raw_trip_data_schema)
    .load(raw_landing_zone))

# COMMAND ----------

@dlt.table(
  name="random_trip_data_silver",
  comment="Taxi trip data transformed with financial data.",
  table_properties={
    "quality": "silver",
    "pipelines.autoOptimize.zOrderCols": "driver_id"
  }
)
def random_trip_data_silver():
  return (dlt.read("random_trip_data_raw")
          .withColumn("car_maintenance_fees", F.col("Total_Amt") - F.col("Trip_Distance") * 0.15)
          .withColumn("driver_pay", F.col("Total_Amt") * 0.4)
          .withColumn("front_office_pay", F.col("Total_Amt") * 0.20))
  
