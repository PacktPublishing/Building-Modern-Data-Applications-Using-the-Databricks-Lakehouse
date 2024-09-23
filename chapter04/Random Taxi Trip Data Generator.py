# Databricks notebook source
# MAGIC %pip install dbldatagen==0.4.0 faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to your cloud storage location
# MAGIC
# MAGIC You can connect to various cloud storage locations. In the example below, we are connecting to an Azure Data Lake Storage (ADLS) Gen2 storage account. To connect to an alternate storage account, please consult the Databricks documentation.
# MAGIC
# MAGIC https://docs.databricks.com/en/connect/storage/index.html

# COMMAND ----------

# Set a SAS token for authenticating with ABFSS and writing the sample NYC taxi trip data
storage_account = "yourstorageaccount"
storage_credential = dbutils.secrets.get("yourscopename", "yoursecretname")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", storage_credential)

storage_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storing data in the Databricks FileSystem 
# MAGIC Alternatively, you can use the Databricks FileSystem (DBFS) to store data in the managed storage account.

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/chp_04/taxi_trip_data")
dbutils.fs.mkdirs("/tmp/chp_04/taxi_trip_data_chkpnt")
spark.sql("CREATE CATALOG IF NOT EXISTS building_modern_dapps")
spark.sql("CREATE SCHEMA IF NOT EXISTS building_modern_dapps.chp_04")


# COMMAND ----------

# Next, we can test the cloud storage path to see if authentication is successful
container_name = "yellowtaxi"
storage_path = "/tmp/chp_04/taxi_trip_data"

# Ensure that you can authenticate with the storage service and list the directory contents
dbutils.fs.ls(storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate random trip data

# COMMAND ----------

import random
from pyspark.sql.types import StringType, DoubleType, TimestampType, IntegerType

# Let's define a helper function for generating some randmon taxi trip data
def generate_randmon_trip_data():
  """Helper function that generate mock NYC taxi trip data."""

  # We'll use the Data Generator and Faker libs to create random trip data
  from dbldatagen import DataGenerator

  # Randomly choose between generating 25 to 150 rows of data
  num_rows = random.uniform(25, 150)

  # Taxi cab Ids
  driver_ids = [20042, 14567, 997, 6579, 3392, 7755, 33209, 101, 45587]

  # Let's define how our sample DataFrame should look
  data_spec = (
    DataGenerator(sparkSession=spark, name="taxi_trip", rows=num_rows, partitions=4,random=True)
      .withIdOutput()
      .withColumn("driver_id", IntegerType(), values=driver_ids)
      .withColumn("Trip_Pickup_DateTime", TimestampType(), uniqueValues=300, random=True)
      .withColumn("Trip_Dropoff_DateTime", TimestampType(), uniqueValues=300, random=True)
      .withColumn("Passenger_Count", IntegerType(), percentNulls=0.0, minValue=1, maxValue=3, random=True)
      .withColumn("Trip_Distance", DoubleType(), minValue=1.0, maxValue=35.0, step=1.55, random=True)
      .withColumn("Start_Lon", DoubleType(), minValue=40.0, maxValue=50.0, step=0.1, random=True)
      .withColumn("Start_Lat", DoubleType(), minValue=74.0, maxValue=80.0, step=0.1, random=True)
      .withColumn("Rate_Code", StringType(), values=["AAC", "BAC", "DDC", "FFE"])
      .withColumn("store_and_forward", IntegerType(), minValue=1, maxValue=30, random=True)
      .withColumn("End_Lon", DoubleType(), minValue=74.0, maxValue=80.0, step=0.1, random=True)
      .withColumn("End_Lat", DoubleType(), minValue=74.0, maxValue=80.0, step=0.1, random=True)
      .withColumn("Payment_Type", StringType(), values=["Cash", "Credit"])
      .withColumn("Fare_Amt", DoubleType(), percentNulls=0.05, minValue=10.00, maxValue=300.00, step=1.25, random=True) 
      .withColumn("surcharge", DoubleType(), percentNulls=0.05, minValue=1.00, maxValue=10.00, step=1.25, random=True) 
      .withColumn("mta_tax", StringType(), percentNulls=0.05, minValue=1.00, maxValue=10.00, step=1.25, random=True)
      .withColumn("Tip_Amt", DoubleType(), percentNulls=0.05, minValue=1.00, maxValue=50.00, step=1.25, random=True) 
      .withColumn("Tolls_Amt", DoubleType(), percentNulls=0.05, minValue=1.00, maxValue=25.00, step=1.25, random=True) 
      .withColumn("Total_Amt", DoubleType(), percentNulls=0.05, minValue=1.00, maxValue=350.00, step=1.25, random=True)
  )

  # Next, build a DataFrame from the data spec
  df = data_spec.build()

  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating random taxi trip data
# MAGIC
# MAGIC We want to simulate unpredictable data ingestion you could expect in a production environment. We'll leverage the Python `random` library to simulate random behavior.
# MAGIC

# COMMAND ----------

import time

# Create the raw landing zone if not exists
raw_landing_zone = f"{storage_path}/raw-zone"
dbutils.fs.mkdirs(raw_landing_zone)

# Choose a random number of iterations of trip data to write to cloud storage
max_num_iterations = random.randint(10, 30)

# Let's generate some trip data at random intervals and random volumes of data
for i in range (0, max_num_iterations):  
  # Generate random trip data
  trip_data_df = generate_randmon_trip_data()

  # Save the trip data to our raw landing zone
  print("Writing randomly generated trip data to landing zone...")
  trip_data_df.write.format("json").mode("append").save(raw_landing_zone)
  trip_data_df.show()
  print(f"Wrote {trip_data_df.count()} new rows.")

  # Choose a random amount of time to pause between data writes
  sleep_length_secs = random.randint(5, 30) 
  print(f"Sleeping for {sleep_length_secs} seconds.")
  time.sleep(sleep_length_secs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup
# MAGIC Optionally, you can cleanup the data that was generated for this hands-on exercise by executing the following cell.

# COMMAND ----------

#dbutils.fs.rm("/tmp/chp_04", recurse=True)
#spark.sql("DROP SCHEMA IF EXISTS building_modern_dapps.chp_04 CASCADE")
