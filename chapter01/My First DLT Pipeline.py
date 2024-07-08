# Databricks notebook source
# MAGIC %md
# MAGIC ## Databricks Datasets
# MAGIC Every Databricks workspace comes with sample datasets located at `/databricks-datasets` in the Databricks filesystem.
# MAGIC
# MAGIC We can use the Databricks filesystem utilities to browse the available datasets.

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Yellow taxi cab dataset
# MAGIC The `yellow_taxi` dataset contains trip data in New York City including pickup and dropoff times and locations.

# COMMAND ----------

path = "/databricks-datasets/nyctaxi/tripdata/yellow"
display(spark.read.format("csv").option("header", True).load(path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building a DLT pipeline
# MAGIC Let's begin by building our first DLT pipeline.
# MAGIC
# MAGIC The DLT Python module contains function decorators for declaring datasets and establishing dependencies.
# MAGIC
# MAGIC Let's begin by importing the `dlt` module.

# COMMAND ----------

import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding a function decorator
# MAGIC The DLT function decorators instruct the framework to create a dataset and also inform the framework about dependencies between datasets.
# MAGIC
# MAGIC When we execute a pipeline update, the DLT system will create a dataflow graph and will take care of updating each dataset in our pipeline in the correct order.
# MAGIC
# MAGIC Let's add a function decorator `@dlt.table()` so that DLT will create a new streaming table from the sample Yellow Taxi dataset.

# COMMAND ----------

@dlt.table(
  comment="The raw NYC taxi cab trip dataset located in `/databricks-datasets/`"
)
def yellow_taxi_raw():
  path = "/databricks-datasets/nyctaxi/tripdata/yellow"
  schema = "vendor_id string, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count integer, trip_distance float, pickup_longitude float, pickup_latitude float, rate_code integer, store_and_fwd_flag integer, dropoff_longitude float, dropoff_lattitude float, payment_type string, fare_amount float, surcharge float, mta_tax float, tip_amount float, tolls_amount float, total_amount float"
  return (spark.readStream
               .schema(schema)
               .format("csv")
               .option("header", True)
               .load(path))
