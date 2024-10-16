# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingesting raw trip data

# COMMAND ----------

@dlt.table(
   name="yellow_taxi_raw",
   comment="The randomly generated taxi trip dataset"
)
def yellow_taxi_raw():
  path = "/tmp/chp_03/taxi_data"
  schema = "trip_id INT, taxi_number INT, passenger_count INT, trip_amount FLOAT, trip_distance FLOAT, trip_date DATE"
  return (spark.readStream
               .schema(schema)
               .format("json")
               .load(path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying data quality rules

# COMMAND ----------

data_quality_rules = {
  "total_amount_assertion": "trip_amount > 0.0",
  "passenger_count": "passenger_count >= 1"
}

@dlt.table(
   name="yellow_taxi_validated",
   comment="Validation table that applies data quality rules to the incoming data"
)
def yellow_taxi_validated():
   return (dlt.readStream("yellow_taxi_raw")
      .withColumn("is_valid", 
                  when(expr(" AND ".join(data_quality_rules.values())), lit(True)).otherwise(lit(False)))
   )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementing a Quarantine Table
# MAGIC In the next section, we'll separate invalid records into a quarantine table using the `is_valid` column.

# COMMAND ----------

@dlt.table(
   name="yellow_taxi_quarantine",
   comment="A quarantine table for incoming data that has not met the validation criteria"
)
def yellow_taxi_quarantine():
   return (dlt.readStream("yellow_taxi_validated").where(expr("is_valid == False")))

# COMMAND ----------

@dlt.table(
   name="yellow_taxi_passing"
)
def yellow_taxi_passing():
   return (dlt.readStream("yellow_taxi_validated").where(expr("is_valid == True")))

# COMMAND ----------


