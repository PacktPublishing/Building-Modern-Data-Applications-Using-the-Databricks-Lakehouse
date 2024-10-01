# Databricks notebook source
# MAGIC %md
# MAGIC ## Putting it all together
# MAGIC Let's revist our previous DLT pipeline definition and update the expecation, `expect_all()` to call our helper function that will read the latest data quality rules from our Delta table.

# COMMAND ----------
def compile_data_quality_rules(rules_table_name, dataset_name):
   """A helper function that reads from the data_quality_rules table and coverts to a format interpreted by a DLT Expectation."""
   rules = spark.sql(f"""SELECT * FROM {rules_table_name} WHERE dataset_name='{dataset_name}'""").collect()
   rules_dict = {}
   # Short circuit if there are no rules found
   if len(rules) == 0:
      raise Exception(f"No rules found for dataset '{dataset_name}'")
   for rule in rules:
      rules_dict[rule.rule_name] = rule.rule_expression
   return rules_dict

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

@dlt.table(
  comment="The raw NYC taxi cab trip dataset located in `/databricks-datasets/`"
)
def yellow_taxi_raw():
  path = "/tmp/chp_03/taxi_data"
  schema = "trip_id INT, taxi_number INT, passenger_count INT, trip_amount FLOAT, trip_distance FLOAT, trip_date DATE"
  return (spark.readStream
               .schema(schema)
               .format("json")
               .load(path))

@dlt.table(
   name="yellow_taxi_validated",
   comment="A dataset containing trip data that has been validated.")
@dlt.expect_all(compile_data_quality_rules("building_modern_dapps.chp_03.data_quality_rules", "yellow_taxi_raw"))
def yellow_taxi_validated():
   return (dlt.readStream("yellow_taxi_raw")
      .withColumn("nyc_congestion_tax", expr("trip_amount * 0.05")))


# COMMAND ----------
