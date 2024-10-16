# Databricks notebook source
# MAGIC %md
# MAGIC ## Decoupling Expectations from DLT
# MAGIC In this notebook example, we'll be looking at how we can decouple the expectations defined in previous examples into a more digestable format by personas that might prefer working with SQL syntax instead.

# COMMAND ----------

# ** IMPORTANT NOTE: **
# You should update the following values to match your environment
CATALOG_NAME = "building_modern_dapps"
SCHEMA_NAME = "chp_03"

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining a Rules table
# MAGIC Let's start by defining a Delta table that will hold all of the data quality rules by datasets.
# MAGIC
# MAGIC We'll introduce 3 columns: a `rule_name`, a `rule_expression`, and a `dataset_name` - everything needed to define an Expectation in DLT.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_quality_rules
# MAGIC (rule_name STRING, rule_expression STRING, dataset_name STRING)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storing data quality rules in tabular format
# MAGIC In this section, we'll be revisting the earlier example, where expectations where specified using a `dict` data structure in Python.
# MAGIC
# MAGIC For some data analysts, using a tabular representation is more preferable than working with a programming language like Python.
# MAGIC
# MAGIC Let's take the same `dict` variable, `assertions` defined earlier and convert it to a tabular format, inserting the entries into our Delta table.
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC assertions = {
# MAGIC    "total_amount_constraint": "trip_amount > 0.0",
# MAGIC    "passenger_count": "passenger_count >= 1"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   data_quality_rules
# MAGIC VALUES
# MAGIC   (
# MAGIC     'valid_total_amount',
# MAGIC     'trip_amount > 0.0',
# MAGIC     'yellow_taxi_raw'
# MAGIC   ),(
# MAGIC     'valid_passenger_count',
# MAGIC     'passenger_count > 0',
# MAGIC     'yellow_taxi_raw'
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lesson learned
# MAGIC As you can see, viewing, updating, or creating new data quality rules doesn't require Python or DLT knowledge, thereby enabling entire groups of personas in an organization to maintain data quality rules of data entering the Lakehouse.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_quality_rules;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dyanmically compiling the data quality rules
# MAGIC Let's define a helper function in Python that will read the latest data quality rules and dynamically add them to our DLT expectations.

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

# MAGIC %md
# MAGIC ## Putting it all together
# MAGIC Let's revist our previous DLT pipeline definition and update the expecation, `expect_all()` to call our helper function that will read the latest data quality rules from our Delta table.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

RULES_TABLE = "data_quality_rules"
DATASET_NAME = "yellow_taxi_raw"

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
@dlt.expect_all(compile_data_quality_rules(RULES_TABLE, DATASET_NAME))
def yellow_taxi_validated():
   return (dlt.readStream("yellow_taxi_raw")
      .withColumn("nyc_congestion_tax", expr("trip_amount * 0.05")))


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optional: uncomment and run the following statements to clean up the data assets created in this exercise
# MAGIC --TRUNCATE TABLE data_quality_rules;
# MAGIC --DROP TABLE IF EXISTS data_quality_rules;

# COMMAND ----------
