# Databricks notebook source
dbutils.widgets.text("wPipelineId", "1234-1234-1234-1234", "Pipeline Id")
dbutils.widgets.text("wTableName", "my_catalog.my_schema.my_table", "Fully-qualified Tablename")

# COMMAND ----------

fq_table_name = dbutils.widgets.get("wTableName")
pipeline_id = dbutils.widgets.get("wPipelineId")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying the Event Log
# MAGIC You will need the `pipeline Id` or fully qualified `table name` (e.g. `catalog_name.schema_name.table_name`) to query event information in the event log. 
# MAGIC
# MAGIC You can retrieve the pipeline identifier either from the DLT UI or by querying the Databricks (Pipelines REST API)[https://docs.databricks.com/api/gcp/workspace/pipelines/listpipelines].
# MAGIC
# MAGIC **IMPORTANT NOTE:**
# MAGIC You will need to use a **shared** cluster or a Databricks SQL warehouse to query the event log.
# MAGIC

# COMMAND ----------

# Note: you will need to use a Shared cluster or a Databricks SQL warehouse to query the event log
if pipeline_id is not None and pipeline_id != "1234-1234-1234-1234":
  spark.sql(
  f"""
  CREATE OR REPLACE TEMPORARY VIEW my_pipeline_event_log_vw AS
  SELECT
    id, level, event_type, message, error, details, timestamp
  FROM
    event_log('{pipeline_id}')
  """
  )
  if spark.table("my_pipeline_event_log_vw").count() == 0:
    print("Hmm. Doesn't seem right. Double check that your DLT pipeline is configured to write to Unity Catalog.")
    print("Or perhaps you haven't run the pipeline yet.")
  else:
    spark.table("my_pipeline_event_log_vw").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying the Event Log by Table Name
# MAGIC You can narrow your search to viewing the events published for a particular dataset within a pipeline.
# MAGIC
# MAGIC You can query the event log by using the `TABLE()` SQL function and providing a fully-qualified table name.

# COMMAND ----------

if fq_table_name is not None and fq_table_name != "my_catalog.my_schema.my_table":
  spark.sql(
  f"""
  CREATE OR REPLACE TEMPORARY VIEW my_table_event_log_vw AS
  SELECT
    id, level, event_type, message, error, details, timestamp
  FROM
    event_log(table({fq_table_name}))
  """
  )
  spark.table("my_table_event_log_vw").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Data Quality 
# MAGIC We can leverage the previously defined views to monitor the ongoing data quality of the datasets within our DLT pipeline.
# MAGIC
# MAGIC Let's create another View specifically for the attributes pertaining to data quality for our pipeline.
# MAGIC
# MAGIC Along the way, we'll leverage the `FROM_JSON()` and `EXPLODE()` Spark SQL functions to pull out necesary elements in the data quality JSON. Data quality metrics are stored in the event log as a serialized JSON String. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW taxi_trip_pipeline_data_quality_vw AS
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   event_type,
# MAGIC   message,
# MAGIC   data_quality.dataset,
# MAGIC   data_quality.name AS expectation_name,
# MAGIC   data_quality.passed_records AS num_passed_records,
# MAGIC   data_quality.failed_records AS num_failed_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       event_type,
# MAGIC       message,
# MAGIC       timestamp,
# MAGIC       -- We'll need to use the `from_json()` function to parse the serialized JSON string for our data quality expectations
# MAGIC       -- We need to provide a schema for the JSON String
# MAGIC       -- And we'll use the `explode()` function to turn the array of expectations into a row for each expectation
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress.data_quality.expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) AS data_quality
# MAGIC     FROM
# MAGIC       my_table_event_log_vw
# MAGIC   );
# MAGIC SELECT *
# MAGIC   FROM data_quality_vw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute high-level data quality metrics
# MAGIC Finally, let's summarize the metrics per dataset. Common questions that are asked by data teams include: 
# MAGIC - "How many records were processed?"
# MAGIC - "How many records failed data quality validation?"
# MAGIC - "What was the percentage of passing records vs. failing records?"
# MAGIC
# MAGIC ...and many more.
# MAGIC
# MAGIC Let's compute the total number of expecatations that were evaluated (number of rows having an expectation applied), as well as the percentage of passing records vs. failing records.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   dataset,
# MAGIC   sum(num_passed_records + num_failed_records) AS total_expectations_evaluated,
# MAGIC   avg(num_passed_records / (num_passed_records + num_failed_records)) * 100 AS avg_pass_rate,
# MAGIC   avg(num_failed_records / (num_passed_records + num_failed_records)) * 100 AS avg_fail_rate
# MAGIC FROM
# MAGIC   taxi_trip_pipeline_data_quality_vw
# MAGIC GROUP BY
# MAGIC   timestamp,
# MAGIC   dataset;

# COMMAND ----------
