# Databricks notebook source
# MAGIC %md
# MAGIC ## Querying the event log
# MAGIC Let’s use Spark's `parse_json()` function to query the event log for all pipeline events, pulling out the data quality events.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW my_pipeline_data_quality_vw AS
# MAGIC SELECT
# MAGIC    parse_json(details :flow_progress :data_quality :expectations)
# MAGIC FROM
# MAGIC    my_pipeline_event_log_vw
# MAGIC WHERE
# MAGIC    event_type = 'flow_progress';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring the Data Quality
# MAGIC Now let’s take it a step further and count all the failing and passing records for each of the datasets in our DLT pipeline.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC    row_expectations.dataset as pipeline_dataset,
# MAGIC    row_expectations.name as expectation_name,
# MAGIC    SUM(row_expectations.passed_records) as num_passing_rows,
# MAGIC    SUM(row_expectations.failed_records) as num_failing_rows
# MAGIC FROM (
# MAGIC    SELECT * FROM my_pipeline_data_quality_vw
# MAGIC )
# MAGIC GROUP BY
# MAGIC    pipeline_dataset
# MAGIC    expectation_name
