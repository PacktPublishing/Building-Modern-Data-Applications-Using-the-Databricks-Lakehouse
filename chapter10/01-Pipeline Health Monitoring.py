# Databricks notebook source
# MAGIC %md
# MAGIC ## Querying the DLT Pipeline Event Log
# MAGIC A common approach to make it easier for data stewards to query events for a particular DLT pipeline is to register a view.
# MAGIC
# MAGIC This allows users to conveniently reference the event log results in subsequent queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW my_pipeline_event_log_vw AS
# MAGIC SELECT
# MAGIC    *
# MAGIC FROM
# MAGIC    event_log("my_dlt_pipeline_id");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolving event log using the `TABLE()` function
# MAGIC The following SQL DDL statement will create a View that retrieves the event log for a dataset called `my_gold_table`:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW my_gold_table_event_log_vw AS
# MAGIC SELECT
# MAGIC    *
# MAGIC FROM
# MAGIC    event_log(TABLE("my_gold_table"));
