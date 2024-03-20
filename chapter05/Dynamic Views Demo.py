# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/COVID/covid-19-data/

# COMMAND ----------

# MAGIC %sh cat /dbfs/databricks-datasets/COVID/covid-19-data/README.md

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a sample catalog and schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS bmdadl_chp5;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG bmdadl_chp5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dynamic_views_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA dynamic_views_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW covid_data_unrestricted_vw
# MAGIC TBLPROPERTIES ('header'='true')
# MAGIC AS
# MAGIC SELECT *
# MAGIC   FROM csv.`/databricks-datasets/COVID/covid-19-data/us-counties-recent.csv`;
# MAGIC SELECT * FROM covid_data_unrestricted_vw;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limiting columns and applying data mask
# MAGIC Next, let's leverage built-in Spark SQL function to apply a simple, yet powerful data mask to sensitive data columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW covid_data_restricted_vw
# MAGIC TBLPROPERTIES ('header'='true')
# MAGIC AS
# MAGIC SELECT
# MAGIC    date,
# MAGIC    county,
# MAGIC    state,
# MAGIC    CASE WHEN is_account_group_member('admins')
# MAGIC     THEN fips
# MAGIC     ELSE concat('***', substring(fips, length(fips)-1, length(fips)))
# MAGIC     END,
# MAGIC    cases,
# MAGIC    CASE WHEN is_account_group_member('admins')
# MAGIC     THEN deaths
# MAGIC     ELSE 'UNKNOWN'
# MAGIC     END
# MAGIC   FROM csv.`/databricks-datasets/COVID/covid-19-data/us-counties-recent.csv`;
# MAGIC SELECT * FROM covid_data_restricted_vw;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limiting rows
# MAGIC In the final view definition, we'll limit which rows an particular user can view based on their group membership.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW covid_data_final_vw
# MAGIC TBLPROPERTIES ('header'='true')
# MAGIC AS
# MAGIC SELECT
# MAGIC    date,
# MAGIC    county,
# MAGIC    state,
# MAGIC    CASE WHEN is_account_group_member('admins')
# MAGIC     THEN fips
# MAGIC     ELSE concat('***', substring(fips, length(fips)-1, length(fips)))
# MAGIC     END,
# MAGIC    cases,
# MAGIC    CASE WHEN is_account_group_member('admins')
# MAGIC     THEN deaths
# MAGIC     ELSE 'UNKNOWN'
# MAGIC     END
# MAGIC   FROM csv.`/databricks-datasets/COVID/covid-19-data/us-counties-recent.csv`
# MAGIC  WHERE CASE WHEN is_account_group_member('admins')
# MAGIC    THEN 1=1
# MAGIC    ELSE state IN ('Alabamba', 'Colorado', 'California', 'Delaware', 'New York', 'Texas', 'Florida')
# MAGIC    END;
# MAGIC SELECT * FROM covid_data_final_vw;

# COMMAND ----------
