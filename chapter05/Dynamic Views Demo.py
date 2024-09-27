# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/COVID/covid-19-data/

# COMMAND ----------

# MAGIC %sh cat /dbfs/databricks-datasets/COVID/covid-19-data/README.md

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a sample catalog and schema

# COMMAND ----------

# Global variables
# ** IMPORTANT NOTE ** You should change these values to match your environment
CATALOG_NAME = "building_modern_dapps"
SCHEMA_NAME = "dynamic_views_demo"
PERSISTENT_TABLE_NAME = "covid_us_counties"

# You should not have to update this path, however
COVID_DATASET_PATH = "/databricks-datasets/COVID/covid-19-data/us-counties.csv"

# Feel free to update the View names
UNRESTRICTED_VIEW_NAME = "covid_us_counties_unrestricted_vw"
RESTRICTED_VIEW_NAME = "covid_us_counties_restricted_vw"
COL_AND_ROW_RESTRICTED_VIEW_NAME = "covid_us_counties_final_vw"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a persistent table 
# MAGIC
# MAGIC First, we'll need to create a persistent object in Unity Catalog to build Views from
# MAGIC
# MAGIC
# MAGIC Let's create a table from the COVID datasets under the `/databricks-datasets/` DBFS directory

# COMMAND ----------

covid_df = (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv("{COVID_DATASET_PATH}"))
(covid_df.write
  .mode("overwrite")
  .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.{PERSISTENT_TABLE_NAME}"))
spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{PERSISTENT_TABLE_NAME}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating an unrestricted View
# MAGIC Creating a View on top of structured data, like CSV files, is simple in Spark.
# MAGIC
# MAGIC However, with this type of view, the rows and column data is unrestricted. In other words, any user with access to the view (having `SELECT` entitlement) can select all data.
# MAGIC
# MAGIC Let's create a View that reads raw CSV COVID outbreak data from the 2020 COVID pandemic outbreak by U.S. counties. This dataset is included in all Databricks workspaces under the `/databricks-datasets/` DBFS directory. 

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE VIEW {UNRESTRICTED_VIEW_NAME} AS
  SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.{PERSISTENT_TABLE_NAME}
""")
spark.table(UNRESTRICTED_VIEW_NAME).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limiting columns and applying data mask
# MAGIC However, there may be certain scenarios when you want to limit access to certain columns or rows within a dataset based on certain privileges. 
# MAGIC
# MAGIC We can use a feature in Databricks called Dynamic Views to evaluate group membership in Unity Catalog. Based upon the group membership, we can give access to a user or we could limit access using a data mask. 
# MAGIC
# MAGIC Let's leverage a built-in Spark SQL function to apply a simple, yet powerful data mask to sensitive data columns, allowing only privileged members of the `admin` group access to view the text.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {RESTRICTED_VIEW_NAME} AS
SELECT
  date,
  county,
  state,
  CASE WHEN is_account_group_member('admins')
    THEN fips
    ELSE concat('***', substring(fips, length(fips)-1, length(fips)))
  END AS fips_id,
  cases,
  CASE WHEN is_account_group_member('admins')
    THEN deaths
    ELSE 'UNKNOWN'
  END AS mortality_cases
FROM {CATALOG_NAME}.{SCHEMA_NAME}.{PERSISTENT_TABLE_NAME}
"""
)
spark.table(RESTRICTED_VIEW_NAME).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limiting rows
# MAGIC In this previous examples, we've limited access to particular columns within the COVID dataset. Using dynamic views, we can also limit access to particular rows using a query predicate.
# MAGIC
# MAGIC In the final view definition, we'll limit which US states a particular user can view based on a users membership to the `admins` group.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {COL_AND_ROW_RESTRICTED_VIEW_NAME} AS
SELECT
   date,
   county,
   state,
   CASE WHEN is_account_group_member('admins')
    THEN fips
    ELSE concat('***', substring(fips, length(fips)-1, length(fips)))
  END AS fips_id,
  cases,
  CASE WHEN is_account_group_member('admins')
    THEN deaths
    ELSE 'UNKNOWN'
  END AS mortality_cases
FROM {CATALOG_NAME}.{SCHEMA_NAME}.{PERSISTENT_TABLE_NAME}
WHERE
  CASE WHEN is_account_group_member('admins')
    THEN 1=1
    ELSE state IN ('Alabamba', 'Colorado', 'California', 'Delaware', 'New York', 'Texas', 'Florida')
  END
""")
(spark.table(COL_AND_ROW_RESTRICTED_VIEW_NAME)
  .groupBy("state", "county")
  .agg({"cases": "count"})
  .orderBy("state", "county")
).withColumnRenamed("count(cases)", "total_covid_cases").display()


# COMMAND ----------
