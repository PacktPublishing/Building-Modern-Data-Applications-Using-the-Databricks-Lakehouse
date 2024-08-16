-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Publishing Datasets to Unity Catalog
-- MAGIC
-- MAGIC Unity Catalog is the new best of breed method for storing data and querying
-- MAGIC datasets in the Lakehouse. One might choose landing data in a data pipeline
-- MAGIC into Unity Catalog over the Hive metastore for a number of reasons including
-- MAGIC the following:
-- MAGIC
-- MAGIC 1. The data is secured by default
-- MAGIC
-- MAGIC 2. There is a consistent definition of access policies across groups and users
-- MAGIC versus defining data access policies for every individual workspace
-- MAGIC
-- MAGIC 3. Open-sourced technology with no risk of vendor lock-in
-- MAGIC
-- MAGIC Furthermore, Unity Catalog offers a Hive compatible API, allowing third-party
-- MAGIC tools to integrate with a Unity Catalog Metastore as if it were a Hive
-- MAGIC metastore.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Important note
-- MAGIC If you are creator and owner of a target catalog and schema objects, as well as
-- MAGIC the creator and owner of a DLT pipeline, then you do not need to execute the
-- MAGIC following `GRANT` statements.
-- MAGIC
-- MAGIC Rather the `GRANT` statements are meant to demonstrate the types of permissions
-- MAGIC needed to share data assets across multiple groups and users in a typical Unity
-- MAGIC Catalog metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a Catalog
-- MAGIC Unity Catalog introduces a three-level namespace when defining tables. The parent namespace will refer to the Catalog object.
-- MAGIC
-- MAGIC A Catalog is a logical container that will hold one to many schemas, or databases.

-- COMMAND ----------


CREATE CATALOG IF NOT EXISTS `chp2_transforming_data`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Updating permissions
-- MAGIC Let's grant permission for the user `my_databricks_user@example.com` to use
-- MAGIC and store datasets in this new Catalog.
-- MAGIC
-- MAGIC The full list of permissions available on the UC Catalog object can be found here: [Privilege Types by UC Object Docs](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#privilege-types-by-securable-object-in-unity-catalog)

-- COMMAND ----------


GRANT USE CATALOG, CREATE SCHEMA ON CATALOG `chp2_transforming_data` TO `my_databricks_user@example.com`;

-- COMMAND ----------


USE CATALOG `chp2_transforming_data`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a Schema
-- MAGIC Next, we'll need to create a schema that will hold the output datasets
-- MAGIC from our DLT pipeline.

-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS `ride_hailing`;

USE SCHEMA `ride_hailing`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Granting Permissions on UC Schema
-- MAGIC UC is simple, yet powerful, allowing data stewards to permit users/groups
-- MAGIC to manipulate data objects.
-- MAGIC
-- MAGIC Let's grant access to a Databricks user to create
-- MAGIC new DLT pipeline objects in the newly created Schema.
-- MAGIC
-- MAGIC The full list of permissions available on the UC Catalog object can be found here: [Privilege Types by UC Object Docs](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#privilege-types-by-securable-object-in-unity-catalog)

-- COMMAND ----------


GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA `ride_hailing`
  TO `my_databricks_user@example.com`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## (Optional) Cleanup
-- MAGIC Execute the following cell to remove the previously created objects.

-- COMMAND ----------


DROP CATALOG IF EXISTS `chp2_transforming_data` CASCADE;

-- COMMAND ----------
