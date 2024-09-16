# Databricks notebook source
# MAGIC %md
# MAGIC ## Write Taxi Driver Table
# MAGIC **Note:** This step is completely **optional**.
# MAGIC If you have a Postgres database available for this exercise,
# MAGIC you can use it to store taxi driver information.
# MAGIC
# MAGIC However, it a Postgres database is not mandatory, but used to demonstrate
# MAGIC the flexibilty of the Databricks platform and DLT in ingesting data from an RDBMS.

# COMMAND ----------

df = (spark.createDataFrame(
      data=[
        (10001, 7292, "Toyota", "Camry", 5),
        (10002, 7782, "Honda", "Civic", 4),
        (10001, 2736, "Toyota", "Corolla", 5),
        (10001, 5284, "Toyota", "Corolla", 5),
        (10001, 1922, "Toyota", "Corolla", 5),
        (10001, 1704, "Toyota", "Corolla", 5),
        (10001, 6560, "Ford", "Escape", 5),
        (10001, 2086, "Ford", "Crown Victoria", 5),
        (10001, 8266, "Ford", "Crown Victoria", 5),
        (10001, 2013, "Toyota", "Camry", 5),
        (10001, 2798, "Toyota", "Sienna", 7)
      ],
      schema="driver_id STRING, taxi_number INT, cab_make STRING, cab_model STRING, seating_capacity INT"
    ))
df.display()

# COMMAND ----------

POSTGRES_HOSTNAME = "<HOSTNAME>"
POSTGRES_PORT = "5432"

# It's recommended to store the credentials in Databricks secrets.
# More info on using Secrets can be found here: https://docs.databricks.com/en/security/secrets/secrets.html
POSTGRES_USERNAME = dbutils.secrets.get("postgresdb_scope", "postgres_username")
POSTGRES_PW = dbutils.secrets.get("postgresdb_scope", "postgres_pw")

POSTGRES_DB = "<TAXI_DRIVER_DB>"
POSTGRES_TABLENAME = "<TAXI_DRIVER_TABLE>"

POSTGRES_CONN_URL = f"jdbc:postgresql://{POSTGRES_HOSTNAME}:{POSTGRES_PORT}/{POSTGRES_DB}"

# COMMAND ----------

# Finally, use PySpark's DataFrame writer to create the table
# in our PostgresDB, overwriting the table if it already exists
(df.write.format("jdbc")
    .option("url", POSTGRES_CONN_URL)
    .option("dbtable", POSTGRES_TABLENAME)
    .option("user", POSTGRES_USERNAME)
    .option("password", POSTGRES_PW)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save())
