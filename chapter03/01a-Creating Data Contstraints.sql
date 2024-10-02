-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS yellow_taxi_catalog;
CREATE SCHEMA IF NOT EXISTS yellow_taxi_catalog.yellow_taxi;

-- COMMAND ----------

CREATE TABLE yellow_taxi_catalog.yellow_taxi.drivers (
  driver_id INTEGER NOT NULL,
  first_name STRING,
  last_name STRING,
  CONSTRAINT drivers_pk PRIMARY KEY(driver_id)
);

-- COMMAND ----------

CREATE TABLE yellow_taxi_catalog.yellow_taxi.rides (
  ride_id INTEGER NOT NULL,
  driver_id INTEGER,
  passentger_count INTEGER,
  total_amount DOUBLE,
  CONSTRAINT rides_pk PRIMARY KEY (ride_id),
  CONSTRAINT drivers_fk FOREIGN KEY (driver_id) REFERENCES yellow_taxi_catalog.yellow_taxi.drivers
);

-- COMMAND ----------

CREATE VIEW yellow_taxi_catalog.yellow_taxi.rides_pk_validation_vw AS
SELECT
  *
FROM
  (
    SELECT
      count(*) AS num_occurrences
    FROM
      yellow_taxi_catalog.yellow_taxi.rides
    GROUP BY
      ride_id
  )
WHERE
  num_occurrences > 1;

-- COMMAND ----------

SELECT count(*) AS num_invalid_pks
  FROM yellow_taxi_catalog.yellow_taxi.rides_pk_validation_vw;

-- COMMAND ----------

-- Optional: Uncomment the following statements to remove data assets created in this exercise
-- DROP SCHEMA yellow_taxi_catalog.yellow_taxi CASCADE;
-- DROP CATALOG yellow_taxi_catalog;
