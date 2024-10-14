-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS building_modern_dapps;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS building_modern_dapps.lineage_demo;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS
  building_modern_dapps.lineage_demo.taxi_drivers (
    driver_id INT,
    max_passengers INT,
    car_make string,
    car_model string,
    car_color string,
    car_year INT
  );

INSERT INTO building_modern_dapps.lineage_demo.taxi_drivers
    (driver_id, max_passengers, car_make, car_model, car_color, car_year)
VALUES
    (101, 4, "Ford", "Crown Victoria", "Yellow", 2004),
    (102, 3, "Lincoln", "MKT", "Black", 2019),
    (3396, 7, "Chevrolet","Suburban", "Grey", 2021);

CREATE TABLE IF NOT EXISTS building_modern_dapps.lineage_demo.taxi_drivers_refined
AS SELECT
  driver_id, max_passengers, concat(car_color, " ", car_year, " ", car_make," ", car_model)
AS
  car_description
FROM
  building_modern_dapps.lineage_demo.taxi_drivers;


-- COMMAND ----------
