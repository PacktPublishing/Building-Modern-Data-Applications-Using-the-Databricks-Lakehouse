# Databricks notebook source
# MAGIC %md
# MAGIC ## Get to know the dataset: US DOT BTS Airline Dataset
# MAGIC This dataset has been taken from the Department of Transportation's (DOT) Bureau of Transportation Statistics (BTS).

# COMMAND ----------

%sh cat /dbfs/databricks-datasets/airlines/README.md

# COMMAND ----------

schema = "Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string,FlightNum int,TailNum string,ActualElapsedTime int,CRSElapsedTime int,AirTime int,ArrDelay int,DepDelay int,Origin string,Dest string,Distance int,TaxiIn int,TaxiOut int,Cancelled string,CancellationCode string,Diverted int,CarrierDelay string,WeatherDelay string,NASDelay string,SecurityDelay string,LateAircraftDelay string,IsArrDelayed string,IsDepDelayed string"

# COMMAND ----------

df = spark.read.format("csv").schema(schema).option("header", True).load("/databricks-datasets/airlines/")
df.createOrReplaceTempView("airlines_tmp_vw")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build a Reference Table: Commercial Jet Airliner
# MAGIC The following reference table will be used to calculate carbon footprint from flight data.
# MAGIC
# MAGIC **Source:** https://en.wikipedia.org/wiki/List_of_commercial_jet_airliners

# COMMAND ----------

# Let's put our reference table in a new schema
catalog_name = "<REPLACE_ME>"
schema_name = "<REPLACE_ME>"
table_name = "<REPLACE_ME>"
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};
""")

# COMMAND ----------

commercial_airpliners = [
  ("Airbus A220", "Canada", 2, 2013, 2016, 287, 287, 5790),
  ("Airbus A320", "Multinational", 2, 1987, 1988, 17757, 10314, 7835),
  ("Airbus A330", "Multinational", 2, 1992, 1994, 1809, 1463, 36740),
  ("Airbus A330neo", "Multinational", 2, 2017, 2018, 123, 123, 36744 ),
  ("Airbus A350 XWB", "Multinational", 2, 2013, 2014, 557, 556, 44000),
  ("Antonov An-148/An-158", "Ukraine", 2, 2004, 2009, 37, 8, 98567 ),
  ("Boeing 737", "United States", 2, 1967, 1968, 11513, 7649, 6875),
  ("Boeing 767", "United States", 2, 1981, 1982, 1283, 764, 23980),
  ("Boeing 777", "United States", 2, 1994, 1995, 1713, 1483, 47890),
  ("Boeing 787 Dreamliner", "United States", 2, 2009, 2011, 1072, 1069, 33340),
  ("Comac ARJ21 Xiangfeng", "China", 2, 2008, 2015, 127, 122, 3500),
  ("Comac C919", "China", 2, 2017, 2023, 8, 4, 6436),
  ("Embraer E-Jet family", "Brazil", 2, 2002, 2004, 1671, 1443, 3071),
  ("Embraer E-Jet E2 family", "Brazil", 2, 2016, 2018, 81, 23, 3071),
  ("Ilyushin Il-96", "Russia", 4, 1988, 1992, 33, 4, 40322),
  ("Sukhoi Superjet SSJ100", "Russia", 2, 2008, 2011, 221, 160, 4175),
  ("Tupolev Tu-204/Tu-214", "Russia", 2, 1989, 1996, 89, 18, 4353)
]
commercial_airliners_schema = "jet_model string,	Origin string,	Engines int,	First_Flight int,	Airline_Service_Entry int, Number_Built int,	Currently_In_Service int, Fuel_Capacity int"
airliners_df = spark.createDataFrame(data=commercial_airpliners, schema=commercial_airliners_schema)
airliners_df.display()

# COMMAND ----------

airliners_table_name = f"{catalog_name}.{schema_name}.{table_name}"
(airliners_df.write
  .format("delta")
  .mode("overwrite")
  .option("mergeSchema", True)
  .saveAsTable(airliners_table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a DLT pipeline that will stream flight data
# MAGIC Next, let's create a DLT pipeline that creates several streaming tables for ingesting and transforming our flight data

# COMMAND ----------

import dlt

# COMMAND ----------

@dlt.table(
  name="commercial_airliner_flights_bronze",
  comment="The commercial airliner flight data dataset located in `/databricks-datasets/`"
)
def commercial_airliner_flights_bronze():
  path = "/databricks-datasets/airlines/"
  schema = "Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string,FlightNum int,TailNum string,ActualElapsedTime int,CRSElapsedTime int,AirTime int,ArrDelay int,DepDelay int,Origin string,Dest string,Distance int,TaxiIn int,TaxiOut int,Cancelled string,CancellationCode string,Diverted int,CarrierDelay string,WeatherDelay string,NASDelay string,SecurityDelay string,LateAircraftDelay string,IsArrDelayed string,IsDepDelayed string"
  return (spark.readStream
            .format("csv")
            .schema(schema)
            .option("header", True)
            .load(path))

# COMMAND ----------

# Let's create a User-defined Function (UDF) for randomly associating
# Tail numbers with a our commercial jets
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def generate_jet_model():
  import random
  commercial_jets = [
    "Airbus A220",
    "Airbus A320",
    "Airbus A330",
    "Airbus A330neo",
    "Airbus A350 XWB",
    "Antonov An-148/An-158",
    "Boeing 737",
    "Boeing 767",
    "Boeing 777",
    "Boeing 787 Dreamliner",
    "Comac ARJ21 Xiangfeng",
    "Comac C919", "China",
    "Embraer E-Jet family",
    "Embraer E-Jet E2 family",
    "Ilyushin Il-96", "Russia",
    "Sukhoi Superjet SSJ100",
    "Tupolev Tu-204/Tu-214",
  ]
  random_index = random.randint(0, 16)
  return commercial_jets[random_index]

# COMMAND ----------

@dlt.table(
  name="commercial_airliner_flights_silver",
  comment="The commercial airliner flight data augmented with randomly generated jet model and used fuel amount."
)
def commercial_airliner_flights_silver():
  from pyspark.sql.functions import col
  return (dlt.read_stream("commercial_airliner_flights_bronze")
          .withColumn("jet_model", generate_jet_model())
          .join(spark.table(airliners_table_name), ["jet_model"], "left"))
