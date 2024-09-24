# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BinaryType

# COMMAND ----------

# Globals
# ** IMPORTANT NOTE: **
# Replace these values to match your environment
CATALOG_NAME = "<REPLACE_ME>"
SCHEMA_NAME = "<REPLACE_ME>"
VOLUME_NAME = "<REPLACE_ME>"

# COMMAND ----------

@dlt.table(
  name="text_docs_raw",
  comment="Raw text documents generated for Generative AI pipeline."
)
def text_docs_raw():
  schema = StructType([
    StructField('content', StringType(), True)
  ])
  volume_path = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/*.txt"
  df = (spark.readStream
        .format("text")
        .schema(schema)
        .load(volume_path))
  return df

# COMMAND ----------

@dlt.table(
  name="pdf_docs_raw",
  comment="Raw PDF documents generated for Generative AI pipeline."
)
def pdf_docs_raw():
  volume_path = "/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/*.pdf"
  df = (spark.read
        .format("binaryFile")
        .load(volume_path))
  df_preprocessed = (df.select(F.col("content")
                               .cast("string")))
  return df_preprocessed

# COMMAND ----------

@dlt.table(
  name="csv_docs_raw",
  comment="Raw CSV documents generated for Generative AI pipeline."
)
def csv_docs_raw():
  schema = StructType([
    StructField('content', StringType(), True),
    StructField('content1', StringType(), True),
    StructField('content2', StringType(), True),
    StructField('content3', StringType(), True),
    StructField('content4', StringType(), True),
    StructField('content5', StringType(), True),
    StructField('content6', StringType(), True),
    StructField('content7', StringType(), True),
    StructField('content8', StringType(), True),
    StructField('content9', StringType(), True),
    StructField('content10', StringType(), True),
    StructField('content11', StringType(), True),
    StructField('content12', StringType(), True),
    StructField('content13', StringType(), True),
    StructField('content14', StringType(), True),
    StructField('content15', StringType(), True),
    StructField('content16', StringType(), True),
    StructField('content17', StringType(), True),
    StructField('content18', StringType(), True),
    StructField('content19', StringType(), True),
    StructField('content20', StringType(), True)
  ])
  volume_path = "/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/*.csv"
  df = (spark.readStream
        .format("csv")
        .schema(schema)
        .option("header", False)
        .load(volume_path))
  df_preprocessed = df.select("content")
  return df_preprocessed

# COMMAND ----------

@dlt.table(
  name="text_docs_silver",
  comment="Combined textual documents for Generative AI pipeline."
)
def text_docs_silver():
  text_docs_df = dlt.read("text_docs_raw").withColumn("type", F.lit("text"))
  csv_docs_df = dlt.read("csv_docs_raw").withColumn("type", F.lit("csv"))
  pdf_docs_df = dlt.read("pdf_docs_raw").withColumn("type", F.lit("pdf"))
  combined_df = text_docs_df.union(csv_docs_df).union(pdf_docs_df)
  return combined_df

# COMMAND ----------
