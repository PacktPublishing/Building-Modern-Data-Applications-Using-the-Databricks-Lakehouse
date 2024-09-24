# Databricks notebook source
# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install faker reportlab

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Documents Volume

# COMMAND ----------

# Globals
# ** IMPORTANT NOTE: **
# Replace these values to match your environment
catalog_name = "<REPLACE_ME>"
schema_name = "<REPLACE_ME>"
volume_name = "<REPLACE_ME>"
storage_location = "<REPLACE_ME>"


# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"""
  CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
  COMMENT 'External volume for storing miscellaneous documents'
  LOCATION '{storage_location}'
""")

# COMMAND ----------

display(
  spark.sql(f"SHOW VOLUMES IN {catalog_name}.{schema_name}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate documents

# COMMAND ----------

from shutil import copyfile


def save_doc_as_text(file_name, save_path, paragraph):
  """Helper function that saves a paragraph of text as a text file"""
  tmp_path = f"/local_disk0/tmp/{file_name}"
  volume_path = f"{save_path}/{file_name}" 
  print(f"Saving text file at : {tmp_path}")
  txtfile = open(tmp_path, "a")
  txtfile.write(paragraph)
  txtfile.close()
  copyfile(tmp_path, volume_path)


def save_doc_as_pdf(file_name, save_path, paragraph):
  """Helper function that saves a paragraph of text as a PDF file"""
  from reportlab.pdfgen.canvas import Canvas
  from reportlab.lib.pagesizes import letter
  from reportlab.lib.units import cm
  tmp_path = f"/local_disk0/tmp/{file_name}"
  volume_path = f"{save_path}/{file_name}" 
  canvas = Canvas(tmp_path, pagesize=letter)
  lines = paragraph.split(".")
  textobject = canvas.beginText(5*cm, 25*cm)
  for line in lines:
    textobject.textLine(line)
    canvas.drawText(textobject)
  canvas.save()
  print(f"Saving PDF file at : {tmp_path}")
  copyfile(tmp_path, volume_path)


def save_doc_as_csv(file_name, save_path, paragraph):
  """Helper function that saves a paragraph of text as a CSV file"""
  import csv
  tmp_path = f"/local_disk0/tmp/{file_name}"
  volume_path = f"{save_path}/{file_name}" 
  print(f"Saving CSV file at : {tmp_path}")
  with open(tmp_path, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Id", "Sentence"])
    i = 1
    for line in paragraph.split("."):
      writer.writerow([i, line])
      i = i + 1
  copyfile(tmp_path, volume_path)

# COMMAND ----------

from faker import Faker
import time
import random

Faker.seed(610)
fake = Faker()

# Randmonly generate documents
num_docs = 5 
num_sentences_per_doc = 100
doc_types = ["txt", "pdf", "csv"]
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
for _ in range(num_docs):
    paragraph = fake.paragraph(nb_sentences=num_sentences_per_doc)
    print(paragraph)

    # Randomly choose a document format type
    doc_type = doc_types[random.randrange(2)]
    print(doc_type)
    if doc_type == "txt":
        doc_name = f"{fake.pystr()}.txt"
        save_doc_as_text(doc_name, volume_path, paragraph)
    elif doc_type == "pdf":
        doc_name = f"{fake.pystr()}.pdf"
        save_doc_as_pdf(doc_name, volume_path, paragraph)
    elif doc_type == "csv":
        doc_name = f"{fake.pystr()}.csv"
        save_doc_as_csv(doc_name, volume_path, paragraph)

    # Sleep for a random interval
    sleep_time = random.randint(3, 30)
    print(f"Sleeping for {sleep_time} seconds...\n\n")
    time.sleep(sleep_time)

# COMMAND ----------

spark.sql(f"""LIST '/Volumes/{catalog_name}/{schema_name}/{volume_name}/'""").display()

# COMMAND ----------
