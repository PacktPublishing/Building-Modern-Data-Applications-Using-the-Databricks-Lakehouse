# Databricks notebook source
# MAGIC %md
# MAGIC ## Using the Python `requests` library
# MAGIC We will be exclusivley using the Python `requests` library in this notebook to consume the Databricks Data Lineage REST API. Let's import the module to begin working with our REST endpoints.

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMPORTANT NOTE
# MAGIC This notebook is dependent upon running the data generator notebook to create UC artifacts which will be used in the following REST API calls.
# MAGIC
# MAGIC Please ensure that you've successfully run the the data generator notebook first.

# COMMAND ----------

# Global variables
# Update these according to your own Databricks workspace

WORKSPACE_NAME = "<REPLACE_ME>"

# DB Secrets are recommended way to protect the API tokens
# Docs: https://docs.databricks.com/en/security/secrets/secrets.html
API_TOKEN = dbutils.secrets.get("api_token_scope", "api_token")

CATALOG_NAME = "chp7_modern_data_apps_databricks_lakehouse"
FULLY_QUALIFIED_TABLE_NAME = f"{CATALOG_NAME}.lineage_demo.combined_table"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetching Table Lineage Info
# MAGIC Let's add a few helper functions that will parse the JSON response payload from the Data Lineage REST API and print the results in a prettier way.

# COMMAND ----------

def print_table_info(conn_type, table_info_json):
  info = table_info_json["tableInfo"]
  print(f"""
        +----------------------------------------------------+
        | {conn_type.upper()} Table Connection Info
        |----------------------------------------------------|
        | Table name : {info['name']}
        |----------------------------------------------------|
        | Catalog name : {info['catalog_name']}
        |----------------------------------------------------|
        | Table type : {info['table_type']}
        |----------------------------------------------------|
        | Lineage timestamp : {info['lineage_timestamp']}
        +----------------------------------------------------+
        """)
  if conn_type.upper() == "UPSTREAMS":
      print(f"""
                                |
                               \|/
        """)

def print_notebook_info(conn_type, notebook_info_json):
  print(f"""
        +----------------------------------------------------+
        | {conn_type.upper()} Notebook Connection Info:
        |----------------------------------------------------|
        | Workspace id : {str(notebook_info_json['workspace_id'])}
        |----------------------------------------------------|
        | Notebook id : {str(notebook_info_json['notebook_id'])}
        |----------------------------------------------------|
        | Lineage timestamp :  {notebook_info_json['lineage_timestamp']}
        +----------------------------------------------------+
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, let's call the Data Lineage API to retrieve the Table lineage information.

# COMMAND ----------

response = requests.get(
  f"https://{WORKSPACE_NAME}.cloud.databricks.com/api/2.0/lineage-tracking/table-lineage",
  headers={
    "Authorization": f"Bearer {API_TOKEN}"
  },
  json={
    "table_name": FULLY_QUALIFIED_TABLE_NAME,
    "include_entity_lineage": "true"
  }
)
if response.status_code == 200:
  connection_flows = ["upstreams", "downstreams"]
  for flow in connection_flows:
    if flow in response.json():
      connections = response.json()[flow]
      for conn in connections:
        if "tableInfo" in conn:
          print_table_info(flow, conn)
        elif "notebookInfos" in conn:
          for notebook_info in conn["notebookInfos"]:
            print_notebook_info(flow, notebook_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetching Column Lineage Info
# MAGIC Let's also define another helper function to display the column connection information nicely JSON.

# COMMAND ----------

def print_column_info(conn_type, column_info_json):
  print(f"""
        Connection flow: {conn_type.upper()}
        Column name : {column_info_json['name']}
        Catalog name : {column_info_json['catalog_name']}
        Schema name : {column_info_json['schema_name']}
        Table name : {column_info_json['table_name']}
        Table type : {column_info_json['table_type']}
        Lineage timestamp : {column_info_json['lineage_timestamp']}
        """)

# COMMAND ----------

# First, let's start off by retrieving the table columns
table_cols = spark.table(FULLY_QUALIFIED_TABLE_NAME).columns
for column_name in table_cols:
  print("+------------------------------------------------------------------------------+")
  print(f"| Column : {FULLY_QUALIFIED_TABLE_NAME}.{column_name}:")
  print("|------------------------------------------------------------------------------|")
  response = requests.get(
    f"https://{WORKSPACE_NAME}.cloud.databricks.com/api/2.0/lineage-tracking/column-lineage",
    headers={
      "Authorization": f"Bearer {API_TOKEN}"
    },
    json={
      "table_name": FULLY_QUALIFIED_TABLE_NAME,
      "column_name": column_name
    }
  )
  if response.status_code == 200:
    if "upstream_cols" in response.json():
      print("| Upstream cols:")
      for column_info in response.json()['upstream_cols']:
        print_column_info("Upstream", column_info)
    if "downstream_cols" in response.json():
      print("| Downstream cols:")
      for column_info in response.json()['downstream_cols']:
        print_column_info("Downstream", column_info)
  print("+------------------------------------------------------------------------------+")

  print("\n")
