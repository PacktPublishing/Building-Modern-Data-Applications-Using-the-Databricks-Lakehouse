# Databricks notebook source
import requests

# COMMAND ----------

# Global vars
# Update these according to your own Databricks workspace
workspace_name = "<REPLACE_ME>"
api_token = "<REPLACE_ME>"
fully_qualified_table_name = "<REPLACE_ME>.lineage_demo.combined_table"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetching Table Lineage Info

# COMMAND ----------

def print_table_info(conn_type, table_info_json):
  info = table_info_json["tableInfo"]
  print(f"""
        {conn_type.upper()} Table Connection Info:
        ==========================================
        Table name : {info['name']}
        Catalog name : {info['catalog_name']}
        Table type : {info['table_type']}
        Lineage timestamp {info['lineage_timestamp']}
        """)

def print_notebook_info(conn_type, notebook_info_json):
  print(f"""
        {conn_type.upper()} Notebook Connection Info:
        ==========================================
        Workspace id : {str(notebook_info_json['workspace_id'])}
        Notebook id : {str(notebook_info_json['notebook_id'])}
        Lineage timestamp {notebook_info_json['lineage_timestamp']}
        """)

# COMMAND ----------

response = requests.get(
  f"https://{workspace_name}.cloud.databricks.com/api/2.0/lineage-tracking/table-lineage",
  headers={
    "Authorization": f"Bearer {api_token}"
  },
  json={
    "table_name": fully_qualified_table_name,
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
table_cols = spark.table(fully_qualified_table_name).columns
for column_name in table_cols:
  print(f"Column : {fully_qualified_table_name}.{column_name}:")
  print("===============================================================")
  response = requests.get(
    f"https://{workspace_name}.cloud.databricks.com/api/2.0/lineage-tracking/column-lineage",
    headers={
      "Authorization": f"Bearer {api_token}"
    },
    json={
      "table_name": fully_qualified_table_name,
      "column_name": column_name
    }
  )
  if response.status_code == 200:
    if "upstream_cols" in response.json():
      print("Upstream cols:")
      for column_info in response.json()['upstream_cols']:
        print_column_info("Upstream", column_info)
    if "downstream_cols" in response.json():
      print("Downstream cols:")
      for column_info in response.json()['downstream_cols']:
        print_column_info("Downstream", column_info)

  print("\n")

# COMMAND ----------
