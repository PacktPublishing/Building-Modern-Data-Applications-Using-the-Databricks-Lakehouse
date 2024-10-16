# Databricks notebook source
import requests

# Workspace URL copied from the browser URL
databricks_workspace_url = f"https://<yourdatabricksworkspaceurl>"

# Generated personal access token
# https://docs.databricks.com/en/dev-tools/auth/pat.html#databricks-personal-access-tokens-for-workspace-users
pat_token = "<dapi-yourapitoken>"

# Pipeline Id created from the Taxi Trip Data Pipeline
pipeline_id = "<yourpipelineid>"

# Get the current pipeline settings
response = requests.get(
  f"{databricks_workspace_url}/api/2.0/pipelines/{pipeline_id}",
  headers={
    "Authorization": f"Bearer {pat_token}"
  }
)

print(response.status_code)
print(response.json())

# COMMAND ----------

# Update the autoscaling mode pipeline settings
# Note: Use the GET request above to fetch the pipeline JSON settings
response = requests.put(
  f"{databricks_workspace_url}/api/2.0/pipelines/{pipeline_id}",
  headers={
    "Authorization": f"Bearer {pat_token}"
  },
  # Update the following JSON object
  json={
    "pipeline_id":"yourpipelineid",
    "id":"yourpipelineid",
    ...
    "clusters":[
        {
          "label":"default",
          "autoscale":{
              "min_workers":1,
              "max_workers":5,
              "mode":"ENHANCED"
          }
        }
    ],
    ...
})

print(response.status_code)
print(response.json())

# COMMAND ----------
