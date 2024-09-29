terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_current_user" "my_user" {

}

resource "databricks_notebook" "dlt_pipeline_notebook" {
  path = "${data.databricks_current_user.my_user.home}/chp_8_terraform/taxi_trips_pipeline.py"
  language = "PYTHON"
  content_base64 = base64encode(<<-EOT
    import dlt
    import pyspark.sql.functions as F

    @dlt.table(
        name="yellow_taxi_bronze",
        comment="The raw NYC taxi cab trip dataset located in `/databricks-datasets/`"
    )
    def yellow_taxi_bronze():
        path = "/databricks-datasets/nyctaxi/tripdata/yellow"
        schema = "vendor_id string, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count integer, trip_distance float, pickup_longitude float, pickup_latitude float, rate_code integer, store_and_fwd_flag integer, dropoff_longitude float, dropoff_lattitude float, payment_type string, fare_amount float, surcharge float, mta_tax float, tip_amount float, tolls_amount float, total_amount float"
        return (spark.readStream
                    .schema(schema)
                    .format("csv")
                    .option("header", True)
                    .load(path))

    @dlt.table(
        name="yellow_taxi_silver",
        comment="Financial information from incoming taxi trips.")
    @dlt.expect_or_fail("valid_total_amount", "total_amount > 0.0")
    def yellow_taxi_silver():
        return (dlt.readStream("yellow_taxi_bronze")
                    .withColumn("driver_payment", F.expr("total_amount * 0.40"))
                    .withColumn("vehicle_maintenance_fee", F.expr("total_amount * 0.05"))
                    .withColumn("adminstrative_fee", F.expr("total_amount * 0.1"))
                    .withColumn("potential_profits", F.expr("total_amount * 0.45")))
    EOT
  )
}

resource "databricks_catalog" "dlt_target_catalog" {
  name = "chp8_deploying_pipelines_w_terraform"
  comment = "The target catalog for Taxi Trips DLT pipeline"
}

resource "databricks_schema" "dlt_target_schema" {
  catalog_name = databricks_catalog.dlt_target_catalog.id
  name = "terraform_demo"
  comment = "The target schema for Taxi Trips DLT pipeline"
}

resource "databricks_pipeline" "taxi_trips_pipeline" {
  name = "Taxi Trips Pipeline"
  library {
    notebook {
      path = "${data.databricks_current_user.my_user.home}/chp_8_terraform/taxi_trips_pipeline.py"
    }
  }
  cluster {
    label = "default"
    num_workers = 2
    autoscale {
      min_workers = 2
      max_workers = 4
      mode = "ENHANCED"
    }
    driver_node_type_id = "i3.2xlarge"
    node_type_id = "i3.xlarge"
  }
  continuous = false
  development = true
  photon = false
  serverless = false
  catalog = databricks_catalog.dlt_target_catalog.name
  target = databricks_schema.dlt_target_schema.name
  edition = "ADVANCED"
  channel = "CURRENT"
}

resource "databricks_job" "taxi_trips_pipeline_job" {
  name = "Taxi Trips Pipeline Update Job"
  description = "Databricks Workflow that executes a pipeline update of the Taxi Trips DLT pipeline."
  job_cluster {
    job_cluster_key = "taxi_trips_pipeline_update_job_cluster"
    new_cluster {
      num_workers = 2
      spark_version = "15.4.x-scala2.12"
      node_type_id  = "i3.xlarge"
      driver_node_type_id = "i3.2xlarge"
    }
  }
  task {
    task_key = "update_taxi_trips_pipeline"
    pipeline_task {
      pipeline_id = databricks_pipeline.taxi_trips_pipeline.id
    }
  }
  trigger {
    pause_status = "PAUSED"
    periodic {
      interval = "1"
      unit = "HOURS"
    }
  }
}

output "workflow_url" {
  value = databricks_job.taxi_trips_pipeline_job.url
}
