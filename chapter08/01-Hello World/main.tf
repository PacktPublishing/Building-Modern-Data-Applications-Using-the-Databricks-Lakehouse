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
  path     = "${data.databricks_current_user.my_user.home}/chp_8_terraform/my_simple_dlt_pipeline.py"
  language = "PYTHON"
  content_base64 = base64encode(<<-EOT
    import dlt

    @dlt.table(
        comment="The raw NYC taxi cab trip dataset located in `/databricks-datasets/`"
    )
    def yellow_taxi_raw():
        path = "/databricks-datasets/nyctaxi/tripdata/yellow"
        schema = "vendor_id string, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count integer, trip_distance float, pickup_longitude float, pickup_latitude float, rate_code integer, store_and_fwd_flag integer, dropoff_longitude float, dropoff_lattitude float, payment_type string, fare_amount float, surcharge float, mta_tax float, tip_amount float, tolls_amount float, total_amount float"
        return (spark.readStream
                    .schema(schema)
                    .format("csv")
                    .option("header", True)
                    .load(path))
    EOT
  )
}

output "notebook_url" {
 value = databricks_notebook.dlt_pipeline_notebook.url
}
