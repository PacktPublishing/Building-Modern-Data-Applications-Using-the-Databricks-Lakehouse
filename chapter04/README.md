## Chapter 4 - Scaling DLT Pipelines

In this chapter, we're going to take a look at several methods for scaling your DLT pipelines to handle the processing demands of a typical production environment. 

This chapter includes a hands-on example for setting autoscaling propertyies using the Databricks REST API.

To follow along in this chapter, you will need to have Databricks workspace permissions to create and start an all-purpose cluster.
Furthermore, you will need to download and execute the accompanying notebook samples:

- `Random Taxi Trip Data Generator.py` -  Simulates unpredictable data ingestion that you could expect in a production environment.
- `Taxi Trip Data Pipeline.py` - Sample DLT pipeline that uses Auto Loader to ingest the random taxi trip data from a landing zone.
- `Update Autoscaling Mode.py` - Sample REST request for fetching and updating the autoscaling mode of our DLT clusters.
