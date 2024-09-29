## Chapter 3 - Managing Data Quality using Delta Live Tables

In this chapter, we're going to cover managing data quality of datasets in a data pipeline. 
We'll introduce Expectations in Delta Live Tables (DLT), which is a way to enforce certain data quality constraints on arriving data before merging the data into downstream tables.

You can always create a new notebook from scratch, but it's recommended to download and import the accompanying notebook samples:

### Technical requirements
To follow along in this chapter, you will need to have Databricks workspace permissions to create and start an all-purpose cluster so that you can execute all of the accompanying notebook cells. You will also need permissions to create and run a new DLT pipeline using a cluster policy. It's recommended to have Unity Catalog permissions to create and use Catalogs, Schemas, and Tables.

### Expected costs
This chapter will create and run several new notebooks and Delta Live Table pipelines using the `Advanced` product edition. As a result, the pipelines are estimated to consume around 10-15 Databricks Units (DBUs).

Please see the Databricks documentation for the latest pricing figures: [Pricing calculator](https://www.databricks.com/product/pricing/product-pricing/instance-types).
