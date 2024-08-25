## Chapter 7 - Viewing Data Lineage using Unity Catalog

In this chapter, we dive into tracing data lineage in the Databricks Intelligence Platform. You’ll learn how to trace data origins, visualize dataset transformations, identify upstream and downstream dependencies, and document lineage using the Lineage Graph capabilities of the Catalog Explorer.

To follow along in this chapter, you will need to have permissions to create catalogs, schemas, and tables, as well as permissions to create and start an all-purpose cluster.

Furthermore, you will need to download and execute the accompanying notebook samples:

- `01-Lineage Data Generator.py` - Data generator notebook that creates several tables used to explore the Lineage Graphing capabilities of the Catalog Explorer.
- `02-Working with the Data Lineage REST API.py` - Sample notebook that contains sample requests to the Data Lineage REST API.
- `Predicting Carbon Footprint.py` - Sample DLT pipeline notebook that ingest commercial airline flight data from `/databricks-datasets` directory.

### Technical requirements
To follow along in this chapter, you will need to have Databricks workspace permissions to create and start an all-purpose cluster so that you can execute all of the accompanying notebook cells. It's recommended to have Unity Catalog permissions to create and use Catalogs, Schemas, and Tables.

### Expected costs
This chapter will create and run several new notebooks that consume the Databricks REST API. As a result, the all-purpose cluster is estimated to consume around 5-10 Databricks Units (DBUs).

Please see the Databricks documentation for the latest pricing figures: [Pricing calculator](https://www.databricks.com/product/pricing/product-pricing/instance-types).

