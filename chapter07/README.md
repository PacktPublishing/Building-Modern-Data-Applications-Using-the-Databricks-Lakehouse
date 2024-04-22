## Chapter 7 - Viewing Data Lineage using Unity Catalog

In this chapter, we dive into tracing data lineage in the Databricks Intelligence Platform. You’ll learn how to trace data origins, visualize dataset transformations, identify upstream and downstream dependencies, and document lineage using the Lineage Graph capabilities of the Catalog Explorer.

To follow along in this chapter, you will need to have permissions to create catalogs, schemas, and tables, as well as permissions to create and start an all-purpose cluster.

Furthermore, you will need to download and execute the accompanying notebook samples:

- `Lineage Demo using Databricks REST API.py` - Data generator notebook that creates several tables used to explore the Lineage Graphing capabilities of the Catalog Explorer.
- `Predicting Carbon Footprint.py` - Sample DLT pipeline notebook that ingest commercial airline flight data from `/databricks-datasets` directory.
- `Working with the Databricks Lineage API.py` - Sample notebook that contains sample requests to the Lineage Tracking REST API.
