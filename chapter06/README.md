## Chapter 6 - Managing Data Locations in Unity Catalog

In this chapter, we’ll explore how to effectively manage input and output data locations using Unity Catalog.

To follow along in this chapter, you will need to have administrator permissions to create external locations, storage credentials, foreign connections, as well as permissions to create and start an all-purpose cluster.

Furthermore, you will need to download and execute the accompanying notebook samples:

- `Generate Fake Text Documents.py` - Sample Python notebook that randomly generates text, PDF, and CSV documents and saves it to a Databricks storage voloume.
- `Preprocess Text Documents.py` - Contains the DLT pipeline definition for a document processing pipeline which has applications in use cases like Generative AI.

### Technical requirements
To follow along in this chapter, you will need to have Databricks workspace permissions to create and start an all-purpose cluster so that you can execute all of the accompanying notebook cells. You will also need permissions to create and run a new DLT pipeline using a cluster policy. It's recommended to have Unity Catalog permissions to create and use Catalogs, Schemas, and Tables.

### Expected costs
This chapter will create and run several new notebooks and a Delta Live Table pipeline using the `Core` product edition. As a result, the pipelines are estimated to consume around 5-10 Databricks Units (DBUs).

Please see the Databricks documentation for the latest pricing figures: [Pricing calculator](https://www.databricks.com/product/pricing/product-pricing/instance-types).
