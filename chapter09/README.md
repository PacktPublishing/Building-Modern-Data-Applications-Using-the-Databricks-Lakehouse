## Chapter 9 - Leveraging Databricks Asset Bundles to Streamline Data Pipeline Deployment

In this chapter, a relatively new **Continuous Integration and Continuous Deployment (CI/CD)** tool, called **Databricks Asset Bundles (DAB)**, which can be leveraged to streamline the development and deployment of data analytical projects across various Databricks workspaces. We'll demonstrate the practical use of DABs through several hands-on exercises, so that you feel comfortable developing your next data analytics projects as a DAB..

To follow along in this chapter, you will need to have workspace administrator permissions to provision new resources in a target Databricks workspace.

You can always create a new notebook from scratch, but it's recommended to download and import the accompanying code samples:

- `01-Hello World` - a "Hello, World!" example DAB for getting started working with DABs to deploy resources in Databricks. 
- `03-GitHub Actions/dab_deployment_workflow.yml` - a sample workflow for incorporating DABs into a CI/CD pipeline using GitHub Actions.

### Technical requirements
To follow along in this chapter, you will need to have Databricks workspace permissions to create and start an all-purpose cluster so that you can execute all of the accompanying notebook cells. You will also need permissions to create and run a new DLT pipeline using a cluster policy. It's recommended to have Unity Catalog permissions to create and use Catalogs, Schemas, and Tables.

### Expected costs
This chapter will create and run several new notebooks and Delta Live Table pipelines using the `Core` product edition. As a result, the pipelines are estimated to consume around 5-10 Databricks Units (DBUs).

Please see the Databricks documentation for the latest pricing figures: [Pricing calculator](https://www.databricks.com/product/pricing/product-pricing/instance-types).
