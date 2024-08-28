## Chapter 8 - Deploying, Maintaining, and Administrating DLT Pipelines using Terraform

In this chapter, we’re going to explore how an automation tool like Terraform can be used to express data pipelines as code, or Infrastructure as Code (IaC), using Terraform.

To follow along in this chapter, you will need to have workspace administrator permissions to provision new resources in a target Databricks workspace.

You can always create a new notebook from scratch, but it's recommended to download and import the accompanying notebook samples:

### Technical requirements
To follow along in this chapter, you will need to have Databricks workspace permissions to create and start an all-purpose cluster so that you can execute all of the accompanying notebook cells. You will also need permissions to create and run a new DLT pipeline using a cluster policy. It's recommended to have Unity Catalog permissions to create and use Catalogs, Schemas, and Tables.

### Expected costs
This chapter will create and run several new notebooks and Delta Live Table pipelines using the `Core` product edition. As a result, the pipelines are estimated to consume around 10-15 Databricks Units (DBUs).

Please see the Databricks documentation for the latest pricing figures: [Pricing calculator](https://www.databricks.com/product/pricing/product-pricing/instance-types).
