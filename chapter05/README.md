## Chapter 5 - Mastering Data Governance in the Lakehouse with Unity Catalog

In this chapter, we're going dive into the implementation of an effective data governance pattern using Unity Catalog in Databricks.
We’ll cover Unity Catalog workspace setup, data cataloging, row and column-level data access control, and data lineage.
By the end of the chapter, you’ll be armed with best practices for data governance, and real-world insights, providing a comprehensive guide for data engineers seeking to enhance data governance and compliance.

To follow along in this chapter, you will need to have Databricks workspace permissions to create and start an all-purpose cluster.

Furthermore, you will need to download and execute the accompanying notebook samples:

- `Dynamic Views Demo.py` - Sample Python notebook that implements row and column level access control using a dynamic view.
- `Data Lineage Demo.sql` - Sample SQL notebook that creates parent-child tables for simulating data lineage tracking in Databricks.

### Technical requirements
To follow along in this chapter, you will need to have Databricks workspace permissions to create and start an all-purpose cluster so that you can execute all of the accompanying notebook cells. It's also recommended that your Databricks user be elevated to an account admin and a metastore admin so that you can deploy a new Unity Catalog Metastore and attach it to your existing Databricks workspace.

### Expected costs
This chapter will create and run several new notebooks on an all-purpose cluster. As a result, the hands-on exercises are estimated to consume around 5-10 Databricks Units (DBUs).

Please see the Databricks documentation for the latest pricing figures: [Pricing calculator](https://www.databricks.com/product/pricing/product-pricing/instance-types).
