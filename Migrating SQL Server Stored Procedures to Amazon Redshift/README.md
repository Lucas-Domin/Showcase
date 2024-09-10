# Overview
The business requirement was to migrate data from an on-premises SQL Server database to AWS, adhering strictly to the client's constraint that all data extractions be performed exclusively through stored procedures. The client specified that data could only be queried and extracted from their database using these procedures, presenting a unique challenge to design an ETL process that complies with these data access protocols while ensuring efficient data flow and management in the cloud environment.

To meet this requirement, the solution utilizes a Site-to-Site VPN to securely connect AWS Glue jobs with the on-premises SQL Server, enabling the execution of stored procedures and the direct retrieval of data into AWS. 

# Data Pipeline Architecture
SQL Server (Source) > Raw Layer > Stage Layer > Analytics Layer > Data Loading Layer > Amazon Redshift (Target)
## Raw Layer
The Raw Layer serves as the entry point for data ingestion, where AWS Glue jobs dynamically execute stored procedures against the SQL Server to extract data. The data extracted is stored in Amazon S3 in two distinct formats: 'latest,' which captures the most recent execution results for operational reporting, and 'snapshot,' which archives historical data to fulfill compliance and auditing needs. For each execution of a stored procedure, logs are generated and stored in a dedicated S3 bucket, capturing the details of the latest execution logs for each stored procedure. This logging mechanism provides visibility into the data extraction process and helps maintain data lineage.

To ensure consistency, AWS Glue jobs dynamically cast data types based on schema metadata retrieved from the AWS Glue Data Catalog. The processed data is then stored in a compressed Parquet format, optimizing it for performance in subsequent transformations and queries.

## Stage Layer
The Stage Layer is designed to handle intermediate data processing and enrichment tasks. This layer involves two types of AWS Glue jobs: one that focuses on copying the latest data for routine processing and another that is tailored for handling temporal data, such as month-based data partitioning. The temporal data processing jobs structure the data into organized year/month directories within S3, allowing for efficient partitioning and retrieval of time-series data. This layer provides a foundational preparation step, enabling more complex transformations and aggregations in the Analytics Layer.

## Analytics Layer
The Analytics Layer focuses on combining, refining, and structuring data into a final format suitable for business intelligence and analytical purposes. Here, the data undergoes a series of complex transformations, primarily involving the consolidation and enrichment of various data sources. AWS Glue jobs in this layer execute SQL queries that join and aggregate data from different tables extracted in the previous stages, including handling specific transformations like renaming fields, trimming spaces, and performing mathematical calculations to derive new metrics. The jobs also generate unified datasets, such as dimension and fact tables, by merging data from multiple sources and handling discrepancies in data fields, ensuring that the resulting datasets are normalized, consistent, and optimized for analytical workloads. These processed datasets are then prepared for the Data Loading Layer to be ingested into Amazon Redshift, where they can be queried efficiently for reporting and analytics.

## Data Loading Layer
The Data Loading Layer is focused on efficiently loading the transformed data into Amazon Redshift, ensuring the data warehouse is prepared for high-performance analytical queries. This layer comprises jobs that handle both dimension and fact data loading operations. Dimension table jobs truncate and ingest new data to maintain integrity and accuracy, while fact table jobs are designed to perform incremental loads based on unique date keys. This strategy enables the efficient management of large volumes of historical data, ensuring that the Redshift data warehouse remains current, consistent, and performant.
<p align="center">
  <img src="https://github.com/user-attachments/assets/f57861cb-bff0-4c6d-8341-0183a4871485" alt="image">
</p>

## Step Functions Orchestration
The Step Functions Orchestration layer is responsible for managing and coordinating the entire ETL pipeline from data ingestion to final loading into Amazon Redshift. The orchestration is designed for modularity, scalability, and fault tolerance. The master Step Function controls multiple sub-step functions, each focusing on a specific layer—Raw, Stage, Analytics, and Data Loading.

After each layer completes processing, the master Step Function triggers Glue Crawlers to update the Glue Data Catalog with the newly processed data, ensuring metadata consistency for future steps. Within each sub-step function, parallel AWS Glue jobs are orchestrated by forwarding different JSON API values as job parameters, allowing the same job to process multiple tables from the Glue Data Catalog concurrently. This dynamic orchestration improves efficiency and optimizes resource usage. To enhance observability, Amazon SNS notifications are integrated for real-time monitoring and alerting, providing visibility into job statuses and enabling quick troubleshooting in the event of any failures.
<p align="center">
![Architecture](https://github.com/user-attachments/assets/fd0801d4-bf2e-4812-a0da-678bb0d3ee79)
</p>
