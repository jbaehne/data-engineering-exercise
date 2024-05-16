# Challenge

## How to run the code

The code, configs, etc are all encapsulated into a Docker container. 

Build the image:        `docker built . -t challenge-image`

Run the container:       `docker run -it -t challenge-image bash`

Run the controller script `sh run.sh`

The code simulates the design and architecture with files being pulled and processed into a local datalake (local to the 
container that is). 

## Architecture Overview

The design leverages the common patterns of bronze and silver datalake tables, serving as a general data access, 
analytics, reporting, etc. 

The solution takes a event based approach with the datalake pipelines using the structured streaming api. This provides
flexibility to run these jobs more batch based (say every 30 mins) or always if required. With a checkpointed 
structured streaming application, either case can be satisfied.

S3 is leveraged as the object store and initial landing point for raw data, providing flexibility not only for this pipeline, 
but possible others that have different latency requirements if the framework needs to be extended. 

S3 can be used as a event bus using bucket notifications, sns, sqs, etc. to fan this data out to multiple consumers
as needed. For say a key value store or rdbms if the required SLA times cannot be met directly from the datalake or using
a datalake table as a source.

Data Collection and Data Pipelines (to data systems) are de coupled. S3 is used as a message bus to integrate the 
two pieces.

**DataLake**

A Datalake consisting of Iceberg tables and 2 segments: bronze and silver. The data will reside in S3. AWS Glue
would be used as the Iceberg Catalog. Each zone, environment would have its own bucket and Glue databases.

Bronze tables are "as-is"/"landing tables"; they are the raw data, append only, in a datalake table format, namely Iceberg.

Silver tables are de-duplicated and "lightly curated" (if required); using a merge they also provide idempotency 
for the pipeline and silver tables.

**Data Collection**
 
Data Collection handled by a Data Collection Service. This service is responsible for interacting
with the Open Library API, acquiring json, and writing it to s3 location.

The process is configuration based. As such, collecting numerous subjects can be achieved by running processes 
for each subject. 

Potential Compute Engines:
1. Lambda
2. ECS
3. EKS

**Datalake Pipelines**

The datalake pipelines are responsible for reading the s3 data created by the Data Collection service. 

The pipelines are designed to be event driven, using Spark Structured Streaming sources and sinks.

Potential Compute Engines
1. EMR
2. Databricks
3. EKS

## Python Script
The `collector/main.py` is the script used to pull data from the Open Library API. It uses requests and 
concurrent.futures to obtain files is worker threads.


## Sample Data Output
 
   - Include a simple 'Authors and Books' relation (no need for a full bridge table): report located at ``/opt/spark/work-dir/reports/authors_and_titles``
    

## Data Processing:
   - Write a SQL query (or Python equivalent) to aggregate:
     - The number of books written each year by an author: reported located at `/opt/spark/work-dir/reports/books_by_year`
     - The average number of books written by an author per year: reported located at `/opt/spark/work-dir/reports/avg_books_by_year`
   
   - Discuss how you would optimize this for a larger dataset.
    
   In distributed computing engines, the main problems you generall comes across as you scale data load 
   are spill, shuffle + skew, serialization, storage, and small files.
   
   **For spill**: look into vertically scaling the relevant spark cluster to reduce spill and thereby the cooerlated 
   serialization and GC issues that come from spilling and varying objects being created during spill.
   
   **For shuffle**: look at leverage Spark adaptive query execution to help mitigate skew https://spark.apache.org/docs/latest/sql-performance-tuning.html#spliting-skewed-shuffle-partitions
    
   **For serialization**: Avoid spilling. Remove any UDFs and replace with native functionality.
   
   **For storage**: Analyze the clustering (both partitions and table order) or the data so more data prunes can occur
   with metadata prior to a data scan. 
   
   **For small files**: Frequently run data compaction jobs on all data sets, ideally getting files sizes between 
   128MB and 1GB. This is a general standard to follow.
   
# Production Improvements

## Data Collection
1. **Scheduling**: Looking into potential using a dag tool to coordinate the different pieces of the pipeline.
2. **Parametrization**: The applications should have better parametrization. Noting should be hard coded. 
3. **DLQ and reprocessing**: the collector service should have DLQing when calls to the API fail. The DLQ messages should
contain the required information to reprocess the call. Seperate workers should run to reprocess the 
4. **python package**: The script should be broken down into seperate files and a python package should be created 
for the code that is published to a pypi server.
5. **Test**: Unit tests are 100% must. 

## Data Pipelines

1 **Spark Api**: It may turn out that the advantages and flexability of using the strucuted streaming API may not end up
outweighing some of the additional complexitys of it.
2. **Query Listener**: A query listener is a must for spark strucuted streaming apps. This listener will parse 
spark query event data and send it to a event store / observability tool to perform analysis on.
3. **Abstract common patterns and build libraries** As your code and requirements grow, the simple scripts should
grown into libraries abstracting common patterns and code. Ideally you can get to a point where most user interaction
is via a configuration rather than code itself; that said, there will always be things that may not make the list of
being worth abstracting.
4. **File Source** A file source is used in the bronze processing. While this can be used, often the pull approach
on files is not as ideal as a push approach. THis is due to the listing that is required with a file sink
5. **Test**: Unit tests are 100% must. Also consider running spark locally in your tests to mock scenarios and ensure
that your code procudes the expected results with the mocked scenarios.
 
 
## Observability

Observability is key in all services. Both the collection service and the data pipelines require it.

Generally speaking each service should have varying levels of observability:
1. Infrastructure level: e.g cpu, memory, network, etc
2. Application level: application metrics, logs, etc.
3. Query level: this is relevant to spark and event listener that can be used to capture and transmit telemetry data.


## Terraform

Terraform should be used to create the following resources
1. S3 Bucket
2. Glue Databases
3. EMR Clusters
4. Lambda Deployments (if lambda used).

Generally speaking, terraform should be leveraged to create any objects that will not be mutated by any other
application/service. 

The approach of module (imported by) -> main module should be leveraged to create reusable modules that are callable
across main modules.

## Security

1. Encrypted Data at Rest in S3: S3 Bucket Setting
2. Encrypted Data in motion between spark and S3
3. Web Token auth would be ideal for the collector service and spark applications to avoid the need to rotate keys.

## Data Quality

A data quality framework should be used on silver and bronze tables. 

Some possible candidates are DBT, Elementary, Great Expectations. THese frameworks have large sets of out of the 
box tests that are mainly driven by configurations. One could uses these tests prior to promoting data to layers
where users are 

