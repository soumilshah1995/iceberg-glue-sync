# Iceberg-Glue-Sync

Iceberg-glue-sync is a Python package that simplifies the process of synchronizing Apache Iceberg tables with AWS Glue Data Catalog. This tool allows you to register one or many Iceberg tables into the Glue Hive metastore, ensuring your Iceberg data is discoverable and queryable through AWS services.

## Features

- Sync multiple Iceberg tables to AWS Glue Data Catalog
- Automatically detect and use the latest Iceberg metadata file
- Support for custom AWS configurations
- Easy-to-use command-line interface

## Installation

Install iceberg-glue-sync using pip:
```
pip install iceberg-glue-sync

```

## Usage

1. Create a YAML configuration file (e.g., `config.yaml`) with your AWS and Iceberg table details:
```

catalog_name: mydatacatalog
aws_account_id: "aws account id"
aws_region: us-east-1

packages:
  - "com.amazonaws:aws-java-sdk-bundle:1.12.661"
  - "org.apache.hadoop:hadoop-aws:3.3.4"
  - "software.amazon.awssdk:bundle:2.29.38"
  - "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1"

databases:
  - name: iceberg_db
    tables:
      - name: customers
        location: s3://<path ot your iceberg>

```
 Run the sync command:
 ```
python run-sync.py --config /Users/soumilshah/IdeaProjects/emr-labs/glue-iceberg-sync/iceberg_sync_config.yaml

```
![image](https://github.com/user-attachments/assets/b7a0f042-806b-4a66-b86a-04af62de2c5e)

