import os
import boto3
import yaml
import argparse
from urllib.parse import urlparse
from pyspark.sql import SparkSession


def create_spark_session(config, spark_config, protocol):
    builder = SparkSession.builder

    if protocol == "s3a":
        default = {
            "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "spark.hadoop.fs.s3a.endpoint": f"s3.{config['aws_region']}.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        }
        for key, value in default.items():
            builder = builder.config(key, value)

    for key, value in spark_config.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def get_latest_metadata_file(iceberg_path):
    bucket_name = urlparse(iceberg_path).netloc
    path_without_bucket = iceberg_path.replace(f's3://{bucket_name}/', '')
    metadata_prefix = f"{path_without_bucket.strip('/')}/metadata/"

    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
        metadata_files = [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'].endswith('.metadata.json')
        ]

        if metadata_files:
            return f"s3://{bucket_name}/{sorted(metadata_files)[-1]}"
        else:
            print(f"No metadata files found for {iceberg_path}")
            return None
    except Exception as e:
        print(f"Error accessing S3 for {iceberg_path}: {str(e)}")
        return None


def sync_iceberg_table(spark, catalog_name, database_name, table_name, iceberg_location):
    latest_metadata = get_latest_metadata_file(iceberg_location)
    if latest_metadata:
        try:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database_name}")

            # Check if the table already exists
            table_exists = spark.sql(f"SHOW TABLES IN {catalog_name}.{database_name}").filter(
                f"tableName = '{table_name}'").count() > 0

            if not table_exists:
                spark.sql(f"""
                CALL {catalog_name}.system.register_table(
                  table => '{database_name}.{table_name}',
                  metadata_file => '{latest_metadata}'
                )
                """)
                print(f"Successfully synced table {database_name}.{table_name}")
            else:
                print(f"Table {database_name}.{table_name} already exists, skipping registration")
        except Exception as e:
            print(f"Error syncing table {database_name}.{table_name}: {str(e)}")
    else:
        print(f"Skipping table {database_name}.{table_name} due to missing metadata")


def main(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    catalog_name = config['catalog_name']
    aws_account_id = config['aws_account_id']
    aws_region = config['aws_region']
    packages = config.get('packages', [])

    conf = {
        "spark": {
            "spark.app.name": "iceberg_lab",
            "spark.jars.packages": ",".join(packages),
            "spark.sql.defaultCatalog": catalog_name,
            "spark.sql.catalog.mydatacatalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.mydatacatalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.mydatacatalog.warehouse": f"s3://{aws_account_id}/iceberg-warehouse/",
            "spark.sql.catalog.mydatacatalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        }
    }

    spark = create_spark_session(config, spark_config=conf.get("spark"), protocol="s3a")

    for database in config['databases']:
        database_name = database['name']
        for table in database['tables']:
            sync_iceberg_table(spark, catalog_name, database_name, table['name'], table['location'])

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Iceberg tables with Glue catalog")
    parser.add_argument("--config", required=True, help="Path to the YAML configuration file")
    args = parser.parse_args()

    main(args.config)
