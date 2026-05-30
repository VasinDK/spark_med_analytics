import logging
from pyspark.sql import SparkSession
import src.config as config

def get_spark_session(app_name: str) -> SparkSession:
    logging.getLogger("py4j").setLevel(logging.ERROR)

    return SparkSession.builder \
        .appName(app_name) \
        .config(f"spark.sql.catalog.{config.DB_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{config.DB_CATALOG}.type", "hadoop") \
        .config(f"spark.sql.catalog.{config.DB_CATALOG}.warehouse", config.SILVER_WAREHOUSE) \
        .config(f"spark.sql.catalog.{config.DB_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config(f"spark.sql.catalog.{config.DB_CATALOG}.s3.endpoint", config.S3_STORAGE) \
        .config(f"spark.sql.catalog.{config.DB_CATALOG}.s3.path-style-access", "true") \
        .config(f"spark.sql.catalog.{config.DB_CATALOG}.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.logLevel", "WARN") \
        .getOrCreate()