from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, DateType, ArrayType
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("BronzeToSilverIcebergJob") \
    .config("spark.sql.catalog.yandex", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.yandex.type", "hadoop") \
    .config("spark.sql.catalog.yandex.warehouse", "s3a://spark-medanalytics-dev-silver/warehouse/") \
    .config("spark.sql.catalog.yandex.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.yandex.s3.endpoint", "https://storage.yandexcloud.net") \
    .config("spark.sql.catalog.yandex.s3.path-style-access", "true") \
    .config("spark.sql.catalog.yandex.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.logLevel", "WARN") \
    .getOrCreate()

print("=== СТАРТ ПРОЦЕССА process ===")

target_table = "yandex.silver.patient_visits"

spark.sql(f"TRUNCATE TABLE {target_table}")

print("=== ETL ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО ===")
spark.stop()
