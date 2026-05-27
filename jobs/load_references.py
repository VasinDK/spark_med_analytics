import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, DateType, ArrayType
from pyspark.sql.functions import col, to_date

def run_etl():
    logging.getLogger("py4j").setLevel(logging.ERROR)

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

    bronze_schema_ref_book = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False)
    ])

    bronze_path_departments = "s3a://spark-medanalytics-dev-bronze/departments.csv"
    bronze_path_professions = "s3a://spark-medanalytics-dev-bronze/professions.csv"

    df_raw_departments = spark.read.schema(bronze_schema_ref_book) \
        .option("header", True) \
        .option("delimiter", ";") \
        .csv(bronze_path_departments)
    df_raw_professions = spark.read.schema(bronze_schema_ref_book) \
        .option("header", True) \
        .option("delimiter", ";") \
        .csv(bronze_path_professions)

    df_raw_departments.createOrReplaceTempView("temp_df_departments")
    df_raw_professions.createOrReplaceTempView("temp_df_professions")

    departments_table = "yandex.silver.departments"
    professions_table = "yandex.silver.professions"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {departments_table} (
            id INT,
            name STRING
        )
        USING iceberg
    """)

    spark.sql(f"""
        MERGE INTO {departments_table} t
        USING temp_df_departments td
        ON t.id = td.id
        WHEN MATCHED THEN UPDATE 
            SET t.name = td.name
        WHEN NOT MATCHED THEN 
            INSERT (id, name) VALUES (td.id, td.name)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {professions_table} (
            id INT,
            name STRING
        )
        USING iceberg
    """)

    spark.sql(f"""
        MERGE INTO {professions_table} t
        USING temp_df_professions tp
        ON t.id = tp.id
        WHEN MATCHED THEN UPDATE SET t.name = tp.name
        WHEN NOT MATCHED THEN INSERT (id, name) VALUES (tp.id, tp.name)
    """)

    spark.stop()

if __name__ == "__main__":
    run_etl()