import logging
import src.config as configuration
from typing import Any, Tuple
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> Tuple[SparkSession, Any]:
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    config = configuration.load_s3_yaml(spark)
    logging.getLogger("py4j").setLevel(config["log_level"]["py4j"])
    
    return spark, config

def build_s3_path(s3_file_config: dict) -> str:
    bucket = s3_file_config["bucket"].strip("/")
    path = s3_file_config["path"].lstrip("/") 
    
    return f"{bucket}/{path}"

def read_s3_csv(spark, s3_path, schema, has_header=True, delimiter=";"):
    return (spark.read
            .schema(schema)
            .option("header", has_header)
            .option("delimiter", delimiter)
            .csv(s3_path))

def upsert_iceberg_table_reference(spark, target_table, source_view):
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                id INT,
                name STRING
            )
            USING iceberg
        """)

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {source_view} ts
        ON t.id = ts.id
        WHEN MATCHED THEN UPDATE 
            SET t.name = ts.name
        WHEN NOT MATCHED THEN 
            INSERT (id, name) VALUES (ts.id, ts.name)
    """)

def getConfSpark(spark):
    logger = logging.getLogger("getConfSpark")

    logger.info("==================================================")
    logger.info("CHECKING SPARK CONFIGURATION AT STARTUP:")
    
    extensions = spark.conf.get("spark.sql.extensions", "NOT FOUND")
    logger.info(f"Spark SQL Extensions: {extensions}")
    
    all_confs = spark.sparkContext.getConf().getAll()
    iceberg_confs = {k: v for k, v in all_confs if "spark.sql.catalog" in k}
    
    if iceberg_confs:
        for key, value in iceberg_confs.items():
            logger.info(f"{key} = {value}")
    else:
        logger.error("CRITICAL WARNING: Iceberg directory settings are missing from the session!")
        
    logger.info("==================================================")