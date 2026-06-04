import logging
import src.config as configuration
from src.exceptions import ConfigurationNotFoundError
from typing import Any, Tuple
from pyspark.sql import SparkSession

def get_spark_session(args: list) -> Tuple[SparkSession, Any]:
    if len(args) < 2:
        raise ConfigurationNotFoundError()
    
    app_name = args[0]
    config_yaml = args[1]
    
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    
    config = configuration.load_s3_yaml(spark, config_yaml)
    logging.getLogger("py4j").setLevel(config["log_level"]["py4j"])
    
    return spark, config

def build_s3_path(s3_file_config: dict) -> str:
    bucket = s3_file_config["bucket"].strip("/")
    path = s3_file_config["path"].lstrip("/") 
    
    return f"s3a://{bucket}/{path}"

def read_s3_csv(spark, s3_path, schema, has_header=True, delimiter=";"):
    return (spark.read
            .schema(schema)
            .option("header", has_header)
            .option("delimiter", delimiter)
            .csv(s3_path))

def upsert_iceberg_table(spark: SparkSession, target_table: str, source_view: str, key_col: str = "id"):
    source_df = spark.table(source_view)
    
    fields_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in source_df.schema])
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            {fields_sql}
        )
        USING iceberg
    """)

    data_cols = [f.name for f in source_df.schema if f.name != key_col]
    
    set_expr = ", ".join([f"t.{col} = ts.{col}" for col in data_cols])
    all_cols = ", ".join([key_col] + data_cols)
    values_expr = ", ".join([f"ts.{col}" for col in [key_col] + data_cols])

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {source_view} ts
        ON t.{key_col} = ts.{key_col}
        WHEN MATCHED THEN UPDATE 
            SET {set_expr}
        WHEN NOT MATCHED THEN 
            INSERT ({all_cols}) VALUES ({values_expr})
    """)