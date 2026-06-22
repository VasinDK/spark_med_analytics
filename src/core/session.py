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
    
    spark = SparkSession.builder \
    .appName(app_name) \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()
    
    config = configuration.load_s3_yaml_config(spark, config_yaml)
    logging.getLogger("py4j").setLevel(config["log_level"]["py4j"])
    
    return spark, config