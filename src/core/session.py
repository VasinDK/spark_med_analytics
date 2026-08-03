import logging
import sys
from pyspark.sql import SparkSession


def get_spark_session(config: dict) -> SparkSession:
    app_name = sys.argv[0]

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    logging.getLogger("py4j").setLevel(config["log_level"]["py4j"])

    return spark
