from src.logging_config import setup_logging
from src.decorators import monitor_job
import sys
from src.utils import get_spark_session, build_s3_path
from pyspark.sql import SparkSession

@monitor_job
def run_etl_gold():
    spark, config = get_spark_session(sys.argv)

# DOTO
# - Получить данные из silver
# - Поло

if __name__ == "__main__":
    setup_logging()
    run_etl_gold()