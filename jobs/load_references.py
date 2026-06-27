import sys
from src.logging_config import setup_logging
from src.core.session import get_spark_session
from src.core.schema_manager import get_s3_url_schemas
from src.utils.s3 import read_s3_csv, build_s3_path
from src.core.data_catalog_registry import DataCatalogRegistry
from src.decorators import monitor_job
from src.core.writer import merge_table_from_view
from pyspark.sql import SparkSession
from src.utils.errors import handle_job_exception

TEMP_DF_DEPARTMENTS = "temp_df_departments"
TEMP_DF_PROFESSIONS = "temp_df_professions"


@monitor_job
def run_etl_reference(spark: SparkSession, config: dict):
    registry = DataCatalogRegistry.from_s3_yaml_file(spark, get_s3_url_schemas(config))
    df_raw_departments = read_s3_csv(
        spark, build_s3_path(config["s3"]["departments_csv"]), registry.get_spark_schema(spark, "silver", "departments")
    )
    df_raw_professions = read_s3_csv(
        spark, build_s3_path(config["s3"]["professions_csv"]), registry.get_spark_schema(spark, "silver", "professions")
    )

    df_raw_departments.createOrReplaceTempView(TEMP_DF_DEPARTMENTS)
    df_raw_professions.createOrReplaceTempView(TEMP_DF_PROFESSIONS)

    merge_table_from_view(spark, registry, 'silver', 'departments', TEMP_DF_DEPARTMENTS)
    merge_table_from_view(spark, registry, 'silver', 'professions', TEMP_DF_PROFESSIONS)


if __name__ == "__main__":
    spark, config = get_spark_session(sys.argv)
    try:
        setup_logging()
        run_etl_reference(spark, config)
    except Exception as e:
        handle_job_exception(spark, e)