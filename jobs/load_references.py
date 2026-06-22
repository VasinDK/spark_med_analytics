import sys
from src.logging_config import setup_logging
from src.core.session import get_spark_session
from src.core.schema_manager import get_s3_url_schemas
from src.utils.s3 import read_s3_csv, build_s3_path
from src.utils.db import upsert_iceberg_table
from src.utils import get_tables_address
from src.core.data_catalog_registry import DataCatalogRegistry
from src.decorators import monitor_job

TEMP_DF_DEPARTMENTS = "temp_df_departments"
TEMP_DF_PROFESSIONS = "temp_df_professions"


@monitor_job
def run_etl_reference():
    spark, config = get_spark_session(sys.argv)

    try:
        registry = DataCatalogRegistry.from_s3_yaml_file(spark, get_s3_url_schemas(config))
        df_raw_departments = read_s3_csv(
            spark, build_s3_path(config["s3"]["departments_csv"]), registry.get_spark_schema("silver", "departments")
        )
        df_raw_professions = read_s3_csv(
            spark, build_s3_path(config["s3"]["professions_csv"]), registry.get_spark_schema("silver", "professions")
        )

        df_raw_departments.createOrReplaceTempView(TEMP_DF_DEPARTMENTS)
        df_raw_professions.createOrReplaceTempView(TEMP_DF_PROFESSIONS)

        upsert_iceberg_table(spark, registry.get_table_address("silver", "departments"), TEMP_DF_DEPARTMENTS)
        upsert_iceberg_table(spark, registry.get_table_address("silver", "professions"), TEMP_DF_PROFESSIONS)

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl_reference()
