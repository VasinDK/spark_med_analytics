import sys
from src.logging_config import setup_logging
from src.utils import get_spark_session, read_s3_csv, upsert_iceberg_table_reference, build_s3_path
from src import schemas
from src.decorators import monitor_job


@monitor_job
def run_etl():
    spark, config = get_spark_session(sys.argv[0])
    try:
        df_raw_departments = read_s3_csv(spark, f"s3a://{build_s3_path(config['s3']['departments_csv'])}", schemas.departments)
        df_raw_professions = read_s3_csv(spark, f"s3a://{build_s3_path(config['s3']['professions_csv'])}", schemas.professions)

        df_raw_departments.createOrReplaceTempView("temp_df_departments")
        df_raw_professions.createOrReplaceTempView("temp_df_professions")

        departments_table = f"{config['db']['catalog']}.{config['db']['schema']}.{config['db']['table']['departments']}"
        professions_table = f"{config['db']['catalog']}.{config['db']['schema']}.{config['db']['table']['professions']}"

        upsert_iceberg_table_reference(spark, departments_table, "temp_df_departments")
        upsert_iceberg_table_reference(spark, professions_table, "temp_df_professions")

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl()
