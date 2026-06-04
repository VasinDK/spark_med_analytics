import sys
from src.logging_config import setup_logging
from src.utils import get_spark_session, read_s3_csv, upsert_iceberg_table, build_s3_path
from src import schemas
from src.decorators import monitor_job

TEMP_DF_DEPARTMENTS = "temp_df_departments"
TEMP_DF_PROFESSIONS = "temp_df_professions"

@monitor_job
def run_etl():
    spark, config = get_spark_session(sys.argv)
    try:
        df_raw_departments = read_s3_csv(spark, build_s3_path(config['s3']['departments_csv']), schemas.departments)
        df_raw_professions = read_s3_csv(spark, build_s3_path(config['s3']['professions_csv']), schemas.professions)

        df_raw_departments.createOrReplaceTempView(TEMP_DF_DEPARTMENTS)
        df_raw_professions.createOrReplaceTempView(TEMP_DF_PROFESSIONS)

        departments_table = f"{config['db']['catalog']}.{config['db']['schema']}.{config['db']['table']['departments']}"
        professions_table = f"{config['db']['catalog']}.{config['db']['schema']}.{config['db']['table']['professions']}"

        upsert_iceberg_table(spark, departments_table, TEMP_DF_DEPARTMENTS)
        upsert_iceberg_table(spark, professions_table, TEMP_DF_PROFESSIONS)

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl()
