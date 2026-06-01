import time, os
import src.config as config
from src.utils import get_spark_session
from src.schemas import ref_book_schema
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, DateType, ArrayType
from pyspark.sql.functions import col, to_date


def run_etl():
    app_name = config.ENV
    print(f"=== app_name: {app_name}")
    # spark = get_spark_session(app_name = app_name)

    # df_raw_departments = spark.read.schema(ref_book_schema) \
    #     .option("header", True) \
    #     .option("delimiter", ";") \
    #     .csv(f"s3a://{config.BRONZE_DEPARTMENTS_CSV}")
    # df_raw_professions = spark.read.schema(ref_book_schema) \
    #     .option("header", True) \
    #     .option("delimiter", ";") \
    #     .csv(f"s3a://{config.BRONZE_PROFESSIONS_CSV}")

    # df_raw_departments.createOrReplaceTempView("temp_df_departments")
    # df_raw_professions.createOrReplaceTempView("temp_df_professions")

    # departments_table = "yandex.silver.departments"
    # professions_table = "yandex.silver.professions"

    # spark.sql(f"""
    #     CREATE TABLE IF NOT EXISTS {departments_table} (
    #         id INT,
    #         name STRING
    #     )
    #     USING iceberg
    # """)

    # spark.sql(f"""
    #     MERGE INTO {departments_table} t
    #     USING temp_df_departments td
    #     ON t.id = td.id
    #     WHEN MATCHED THEN UPDATE 
    #         SET t.name = td.name
    #     WHEN NOT MATCHED THEN 
    #         INSERT (id, name) VALUES (td.id, td.name)
    # """)

    # spark.sql(f"""
    #     CREATE TABLE IF NOT EXISTS {professions_table} (
    #         id INT,
    #         name STRING
    #     )
    #     USING iceberg
    # """)

    # spark.sql(f"""
    #     MERGE INTO {professions_table} t
    #     USING temp_df_professions tp
    #     ON t.id = tp.id
    #     WHEN MATCHED THEN UPDATE SET t.name = tp.name
    #     WHEN NOT MATCHED THEN INSERT (id, name) VALUES (tp.id, tp.name)
    # """)

    # spark.stop()

if __name__ == "__main__":
    start_time = time.perf_counter()
    run_etl()
    end_time = time.perf_counter()
    
    minutes, seconds = divmod(int(end_time - start_time), 60)
    print(f"=== ВРЕМЯ ВЫПОЛНЕНИЯ: {minutes} мин {seconds} сек ===")