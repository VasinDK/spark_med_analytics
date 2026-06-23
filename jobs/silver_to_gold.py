import sys
from src.logging_config import setup_logging
from src.decorators import monitor_job
from src.core.session import get_spark_session
from pyspark.sql.functions import collect_list
from src.core.data_catalog_registry import DataCatalogRegistry
from src.core.schema_manager import get_s3_url_schemas


@monitor_job
def run_etl_gold():
    spark, config = get_spark_session(sys.argv)

    try:
        registry = DataCatalogRegistry.from_s3_yaml_file(spark, get_s3_url_schemas(config))

        df_visits = spark.read.table(registry.get_table_address("silver","visits"))
        df_symptoms = spark.read.table(registry.get_table_address("silver","visits_symptoms"))
        df_symptoms_grouped = df_symptoms.groupBy("visit_id", "visit_date").agg(
            collect_list("symptoms_code").alias("symptoms_list")
        )
        df_chronic = spark.read.table(registry.get_table_address("silver", "visits_chronic"))
        df_chronic_grouped = df_chronic.groupBy("visit_id", "visit_date").agg(
            collect_list("chronic_diseases").alias("chronic_list")
        )
        df_departments = spark.read.table(registry.get_table_address("silver", "departments"))
        df_departments_prepared = df_departments.withColumnRenamed("name", "department_name")
        df_professions = spark.read.table(registry.get_table_address("silver", "professions"))
        df_professions_prepared = df_professions.withColumnRenamed("name", "profession_name")

        df_total = (df_visits
            .join(
                df_symptoms_grouped, 
                on=(df_visits["id"] == df_symptoms_grouped["visit_id"]) &
                   (df_visits["visit_date"] == df_symptoms_grouped["visit_date"]), 
                how="left"
            )
            .drop(df_symptoms_grouped["visit_id"])
            .drop(df_symptoms_grouped["visit_date"])
            .join(
                df_chronic_grouped, 
                on=(df_visits["id"] == df_chronic_grouped["visit_id"]) & 
                   (df_visits["visit_date"] == df_chronic_grouped["visit_date"]),
                how="left"
            )
            .drop(df_chronic_grouped["visit_id"])
            .drop(df_chronic_grouped["visit_date"])
            .join(df_departments_prepared, on = df_visits["department_id"] == df_departments_prepared["id"], how="left")
            .drop(df_departments_prepared["id"])
            .join(df_professions_prepared, on = df_visits["profession_id"] == df_professions_prepared["id"], how="left")
            .drop(df_professions_prepared["id"])
        )
        # обновлять раз в сутки последние 3 дня
        (df_total.write
         .format("iceberg")
         .mode("overwrite")
         .option("write.format.default", "parquet")
         .partitionBy("visit_date")
         .save(registry.get_table_address("gold","visits")))

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl_gold()