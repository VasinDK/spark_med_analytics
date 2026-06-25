import sys
import logging
from src.logging_config import setup_logging
from src.decorators import monitor_job
from src.core.session import get_spark_session
from pyspark.sql.functions import collect_list, date_sub, current_date, col, current_timestamp
from src.core.data_catalog_registry import DataCatalogRegistry
from src.core.schema_manager import get_s3_url_schemas

logger = logging.getLogger(__name__)

@monitor_job
def run_etl_gold():
    spark, config = get_spark_session(sys.argv)

    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        registry = DataCatalogRegistry.from_s3_yaml_file(spark, get_s3_url_schemas(config))

        df_visits = spark.read.table(registry.get_table_address("silver", "visits")) \
            .filter(col("created_at") >= date_sub(current_date(), config['buffer_days'])) \
            .drop("updated_at")

        if not df_visits.take(1):
            # exception
            logger.info("Нет новых или измененных данных для обновления слоя Gold.")
            return

        df_symptoms_grouped = spark.read.table(registry.get_table_address("silver","visits_symptoms")) \
            .filter(col("created_at") >= date_sub(current_date(), config['buffer_days'])) \
            .groupBy("visit_id").agg(collect_list("symptoms_code").alias("symptoms_list"))

        df_chronic_grouped = spark.read.table(registry.get_table_address("silver", "visits_chronic")) \
            .filter(col("created_at") >= date_sub(current_date(), config['buffer_days'])) \
            .groupBy("visit_id").agg(collect_list("chronic_diseases").alias("chronic_list"))
        
        df_departments_prepared = spark.read.table(registry.get_table_address("silver", "departments")) \
            .withColumnRenamed("name", "department_name")

        df_professions_prepared = spark.read.table(registry.get_table_address("silver", "professions")) \
            .withColumnRenamed("name", "profession_name")
        
        df_total = (df_visits
            .join(
                df_symptoms_grouped, 
                on=df_visits["id"] == df_symptoms_grouped["visit_id"], 
                how="left"
            )
            .drop(df_symptoms_grouped["visit_id"])
            .join(
                df_chronic_grouped, 
                on=df_visits["id"] == df_chronic_grouped["visit_id"],
                how="left"
            )
            .drop(df_chronic_grouped["visit_id"])
            .join(df_departments_prepared, on = df_visits["department_id"] == df_departments_prepared["id"], how="left")
            .drop(df_departments_prepared["id"])
            .join(df_professions_prepared, on = df_visits["profession_id"] == df_professions_prepared["id"], how="left")
            .drop(df_professions_prepared["id"])
            .withColumn("updated_at", current_timestamp())
        )

        gold_fields = [f["name"].lower() for f in registry.get_fields("gold", "visits")]
        df_total_aligned = df_total.select(*gold_fields)

        (df_total_aligned.write
         .format("iceberg")
         .mode("overwrite")
         .option("write.format.default", "parquet")
         .save(registry.get_table_address("gold","visits")))

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl_gold()