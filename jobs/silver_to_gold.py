import sys
import logging
from src.logging_config import setup_logging
from src.decorators import monitor_job
from src.core.session import get_spark_session
from pyspark.sql.functions import collect_list, col, current_timestamp, broadcast
from src.core.data_catalog_registry import DataCatalogRegistry
from src.core.schema_manager import get_s3_url_schemas
from src.utils.db import get_last_date
from src.core.writer import merge_table_from_view
from src import constants
from src.exceptions import NoDataGoldError

TEMP_GOLD_DATA = "temp_gold_data"

logger = logging.getLogger(__name__)

@monitor_job
def run_etl_gold():
    spark, config = get_spark_session(sys.argv)

    try:
        registry = DataCatalogRegistry.from_s3_yaml_file(spark, get_s3_url_schemas(config))
        gold_table_address = registry.get_table_address("gold", "visits")
        last_visit = get_last_date(spark.read.table(gold_table_address))
        watermark_date = last_visit if last_visit else "1970-01-01"
        logger.info(constants.INCREMENT_POINT.format(watermark_date))

        df_visits = spark.read.table(registry.get_table_address("silver", "visits")) \
            .filter(col("created_at") >= watermark_date) \
            .drop("updated_at")

        if df_visits.isEmpty():
            raise NoDataGoldError()

        df_active_visits = df_visits.select(col("id").alias("active_id"), col("visit_date").alias("active_date")).distinct()

        df_symptoms_grouped = (spark.read.table(registry.get_table_address("silver", "visits_symptoms"))
            .join(df_active_visits, 
                  on=((col("visit_id") == col("active_id")) & (col("visit_date") == col("active_date"))), 
                  how="inner")
            .groupBy("visit_id", "visit_date")
            .agg(collect_list("symptoms_code").alias("symptoms_list"))
        )

        df_chronic_grouped = (spark.read.table(registry.get_table_address("silver", "visits_chronic"))
            .join(df_active_visits, 
                  on=((col("visit_id") == col("active_id")) & (col("visit_date") == col("active_date"))), 
                  how="inner")
            .groupBy("visit_id", "visit_date")
            .agg(collect_list("chronic_diseases").alias("chronic_list"))
        )
        
        df_departments_prepared = spark.read.table(registry.get_table_address("silver", "departments")) \
            .withColumnRenamed("name", "department_name")

        df_professions_prepared = spark.read.table(registry.get_table_address("silver", "professions")) \
            .withColumnRenamed("name", "profession_name")
        
        df_total = (df_visits
            .join(
                df_symptoms_grouped, 
                on=((df_visits["visit_date"] == df_symptoms_grouped["visit_date"]) & 
                    (df_visits["id"] == df_symptoms_grouped["visit_id"])), 
                how="left"
            )
            .drop(df_symptoms_grouped["visit_id"])
            .drop(df_symptoms_grouped["visit_date"])
            .join(
                df_chronic_grouped, 
                on=((df_visits["visit_date"] == df_chronic_grouped["visit_date"]) & 
                    (df_visits["id"] == df_chronic_grouped["visit_id"])),
                how="left"
            )
            .drop(df_chronic_grouped["visit_id"])
            .drop(df_chronic_grouped["visit_date"])
            .join(broadcast(df_departments_prepared), on=df_visits["department_id"] == df_departments_prepared["id"], how="left")
            .drop(df_departments_prepared["id"])
            .join(broadcast(df_professions_prepared), on=df_visits["profession_id"] == df_professions_prepared["id"], how="left")
            .drop(df_professions_prepared["id"])
            .withColumn("updated_at", current_timestamp())
        )

        gold_fields = [f["name"].lower() for f in registry.get_fields("gold", "visits")]
        df_total_aligned = df_total.select(*gold_fields)
        df_total_aligned.createOrReplaceTempView(TEMP_GOLD_DATA)

        merge_table_from_view(spark, registry, 'gold', 'visits', TEMP_GOLD_DATA)

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl_gold()