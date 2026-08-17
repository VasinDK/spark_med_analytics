from src.logging_config import setup_logging
from src.decorators import monitor_job
from src.core.session import get_spark_session
from src.transforms import cast_visit_date, add_id, add_bmi, cast_bronze
from src.utils import s3, validate, finalize_validation, errors
from src.core.data_catalog_registry import DataCatalogRegistry
from src.core.writer import merge_table_from_view, upsert_array_relation, add_quarantine
from src.utils.metrics_validate import MetricsValidate
from pyspark.sql import DataFrame, SparkSession
from src.utils.action_context import ActionContext
from pyspark import StorageLevel
from src.config import get_config
from src.exceptions import CriticalDataQualityError
from src.constants import CRITICAL_ERROR_PERCENT_DETAILS

TEMP_SILVER_DATA = "temp_silver_data"


@monitor_job
def run_etl_silver(spark: SparkSession):
    config = get_config()

    registry = DataCatalogRegistry.from_dict(config["schema"])
    metrics = MetricsValidate()

    with ActionContext(spark, config, metrics):
        base_path = s3.build_s3_path(config["cfg"]["s3"]["visits_raw_json"])
        input_path = f"{base_path}{config['ds']}/*"

        df_raw = (
            spark.read.option("multiline", "true")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(input_path)
        )

        df_bronze = df_raw.transform(cast_bronze(registry)).transform(
            validate(config["cfg"]["dq_rule"])
        )

        df_bronze.persist(StorageLevel.MEMORY_AND_DISK)

        df_clean: DataFrame
        df_quarantine: DataFrame
        df_clean, df_quarantine = finalize_validation(df_bronze, metrics)

        if metrics.invalid_rows > 0:
            add_quarantine(
                df_quarantine, s3.build_s3_path(config["cfg"]["s3"]["quarantine_path"])
            )

        if metrics.total_rows == 0:
            df_bronze.unpersist()
            return

        if metrics.error_percent > config["cfg"]["dq_rule"]["percent_marriage"]:
            error_msg = CRITICAL_ERROR_PERCENT_DETAILS.format(
                metrics.error_percent, config["cfg"]["dq_rule"]["percent_marriage"]
            )
            raise CriticalDataQualityError(error_msg)

        df_silver = (
            df_clean.transform(cast_visit_date).transform(add_id).transform(add_bmi)
        )

        df_silver.persist(StorageLevel.MEMORY_AND_DISK)
        df_bronze.unpersist()
        df_silver.createOrReplaceTempView(TEMP_SILVER_DATA)

        merge_table_from_view(spark, registry, "silver", "visits", TEMP_SILVER_DATA)

        symptoms_target = {
            "table_address": registry.get_table_address("silver", "visits_symptoms"),
            "raw_col": "symptoms_code",
            "target_col": "symptoms_code",
            "all_columns": [
                f["name"].lower()
                for f in registry.get_fields("silver", "visits_symptoms")
            ],
        }
        upsert_array_relation(spark, symptoms_target, TEMP_SILVER_DATA)

        chronic_target = {
            "table_address": registry.get_table_address("silver", "visits_chronic"),
            "raw_col": "chronic_diseases",
            "target_col": "chronic_diseases",
            "all_columns": [
                f["name"].lower()
                for f in registry.get_fields("silver", "visits_chronic")
            ],
        }
        upsert_array_relation(spark, chronic_target, TEMP_SILVER_DATA)

        df_silver.unpersist()


if __name__ == "__main__":
    setup_logging()
    spark = get_spark_session(get_config()["cfg"])
    try:
        run_etl_silver(spark)
    except Exception as e:
        errors.handle_job_exception(spark, e)
