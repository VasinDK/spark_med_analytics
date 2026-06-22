import sys
from src.logging_config import setup_logging
from src.decorators import monitor_job
from src.core.session import get_spark_session
from src.utils.s3 import build_s3_path
from src.utils.db import add_quarantine
from src.transforms import cast_visit_date, add_id, add_bmi, cast_bronze
from src.utils import validate
from src.core.schema_manager import get_s3_url_schemas
from src.core.data_catalog_registry import DataCatalogRegistry
from src.core.writer import merge_table_from_view, upsert_array_relation
from functools import partial

TEMP_SILVER_DATA = "temp_silver_data"

@monitor_job
def run_etl_silver():
    spark, config = get_spark_session(sys.argv)

    try:
        registry = DataCatalogRegistry.from_s3_yaml_file(spark, get_s3_url_schemas(config))
        df_raw = (
            spark.read
            .option("multiline", "true")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(build_s3_path(config["s3"]["visits_json"]))
        )

        corrupt_count = df_raw.filter(df_raw["_corrupt_record"].isNotNull()).count()
        if corrupt_count > 0:
            print(f"Внимание! Найдено {corrupt_count} сломанных JSON-строк.")

        df_clean, df_quarantine, metrics = validate(spark, cast_bronze(df_raw, registry), config["dq_rule"])
        add_quarantine(df_quarantine, build_s3_path(config["s3"]["quarantine_path"]))

        df_silver = (
            df_clean.transform(cast_visit_date).transform(add_id).transform(add_bmi)
        )
        df_silver.localCheckpoint(eager=True)
        df_silver.createOrReplaceTempView(TEMP_SILVER_DATA)

        merge_table_from_view(spark, registry, 'silver', 'visits', TEMP_SILVER_DATA)

        symptoms_target = {
            "table_address": registry.get_table_address('silver', 'visits_symptoms'),
            "raw_col": "symptoms_code",
            "target_col": "symptoms_code"
        }
        upsert_array_relation(spark, symptoms_target, TEMP_SILVER_DATA)

        chronic_target = {
            "table_address": registry.get_table_address('silver', 'visits_chronic'),
            "raw_col": "chronic_diseases",
            "target_col": "chronic_diseases"
        }
        upsert_array_relation(spark, chronic_target, TEMP_SILVER_DATA)

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl_silver()
