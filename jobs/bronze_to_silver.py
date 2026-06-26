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

        df_bronze = df_raw.transform(cast_bronze(registry))

        df_clean, df_quarantine, metrics = validate(df_bronze, config["dq_rule"])
        
        if metrics.invalid_rows > 0:    
            add_quarantine(df_quarantine, build_s3_path(config["s3"]["quarantine_path"]))

        if metrics.total_rows == 0:
            return

        df_silver = (df_clean
            .transform(cast_visit_date)
            .transform(add_id)
            .transform(add_bmi))
        
        df_silver_ready = df_silver.localCheckpoint(eager=True)
        df_silver_ready.createOrReplaceTempView(TEMP_SILVER_DATA)

        merge_table_from_view(spark, registry, 'silver', 'visits', TEMP_SILVER_DATA)

        symptoms_target = {
            "table_address": registry.get_table_address('silver', 'visits_symptoms'),
            "raw_col": "symptoms_code",
            "target_col": "symptoms_code",
            'all_columns': [f["name"].lower() for f in registry.get_fields('silver', 'visits_symptoms')],
        }
        upsert_array_relation(spark, symptoms_target, TEMP_SILVER_DATA)

        chronic_target = {
            "table_address": registry.get_table_address('silver', 'visits_chronic'),
            "raw_col": "chronic_diseases",
            "target_col": "chronic_diseases",
            'all_columns': [f["name"].lower() for f in registry.get_fields('silver', 'visits_chronic')],
        }
        upsert_array_relation(spark, chronic_target, TEMP_SILVER_DATA)

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl_silver()
