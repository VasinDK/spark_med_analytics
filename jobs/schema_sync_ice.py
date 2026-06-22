import sys
import logging
from src.core.data_catalog_registry import DataCatalogRegistry
from src import constants
from src.utils.metrics import StatsTableSync
from src.logging_config import setup_logging
from src.decorators import monitor_job
from src.core.session import get_spark_session
from src.core.schema_manager import create_database, sync_single_table, get_s3_url_schemas

logger = logging.getLogger(__name__)

@monitor_job
def run_schema_sync_ice(layer: str):
    spark, config = get_spark_session(sys.argv)

    try:
        registry = DataCatalogRegistry.from_s3_yaml_file(spark, get_s3_url_schemas(config))
        stats = StatsTableSync()
        create_database(spark, registry.get_catalog_schema(layer))

        for table_key in registry.get_active_tables():
            sync_single_table(spark, registry, layer, table_key, stats)

        logger.info("=" * 30)
        logger.info(constants.REPORT_SCHEMA_SYNCHRONIZATION)
        for key, value in stats.to_dict().items():
            logger.info(f"  • {key}:  {value}")
        logger.info("=" * 30)

    finally:
        spark.stop()

if __name__ == "__main__":
    setup_logging()
    run_schema_sync_ice('silver')
    run_schema_sync_ice('gold')
