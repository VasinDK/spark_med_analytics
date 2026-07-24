import logging
from src.core.data_catalog_registry import DataCatalogRegistry
from src import constants
from src.utils.stats_table_sync import StatsTableSync
from src.logging_config import setup_logging
from src.decorators import monitor_job
from src.core.session import get_spark_session
from src.core.schema_manager import create_database, sync_single_table
from pyspark.sql import SparkSession
from src.utils.errors import handle_job_exception
from src.config import get_config

logger = logging.getLogger(__name__)

@monitor_job
def run_ice_schema_migration(spark: SparkSession, layer: str):
    registry = DataCatalogRegistry.from_s3_yaml_file(get_config()['schemas'])
    stats = StatsTableSync()
    create_database(spark, registry.get_catalog_schema(layer))

    for table_key in registry.get_active_tables():
        sync_single_table(spark, registry, layer, table_key, stats)

    logger.info("=" * 30)
    logger.info(constants.REPORT_SCHEMA_SYNCHRONIZATION)
    for key, value in stats.to_dict().items():
        logger.info(f"  • {key}:  {value}")
    logger.info("=" * 30)

if __name__ == "__main__":
    config = get_config()['cfg']
    setup_logging()
    spark = get_spark_session(config)
    try:
        run_ice_schema_migration(spark, 'silver')
        run_ice_schema_migration(spark, 'gold')
    except Exception as e:
        handle_job_exception(spark, e)
