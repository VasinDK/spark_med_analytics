def optimize_iceberg_tables(spark: SparkSession, tables: dict, catalog: str):
    """Проводит регламентное обслуживание таблиц для предотвращения деградации скорости."""
    logger = logging.getLogger(__name__)
    for table_key, table_path in tables.items():
        try:
            logger.info(f"Starting Iceberg optimize actions for: {table_path}")
            # Сжатие мелких файлов данных в крупные блоки
            spark.sql(f"CALL {catalog}.system.rewrite_data_files(table => '{table_path}')")
            # Очистка снапшотов старше 7 дней
            spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{table_path}', older_than => NOW() - INTERVAL 7 DAYS)")
        except Exception as e:
            logger.error(f"Failed to optimize table {table_path}: {e}")
