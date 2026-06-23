import logging
from typing import List, Dict, Any
from pyspark.sql import SparkSession
from src.utils.metrics import StatsTableSync
from src.exceptions import ColumnNotNullError
from src.constants import constants
from src.core.data_catalog_registry import DataCatalogRegistry

logger = logging.getLogger(__name__)

def create_database(spark: SparkSession, catalog: str, schema: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{schema}")

def create_table_ice(spark: SparkSession, table_address: str, table_meta: Dict[str, Any]):
    fields: List[Dict[str, Any]] = table_meta["fields"]
    partition_by: List[str] = table_meta.get("partition_by", [])
    tbl_properties: Dict[str, str] = table_meta.get("tbl_properties", {})

    columns_spec = []
    for field in fields:
        col_name = field['name'].lower()
        col_type = field['type']
        null_str = "NOT NULL" if not field.get('nullable', True) else ""
        
        columns_spec.append(f"{col_name} {col_type} {null_str}".strip())

    partition_spec = ""
    if partition_by:
        partition_spec = f"PARTITIONED BY ({', '.join(partition_by)})"

    props_spec = ""
    if tbl_properties:
        props_list = [f"'{k}'='{v}'" for k, v in tbl_properties.items()]
        props_spec = f"TBLPROPERTIES ({', '.join(props_list)})"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_address} (
            {', '.join(columns_spec)}
        )
        USING iceberg
        {partition_spec}
        {props_spec}
    """)

def sync_table_columns(
    spark: SparkSession, 
    target_address: str, 
    yaml_fields: List[Dict[str, Any]], 
    stats: StatsTableSync
) -> None:
    current_list_col = spark.catalog.listColumns(target_address)
    current_columns = {col.name.lower(): col.dataType for col in current_list_col}
    yaml_columns = {f['name'].lower(): f for f in yaml_fields}

    for col_name, _ in list(current_columns.items()):
        if col_name not in yaml_columns:
            logger.info(constants.DELETING_OUTDATED_COLUMN.format(col_name, target_address))
            spark.sql(f"ALTER TABLE {target_address} DROP COLUMN {col_name}")
            del current_columns[col_name]
            stats.columns_deleted += 1

    for field in yaml_fields:
        f_name = field['name']
        f_name_lower = f_name.lower()
        f_type_raw = field['type']
        f_type_normalized = f_type_raw

        if f_name_lower not in current_columns:
            if not field.get('nullable', True):
                raise ColumnNotNullError()
            logger.info(constants.ADDING_NEW_COLUMN.format(f_name, f_type_raw, target_address))
            spark.sql(f"ALTER TABLE {target_address} ADD COLUMN {f_name} {f_type_raw}")
            stats.columns_added += 1
        else:
            current_type = current_columns[f_name_lower]
            if current_type != f_type_normalized:
                logger.info(constants.CHANGING_COLUMN_TYPE.format(f_name, target_address, current_type, f_type_raw))
                spark.sql(f"ALTER TABLE {target_address} ALTER COLUMN {f_name} SET DATA TYPE {f_type_raw}")
                stats.types_changed += 1
    
def sync_single_table(
    spark: SparkSession, 
    registry: DataCatalogRegistry, 
    layer: str, 
    table_key: str, 
    stats: StatsTableSync
) -> None:
    target_address = registry.get_table_address(layer, table_key)
    
    if not spark.catalog.tableExists(target_address):
        table_meta = registry.get_table_metadata(layer, table_key)
        create_table_ice(spark, target_address, table_meta)

        logger.info(constants.CREATING_NEW_TABLE.format(target_address))
        stats.tables_created += 1
        stats.columns_added += len(table_meta["fields"])
        return

    yaml_fields = registry.get_fields(layer, table_key)
    sync_table_columns(spark, target_address, yaml_fields, stats)
    stats.tables_checked += 1

def get_s3_url_schemas(config: dict) -> str:
    code_bucket = f"{config['infrastructure']['code_bucket']}".strip("/")
    schemas = f"{config['infrastructure']['schemas']}".strip("/")
    
    return f"s3a://{code_bucket}/{schemas}"