import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from src.core.data_catalog_registry import DataCatalogRegistry

logger = logging.getLogger(__name__)

def merge_table_from_view(
    spark: SparkSession, 
    registry: DataCatalogRegistry, 
    layer: str, 
    table_key: str, 
    source_view: str
):
    target_table = registry.get_table_address(layer, table_key)
    fields = registry.get_fields(layer, table_key)
    merge_keys = registry.get_merge_keys(layer, table_key)

    if not merge_keys:
        raise ValueError(f"В конфигурации schemas.yaml не заданы 'merge_keys' для {layer}.{table_key}")

    all_columns = [f["name"].lower() for f in fields]
    key_columns = [k.lower() for k in merge_keys]
    
    update_columns = [col for col in all_columns if col not in key_columns and col != "id"]

    on_clause = " AND ".join([f"t.{col} = td.{col}" for col in key_columns])
    update_clause = ", ".join([f"t.{col} = td.{col}" for col in update_columns])
    insert_cols = ", ".join(all_columns)
    insert_values = ", ".join([f"td.{col}" for col in all_columns])

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {source_view} td
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values})
    """)

# There are no custom fields. All fields are internal. SQL injection is not applicable. 
def upsert_array_relation(spark: SparkSession, target: dict, source_view: str):
    spark.sql(f"DELETE FROM {target['table_address']} WHERE visit_id IN (SELECT id FROM {source_view})")

    spark.sql(f"""
        INSERT INTO {target['table_address']}
        SELECT id AS visit_id, visit_date, explode({target['raw_col']}) AS {target['target_col']}
        FROM {source_view}
        WHERE {target['raw_col']} IS NOT NULL
    """)
    