import logging
from pyspark.sql import SparkSession, DataFrame
from src import constants
from src.core.data_catalog_registry import DataCatalogRegistry
from src.exceptions import QuarantineWriteError, MergeTableError

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
        raise MergeTableError(constants.MERGE_KEYS_ERROR.format(layer, table_key))

    all_columns = [f["name"].lower() for f in fields]
    key_columns = [k.lower() for k in merge_keys]
    tech_columns = key_columns + ["created_at", "updated_at", "id"]
    on_clause = " AND ".join([f"t.{col} = td.{col}" for col in key_columns])

    update_columns = [col for col in all_columns if col not in tech_columns]
    update_pairs = [f"t.{col} = td.{col}" for col in update_columns]
    if "updated_at" in all_columns: 
        update_pairs.append("t.updated_at = current_timestamp()")  

    insert_cols = ", ".join(all_columns)
    insert_values = ", ".join(["current_timestamp()" if col in ("created_at", "updated_at") else f"td.{col}" for col in all_columns])
    update_clause = f"WHEN MATCHED THEN UPDATE SET {', '.join(update_pairs)}" if update_pairs else ""

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {source_view} td
        ON {on_clause}
        {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values})
    """)

def upsert_array_relation(spark: SparkSession, target: dict, source_view: str):
    bounds = spark.sql(f"SELECT min(visit_date), max(visit_date) FROM {source_view}").collect()[0]
    min_date = bounds[0]
    max_date = bounds[1]

    if min_date and max_date:
        spark.sql(f"""
            MERGE INTO {target['table_address']} t
            USING {source_view} src
            ON t.visit_id = src.id 
                AND t.visit_date = src.visit_date
                AND t.visit_date BETWEEN '{min_date}' AND '{max_date}' 
            WHEN MATCHED THEN DELETE
        """)

    select_exprs = []
    for col in target['all_columns']:
        if col == "visit_id":
            select_exprs.append(f"id AS visit_id")
        elif col == target['target_col'].lower():
            select_exprs.append(f"explode({target['raw_col']}) AS {target['target_col']}")
        else:
            select_exprs.append(col)

    select_clause = ", ".join(select_exprs)
    columns_clause = ", ".join(target['all_columns'])
    spark.sql(f"""
        INSERT INTO {target['table_address']} ({columns_clause})
        SELECT {select_clause} FROM {source_view}
        WHERE {target['raw_col']} IS NOT NULL AND size({target['raw_col']}) > 0
    """)

def add_quarantine(df: DataFrame, url: str): 
    try:
        (df.write
            .mode("append")
            .option("mergeSchema", "true")
            .option("write.format.default", "parquet")
            .save(url))
    except Exception as e:
        raise QuarantineWriteError(constants.QUARANTINE_WRITE_ERROR.format(e)) from e