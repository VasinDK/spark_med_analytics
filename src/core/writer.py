import logging
from pyspark.sql import SparkSession, DataFrame
from src import constants
from src.core.data_catalog_registry import DataCatalogRegistry
from src.exceptions import QuarantineWriteError

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
    tech_columns = key_columns + ["created_at", "updated_at", "id"]
    on_clause = " AND ".join([f"t.{col} = td.{col}" for col in key_columns])

    update_columns = [col for col in all_columns if col not in tech_columns]
    update_pairs = [f"t.{col} = td.{col}" for col in update_columns]
    if "updated_at" in all_columns: 
        update_pairs.append("t.updated_at = current_timestamp()")  

    update_clause = ", ".join(update_pairs)

    insert_cols = ", ".join(all_columns)
    insert_values_list = []
    for col in all_columns:
        if col == "created_at" or col == "updated_at":
            insert_values_list.append("current_timestamp()") 
        else:
            insert_values_list.append(f"td.{col}")
            
    insert_values = ", ".join(insert_values_list)

    matched_clause = ""
    if update_clause:
        matched_clause = f"""
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
        """

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {source_view} td
        ON {on_clause}
        {matched_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values})
    """)

def upsert_array_relation(spark: SparkSession, target: dict, source_view: str):
    distinct_days = [
        str(row["visit_day"]) 
        for row in spark.sql(f"""
                                SELECT DISTINCT date(visit_date) AS visit_day 
                                FROM {source_view} WHERE visit_date IS NOT NULL
                            """).collect()
    ]
    
    if distinct_days:
        dates_str = ", ".join([f"'{d}'" for d in distinct_days])
        spark.sql(f"""
            DELETE FROM {target['table_address']} 
            WHERE date(visit_date) IN ({dates_str}) 
            AND visit_id IN (SELECT id FROM {source_view})
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