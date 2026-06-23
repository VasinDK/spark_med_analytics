from src.exceptions import QuarantineWriteError
from src import constants
from pyspark.sql import SparkSession, DataFrame

# TYPE_MAPPING_JSON_SPARK = {
#     "integer": "int",
#     "string": "string",
#     "timestamp": "timestamp",
#     "float": "float",
#     "double": "double",
#     "boolean": "boolean",
#     "long": "bigint",
# }

# def upsert_iceberg_table(spark: SparkSession, target_table: str, source_view: str, key_col: str = "id"):
#     source_df = spark.table(source_view)
    
#     fields_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in source_df.schema])
    
#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS {target_table} (
#             {fields_sql}
#         )
#         USING iceberg
#     """)

#     data_cols = [f.name for f in source_df.schema if f.name != key_col]
    
#     set_expr = ", ".join([f"t.{col} = ts.{col}" for col in data_cols])
#     all_cols = ", ".join([key_col] + data_cols)
#     values_expr = ", ".join([f"ts.{col}" for col in [key_col] + data_cols])

#     spark.sql(f"""
#         MERGE INTO {target_table} t
#         USING {source_view} ts
#         ON t.{key_col} = ts.{key_col}
#         WHEN MATCHED THEN UPDATE 
#             SET {set_expr}
#         WHEN NOT MATCHED THEN 
#             INSERT ({all_cols}) VALUES ({values_expr})
#     """)

# def get_tables_address(config: dict, layer: str) -> dict:
#     s3_yaml_path = f"s3a://{config['infrastructure']['code_bucket']}/{config['databases'][layer]['schemas_path']}"
#     db_config = config['databases'][layer]
#     db_schema = f"{db_config['catalog']}.{db_config['schema']}"
#     return {
#         key: f"{db_schema}.{value}" for key, value in db_config['tables'].items()
#     }

def add_quarantine(df: DataFrame, url: str): 
    try:
        (df.write
            .mode("append")
            .format("parquet")
            .option("mergeSchema", "true")
            .save(url))
    except Exception as e:
        raise QuarantineWriteError(constants.QUARANTINE_WRITE_ERROR.format(e)) from e

# def normalize_type(data_type: str) -> str:
#     clean_type = data_type.lower().strip()
#     if clean_type == "array_string":
#         return "ARRAY<STRING>"
        
#     if clean_type.startswith("array_") and len(clean_type) > 6:
#         inner_type = clean_type.split("_")[1]
#         spark_inner_type = TYPE_MAPPING_JSON_SPARK.get(inner_type, inner_type)
#         return f"ARRAY<{spark_inner_type.upper()}>"
        
#     return TYPE_MAPPING_JSON_SPARK.get(clean_type, clean_type)