import logging
import src.config as configuration
from src.exceptions import ConfigurationNotFoundError, CriticalDataQualityError, QuarantineWriteError
from src.schemas import Metrics
from src import constants
from typing import Any, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, md5, concat_ws, size, current_timestamp, expr, array, when

def get_spark_session(args: list) -> Tuple[SparkSession, Any]:
    if len(args) < 2:
        raise ConfigurationNotFoundError()
    
    app_name = args[0]
    config_yaml = args[1]
    
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    
    config = configuration.load_s3_yaml(spark, config_yaml)
    logging.getLogger("py4j").setLevel(config["log_level"]["py4j"])
    
    return spark, config

def build_s3_path(s3_file_config: dict) -> str:
    bucket = s3_file_config["bucket"].strip("/")
    path = s3_file_config["path"].lstrip("/") 
    
    return f"s3a://{bucket}/{path}"

def read_s3_csv(spark, s3_path, schema, has_header=True, delimiter=";"):
    return (spark.read
            .schema(schema)
            .option("header", has_header)
            .option("delimiter", delimiter)
            .csv(s3_path))

def upsert_iceberg_table(spark: SparkSession, target_table: str, source_view: str, key_col: str = "id"):
    source_df = spark.table(source_view)
    
    fields_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in source_df.schema])
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            {fields_sql}
        )
        USING iceberg
    """)

    data_cols = [f.name for f in source_df.schema if f.name != key_col]
    
    set_expr = ", ".join([f"t.{col} = ts.{col}" for col in data_cols])
    all_cols = ", ".join([key_col] + data_cols)
    values_expr = ", ".join([f"ts.{col}" for col in [key_col] + data_cols])

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {source_view} ts
        ON t.{key_col} = ts.{key_col}
        WHEN MATCHED THEN UPDATE 
            SET {set_expr}
        WHEN NOT MATCHED THEN 
            INSERT ({all_cols}) VALUES ({values_expr})
    """)

def add_id(df: DataFrame) -> DataFrame: 
    return (df
            .withColumn("visit_date", to_date(col("visit_date")))
            .withColumn("id", md5(concat_ws("-", col("visit_date"), col("snils"), col("disease_code")))))

def add_quarantine(df: DataFrame, url: str): 
    try:
        (df.write
            .mode("append")
            .format("parquet")
            .option("mergeSchema", "true")
            .save(url))
    except Exception as e:
        raise QuarantineWriteError(constants.QUARANTINE_WRITE_ERROR.format(e)) from e

def validate(
    spark: SparkSession,
    df: DataFrame, 
    dq_config: dict, 
    max_error_percent: float = constants.MAX_ERROR_PERCENT
) -> Tuple[DataFrame, DataFrame, Metrics]:
    logger = logging.getLogger(__name__)
    app_id = spark.sparkContext.applicationId
    
    age_filter = "age >= {} AND age <= {}".format(dq_config['min_age'], dq_config['max_age'])
    temp_filter = "temperature >= {} AND temperature <= {}".format(dq_config['min_temp'], dq_config['max_temp'])
    
    age_condition_expr = "NOT ({}) OR age IS NULL".format(age_filter)
    temp_condition_expr = "NOT ({}) OR temperature IS NULL".format(temp_filter)
    
    df_marked = df.withColumn(
        "errors",
        array(
            when(expr(age_condition_expr), constants.ERR_INVALID_AGE),
            when(expr(temp_condition_expr), constants.ERR_INVALID_TEMP)
        )
    )
    
    df_marked = df_marked.withColumn("errors", expr("array_remove(errors, null)"))
    df_marked = df_marked.localCheckpoint(eager=True)

    df_clean = df_marked.filter(size("errors") == 0).drop("errors")
    df_quarantine = df_marked.filter(size("errors") > 0)
    
    total_count = df_marked.count()
    if total_count == 0:
        logger.warning(constants.EMPTY_INPUT_WARNING)
        return df_marked.drop("errors"), df_marked, Metrics(app_id = app_id)
        
    invalid_count = df_quarantine.count()
    valid_count = total_count - invalid_count
    error_percent = (invalid_count / total_count) * 100
    
    metrics = Metrics(
        app_id = app_id,
        total_rows = total_count,
        valid_rows = valid_count,
        invalid_rows = invalid_count,
        error_percent = round(error_percent, 2)
    )
    
    logger.info(constants.QUALITY_METRICS_LOG.format(metrics))
    
    if error_percent > max_error_percent:
        error_msg = constants.CRITICAL_ERROR_PERCENT_DETAILS.format(error_percent, max_error_percent)
        raise CriticalDataQualityError(error_msg)

    df_invalid = df_quarantine.withColumn("rejected_at", current_timestamp())
            
    return df_clean, df_invalid, metrics