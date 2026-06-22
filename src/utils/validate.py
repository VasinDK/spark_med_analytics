import logging
from src.exceptions import CriticalDataQualityError
from src.utils.metrics import MetricsValidate
from src import constants
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import round, size, current_timestamp, expr, array, when

def validate(
    spark: SparkSession,
    df: DataFrame, 
    dq_config: dict, 
    max_error_percent: float = constants.MAX_ERROR_PERCENT
) -> Tuple[DataFrame, DataFrame, MetricsValidate]:
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

    total_count = df_marked.count()
    if total_count == 0:
        logger.warning(constants.EMPTY_INPUT_WARNING)
        return df_marked.drop("errors"), df_marked.drop("errors"), MetricsValidate(spark_app_id = app_id)

    df_clean = df_marked.filter(size("errors") == 0).drop("errors")
    df_quarantine = df_marked.filter(size("errors") > 0)
    
    invalid_count = df_quarantine.count()
    valid_count = total_count - invalid_count
    error_percent = (invalid_count / total_count) * 100
    
    metrics = MetricsValidate(
        spark_app_id = app_id,
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