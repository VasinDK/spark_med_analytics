import logging
from src.exceptions import CriticalDataQualityError
from src.utils.metrics import MetricsValidate
from src import constants
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, size, expr, array, when, sum as _sum, round as _round

def validate(
    df: DataFrame, 
    dq_config: dict, 
) -> Tuple[DataFrame, DataFrame, MetricsValidate]:
    logger = logging.getLogger(__name__)
    
    age_filter = "age >= {} AND age <= {}".format(dq_config['min_age'], dq_config['max_age'])
    temp_filter = "temperature >= {} AND temperature <= {}".format(dq_config['min_temp'], dq_config['max_temp'])
    
    age_condition_expr = "NOT ({}) OR age IS NULL".format(age_filter)
    temp_condition_expr = "NOT ({}) OR temperature IS NULL".format(temp_filter)
        
    has_corrupt_col = "_corrupt_record" in df.columns
    corrupt_condition = "NOT (_corrupt_record IS NULL)" if has_corrupt_col else "false"
    

    df_marked = df.withColumn(
        "errors",
        array(
            when(expr(corrupt_condition), constants.ERR_CORRUPT_JSON), 
            when(expr(age_condition_expr), constants.ERR_INVALID_AGE),
            when(expr(temp_condition_expr), constants.ERR_INVALID_TEMP)
        )
    )

    df_marked = df_marked.withColumn("errors", expr("array_remove(errors, null)"))
    df_marked = df_marked.localCheckpoint(eager=True)

    metrics_row = df_marked.agg(
        _sum(when(size("errors") == 0, 1).otherwise(0)).alias("valid_rows"),
        _sum(when(size("errors") > 0, 1).otherwise(0)).alias("invalid_rows"),
        _sum(when(expr(corrupt_condition), 1).otherwise(0)).alias("corrupt_rows")
    ).collect()[0]

    valid_count = metrics_row.valid_rows
    invalid_count = metrics_row.invalid_rows
    corrupt_json_count = metrics_row.corrupt_rows
    total_count = valid_count + invalid_count
    
    if total_count == 0:
        logger.warning(constants.EMPTY_INPUT_WARNING)
        return df_marked.drop("errors"), df_marked.drop("errors"), MetricsValidate()

    df_clean = df_marked.filter(size("errors") == 0).drop("errors")
    df_quarantine = df_marked.filter(size("errors") > 0).withColumn("created_at", current_timestamp())
    
    error_percent = (invalid_count / total_count) * 100
    
    metrics = MetricsValidate(
        total_rows = total_count,
        valid_rows = valid_count,
        invalid_rows = invalid_count,
        error_percent = _round(error_percent, 2)
    )
    
    logger.info(constants.QUALITY_METRICS_LOG.format(metrics))

    if has_corrupt_col:
        if corrupt_json_count > 0:
            logger.warning(constants.BROKEN_JSON_STRINGS.format(corrupt_json_count))
    
    if error_percent > constants.MAX_ERROR_PERCENT:
        error_msg = constants.CRITICAL_ERROR_PERCENT_DETAILS.format(error_percent, constants.MAX_ERROR_PERCENT)
        raise CriticalDataQualityError(error_msg)

    df_invalid = df_quarantine.withColumn("rejected_at", current_timestamp())
            
    return df_clean, df_invalid, metrics