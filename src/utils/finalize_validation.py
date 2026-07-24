import logging
from src.utils.metrics_validate import MetricsValidate
from src import constants
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, size, expr, array, when, sum as _sum


def finalize_validation(df_marked: DataFrame, metrics: MetricsValidate):
    logger = logging.getLogger(__name__)

    df_marked = df_marked.withColumn("errors", expr("array_remove(errors, null)"))
    
    has_corrupt_col = "_corrupt_record" in df_marked.columns
    corrupt_condition = "NOT (_corrupt_record IS NULL)" if has_corrupt_col else "false"
    
    aggregated_data = df_marked.agg(
        _sum(when(size("errors") == 0, 1).otherwise(0)).alias("valid_rows"),
        _sum(when(size("errors") > 0, 1).otherwise(0)).alias("invalid_rows"),
        _sum(when(expr(corrupt_condition), 1).otherwise(0)).alias("corrupt_rows")
    ).collect()

    metrics_row = aggregated_data[0]

    valid_count = metrics_row["valid_rows"] or 0
    invalid_count = metrics_row["invalid_rows"] or 0
    corrupt_json_count = metrics_row["corrupt_rows"] or 0
    total_count = valid_count + invalid_count
    
    if total_count == 0:
        logger.warning(constants.EMPTY_INPUT_WARNING)
        return df_marked.drop("errors"), df_marked.drop("errors")

    df_clean = df_marked.filter(size("errors") == 0).drop("errors")
    df_quarantine = df_marked.filter(size("errors") > 0).withColumn("created_at", current_timestamp())
    
    error_percent = (invalid_count / total_count) * 100
    
    metrics.total_rows = total_count
    metrics.valid_rows = valid_count
    metrics.invalid_rows = invalid_count
    metrics.error_percent = round(error_percent, 2)
    
    logger.info(constants.QUALITY_METRICS_LOG.format(metrics))

    if has_corrupt_col and corrupt_json_count > 0:
        logger.warning(constants.BROKEN_JSON_STRINGS.format(corrupt_json_count))

    df_invalid = df_quarantine.withColumn("rejected_at", current_timestamp())
            
    return df_clean, df_invalid