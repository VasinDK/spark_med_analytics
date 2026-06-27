from src import constants
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, array, when

def validate(dq_config: dict):
    def _inner(df: DataFrame) -> DataFrame:
        age_filter = "age >= {} AND age <= {}".format(dq_config['min_age'], dq_config['max_age'])
        temp_filter = "temperature >= {} AND temperature <= {}".format(dq_config['min_temp'], dq_config['max_temp'])
        
        age_condition_expr = "NOT ({}) OR age IS NULL".format(age_filter)
        temp_condition_expr = "NOT ({}) OR temperature IS NULL".format(temp_filter)
            
        has_corrupt_col = "_corrupt_record" in df.columns
        corrupt_condition = "NOT (_corrupt_record IS NULL)" if has_corrupt_col else "false"
        

        return df.withColumn(
            "errors",
            array(
                when(expr(corrupt_condition), constants.ERR_CORRUPT_JSON), 
                when(expr(age_condition_expr), constants.ERR_INVALID_AGE),
                when(expr(temp_condition_expr), constants.ERR_INVALID_TEMP)
            )
        )

    return _inner
    