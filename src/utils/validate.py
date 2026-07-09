from src import constants
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, array, when

def validate(dq_config: dict):
    def _inner(df: DataFrame) -> DataFrame:
        age_condition_expr = "age < {} OR age > {}".format(dq_config['min_age'], dq_config['max_age'])
        temp_condition_expr = "temperature < {} OR temperature > {}".format(dq_config['min_temp'], dq_config['max_temp'])
            
        has_corrupt_col = "_corrupt_record" in df.columns
        corrupt_condition = "_corrupt_record IS NOT NULL" if has_corrupt_col else "false"

        return df.withColumn(
            "errors",
            array(
                when(expr(corrupt_condition), constants.ERR_CORRUPT_JSON), 
                when(expr(age_condition_expr), constants.ERR_INVALID_AGE),
                when(expr(temp_condition_expr), constants.ERR_INVALID_TEMP)
            )
        )

    return _inner
