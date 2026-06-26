from pyspark.sql import DataFrame
from pyspark.sql.functions import max as _max

def get_last_date(df: DataFrame):
    row = df.select(_max("created_at")).collect()

    if row and row[0][0] is not None:
        return row[0][0]
    
    return None