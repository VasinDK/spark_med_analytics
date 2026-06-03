from pyspark.sql.types import StructType, StructField, StringType, IntegerType

departments = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False)
])

professions = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False)
])
