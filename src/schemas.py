from pyspark.sql.types import StructType, StructField, StringType, IntegerType

ref_book_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False)
])

