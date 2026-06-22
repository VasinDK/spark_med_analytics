from pyspark.sql import SparkSession, DataFrame

def build_s3_path(s3_file_config: dict) -> str:
    bucket = s3_file_config["bucket"].strip("/")
    path = s3_file_config["path"].lstrip("/") 
    
    return f"s3a://{bucket}/{path}"

def read_s3_csv(spark: SparkSession, s3_path: str, schema, has_header=True, delimiter=";") -> DataFrame:
    return (spark.read
            .schema(schema)
            .option("header", has_header)
            .option("delimiter", delimiter)
            .csv(s3_path))