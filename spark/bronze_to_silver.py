from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, DateType, ArrayType
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("BronzeToSilverIcebergJob") \
    .config("spark.sql.catalog.yandex", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.yandex.type", "hadoop") \
    .config("spark.sql.catalog.yandex.warehouse", "s3a://spark-medanalytics-dev-silver/warehouse/") \
    .config("spark.sql.catalog.yandex.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.yandex.s3.endpoint", "https://yandexcloud.net") \
    .config("spark.sql.catalog.yandex.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

print("=== СТАРТ ПРОЦЕССА ETL: BRONZE -> SILVER ===")

bronze_schema = StructType([
    StructField("id", LongType(), True),
    StructField("visit_date", StringType(), True), 
    StructField("age", IntegerType(), True),
    StructField("gender_id", IntegerType(), True),
    StructField("profession_id", IntegerType(), True),
    StructField("doctor_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("snils", StringType(), True),
    StructField("height", IntegerType(), True),
    StructField("weight", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("bp_systolic", IntegerType(), True),
    StructField("bp_diastolic", IntegerType(), True),
    StructField("disease_code", StringType(), True),
    StructField("blood_type", StringType(), True),
    StructField("symptoms_code", ArrayType(StringType()), True),
    StructField("chronic_diseases", ArrayType(StringType()), True),
    StructField("lab_hemoglobin", FloatType(), True),
    StructField("lab_leukocytes", FloatType(), True),
    StructField("lab_glucose", FloatType(), True),
    StructField("lab_cholesterol", FloatType(), True)
])

bronze_path = "s3a://spark-medanalytics-dev-bronze/patient_visits_1m.json"
print(f"Читаем данные из: {bronze_path}")

df_raw = spark.read.schema(bronze_schema).json(bronze_path)

df_silver = df_raw \
    .withColumn("visit_date", to_date(col("visit_date")))

spark.sql("CREATE DATABASE IF NOT EXISTS yandex.silver")

target_table = "yandex.silver.patient_visits"
print(f"Записываем строго типизированные данные в Iceberg: {target_table}")

df_silver.write \
    .format("iceberg") \
    .partitionBy("visit_date") \
    .mode("append") \
    .save(target_table)

print("=== ПРОВЕРКА РЕЗУЛЬТАТА ИЗ SILVER СЛОЯ ===")

spark.sql(f"SELECT * FROM {target_table} LIMIT 5").show()

print("=== ETL ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО ===")
spark.stop()
