import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
from pyspark.sql.functions import col, to_date, md5, concat_ws

def run_etl():
    logging.getLogger("py4j").setLevel(logging.ERROR)

    spark = SparkSession.builder \
        .appName("BronzeToSilverIcebergJob") \
        .config("spark.sql.catalog.yandex", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.yandex.type", "hadoop") \
        .config("spark.sql.catalog.yandex.warehouse", "s3a://spark-medanalytics-dev-silver/warehouse/") \
        .config("spark.sql.catalog.yandex.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.yandex.s3.endpoint", "https://storage.yandexcloud.net") \
        .config("spark.sql.catalog.yandex.s3.path-style-access", "true") \
        .config("spark.sql.catalog.yandex.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.logLevel", "WARN") \
        .getOrCreate()

    bronze_schema = StructType([
        StructField("id", LongType(), False),
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

    bronze_path = "s3a://spark-medanalytics-dev-bronze/10_patient_visits_1m.json"
    df_raw = spark.read.schema(bronze_schema).option("multiline", "true").json(bronze_path)

    df_silver = df_raw \
        .withColumn("visit_date", to_date(col("visit_date"))) \
        .withColumn("id", md5(concat_ws("-", col("visit_date"), col("snils"),col("disease_code"))))

    df_silver.createOrReplaceTempView("temp_bronze_data")

    target_table_visits                     = "yandex.silver.visits"
    target_table_visits_symptoms_code       = "yandex.silver.visits_symptoms_code"
    target_table_visits_chronic_diseases    = "yandex.silver.visits_chronic_diseases"

    spark.sql("CREATE DATABASE IF NOT EXISTS yandex.silver")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table_visits} (
            id STRING, visit_date DATE, age INT, gender_id INT, profession_id INT, doctor_id INT, department_id INT,
            snils STRING, height INT, weight FLOAT, temperature FLOAT, bp_systolic INT, bp_diastolic INT,
            disease_code STRING, blood_type STRING, lab_hemoglobin FLOAT, lab_leukocytes FLOAT, lab_glucose FLOAT, lab_cholesterol FLOAT
        ) USING iceberg PARTITIONED BY (visit_date)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table_visits_symptoms_code}(
            visit_id STRING, symptoms_code STRING
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table_visits_chronic_diseases}(
            visit_id STRING, chronic_diseases STRING
        ) USING iceberg
    """)

    spark.sql(f"""
        MERGE INTO {target_table_visits} t
        USING temp_bronze_data td
        ON t.visit_date = td.visit_date AND t.snils = td.snils AND t.disease_code = td.disease_code
        WHEN MATCHED THEN
            UPDATE SET t.age = td.age, t.weight = td.weight, t.temperature = td.temperature, 
                    t.bp_systolic = td.bp_systolic, t.bp_diastolic = td.bp_diastolic,
                    t.lab_hemoglobin = td.lab_hemoglobin, t.lab_glucose = td.lab_glucose, t.lab_cholesterol = td.lab_cholesterol
        WHEN NOT MATCHED THEN
            INSERT (id, visit_date, age, gender_id, profession_id, doctor_id, department_id, snils, height, weight, 
                    temperature, bp_systolic, bp_diastolic, disease_code, blood_type, lab_hemoglobin, lab_leukocytes, lab_glucose, lab_cholesterol)
            VALUES (td.id, td.visit_date, td.age, td.gender_id, td.profession_id, td.doctor_id, td.department_id, td.snils, td.height, td.weight, 
                    td.temperature, td.bp_systolic, td.bp_diastolic, td.disease_code, td.blood_type, td.lab_hemoglobin, td.lab_leukocytes, td.lab_glucose, td.lab_cholesterol)
    """)

    spark.sql(f"""
        SELECT id AS real_visit_id, visit_date, snils, disease_code 
        FROM {target_table_visits}
    """).createOrReplaceTempView("v_actual_ids")

    spark.sql("""
        SELECT a.real_visit_id, b.symptoms_code, b.chronic_diseases
        FROM temp_bronze_data b
        JOIN v_actual_ids a 
        ON a.visit_date = b.visit_date AND a.snils = b.snils AND a.disease_code = b.disease_code
    """).createOrReplaceTempView("temp_prepared_child_data")

    spark.sql(f"""
        DELETE FROM {target_table_visits_symptoms_code} 
        WHERE visit_id IN (SELECT real_visit_id FROM temp_prepared_child_data)
    """)

    spark.sql(f"""
        DELETE FROM {target_table_visits_chronic_diseases} 
        WHERE visit_id IN (SELECT real_visit_id FROM temp_prepared_child_data)
    """)

    spark.sql(f"""
        INSERT INTO {target_table_visits_symptoms_code}
        SELECT real_visit_id AS visit_id, explode(symptoms_code) AS symptoms_code 
        FROM temp_prepared_child_data
    """)

    spark.sql(f"""
        INSERT INTO {target_table_visits_chronic_diseases}
        SELECT real_visit_id AS visit_id, explode(chronic_diseases) AS chronic_diseases 
        FROM temp_prepared_child_data
    """)
    print("=== ETL ПРОЦЕСС СВЯЗАННЫХ ТАБЛИЦ ЗАВЕРШЕН УСПЕШНО ===")
    spark.stop()

if __name__ == "__main__":
    start_time = time.perf_counter()
    run_etl()
    end_time = time.perf_counter()
    execute_time = end_time - start_time
    print(f"=== время выполнения {execute_time // 60} минут и {execute_time % 60} секунд ===")