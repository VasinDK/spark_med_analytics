import sys
from src.logging_config import setup_logging
from src.schemas import bronze_schema
from src.decorators import monitor_job
from src.utils import get_spark_session, build_s3_path, validate,  add_id, add_quarantine
from pyspark.sql import SparkSession

TEMP_BRONZE_DATA = "temp_bronze_data"

def create_tables(spark: SparkSession, db_schema: str, tables: dict):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_schema}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tables["visits"]} (
            id STRING, visit_date DATE, age INT, gender_id INT, profession_id INT, doctor_id INT, department_id INT,
            snils STRING, height INT, weight FLOAT, temperature FLOAT, bp_systolic INT, bp_diastolic INT,
            disease_code STRING, blood_type STRING, lab_hemoglobin FLOAT, lab_leukocytes FLOAT, lab_glucose FLOAT, lab_cholesterol FLOAT
        ) USING iceberg PARTITIONED BY (visit_date)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tables["visits_symptoms"]}(
            visit_id STRING, symptoms_code STRING
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tables["visits_chronic_dis"]}(
            visit_id STRING, chronic_diseases STRING
        ) USING iceberg
    """)

def upsert_tables(spark: SparkSession, tables: dict):
    spark.sql(f"""
        MERGE INTO {tables["visits"]} t
        USING {TEMP_BRONZE_DATA} td
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
        DELETE FROM {tables["visits_symptoms"]} 
        WHERE visit_id IN (SELECT id FROM {TEMP_BRONZE_DATA})
    """)

    spark.sql(f"""
        DELETE FROM {tables["visits_chronic_dis"]} 
        WHERE visit_id IN (SELECT id FROM {TEMP_BRONZE_DATA})
    """)

    spark.sql(f"""
        INSERT INTO {tables["visits_symptoms"]}
        SELECT id AS visit_id, explode(symptoms_code) AS symptoms_code 
        FROM {TEMP_BRONZE_DATA}
        WHERE symptoms_code IS NOT NULL
    """)

    spark.sql(f"""
        INSERT INTO {tables["visits_chronic_dis"]}
        SELECT id AS visit_id, explode(chronic_diseases) AS chronic_diseases 
        FROM {TEMP_BRONZE_DATA}
        WHERE chronic_diseases IS NOT NULL
    """)
    
@monitor_job
def run_etl_silver():
    spark, config = get_spark_session(sys.argv)

    try:
        bronze_path = f"s3a://{build_s3_path(config["s3"]["visits_json"])}"

        df_raw = (
            spark.read.schema(bronze_schema)
            .option("multiline", "true")
            .json(bronze_path)
        )

        df_clean, df_quarantine, metrics = validate(spark, df_raw, config["dq_rule"])
        add_quarantine(df_quarantine, build_s3_path(config["s3"]["quarantine_path"]))
        df_silver = df_clean.transform(add_id)
        df_silver.localCheckpoint(eager=True)
        df_silver.createOrReplaceTempView(TEMP_BRONZE_DATA)

        db_schema = f"{config['db']['catalog']}.{config['db']['schema']}"
        tables = {
            "visits": f"{db_schema}.{config['db']['table']['visits']}",
            "visits_symptoms": f"{db_schema}.{config['db']['table']['symptoms']}",
            "visits_chronic_dis": f"{db_schema}.{config['db']['table']['chronic']}"
        }
        create_tables(spark, db_schema, tables)
        upsert_tables(spark, tables)

    finally:
        spark.stop()


if __name__ == "__main__":
    setup_logging()
    run_etl_silver()
