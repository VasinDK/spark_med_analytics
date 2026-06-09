from pyspark.sql.types import StructType, StructField, StringType, \
    IntegerType, FloatType, ArrayType, LongType, DateType
from dataclasses import dataclass, asdict

bronze = StructType([
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

departments = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False)
])

professions = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False)
])

# gold_schema = StructType([
#         StructField("id", StringType(), False),
#         StructField("visit_date", DateType(), True), 
#         StructField("age", IntegerType(), True),
#         StructField("gender_id", IntegerType(), True),
#         StructField("profession_id", IntegerType(), True),
#         StructField("doctor_id", IntegerType(), True),
#         StructField("department_id", IntegerType(), True),
#         StructField("snils", StringType(), True),
#         StructField("height", IntegerType(), True),
#         StructField("weight", FloatType(), True),
#         StructField("temperature", FloatType(), True),
#         StructField("bp_systolic", IntegerType(), True),
#         StructField("bp_diastolic", IntegerType(), True),
#         StructField("disease_code", StringType(), True),
#         StructField("blood_type", StringType(), True),
#         StructField("symptoms_code", ArrayType(StringType()), True),
#         StructField("chronic_diseases", ArrayType(StringType()), True),
#         StructField("lab_hemoglobin", FloatType(), True),
#         StructField("lab_leukocytes", FloatType(), True),
#         StructField("lab_glucose", FloatType(), True),
#         StructField("lab_cholesterol", FloatType(), True)
#     ])

visits = StructType([
    StructField("id", StringType(), False),
    StructField("visit_date", DateType(), True),
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
    StructField("lab_hemoglobin", FloatType(), True),
    StructField("lab_leukocytes", FloatType(), True),
    StructField("lab_glucose", FloatType(), True),
    StructField("lab_cholesterol", FloatType(), True)
])

symptoms = StructType([
    StructField("visit_id", StringType(), False),
    StructField("symptoms_code", StringType(), False)
])

chronic = StructType([
    StructField("visit_id", StringType(), False),
    StructField("chronic_diseases", StringType(), False)
])

@dataclass
class Metrics:
    spark_app_id: str = ''
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    error_percent: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)