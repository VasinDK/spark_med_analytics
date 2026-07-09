import pytest
import os
import sys
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    TimestampType, ArrayType
)
from src.core.data_catalog_registry import DataCatalogRegistry

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] = f"/usr/lib/jvm/java-17-openjdk-amd64/bin:{os.environ.get('PATH', '')}"

SAMPLE_SCHEMAS_YAML = {
    "version": "1.0",
    "databases": {
        "bronze": {
            "catalog": "iceberg",
            "schema": "bronze",
            "tables": {
                "visits_raw": {
                    "status": "active",
                    "fields": [
                        {"name": "id", "type": "bigint", "nullable": False},
                        {"name": "visit_date", "type": "string", "nullable": True},
                        {"name": "age", "type": "integer", "nullable": True},
                        {"name": "temperature", "type": "float", "nullable": True},
                        {"name": "snils", "type": "string", "nullable": True},
                        {"name": "disease_code", "type": "string", "nullable": True},
                        {"name": "height", "type": "integer", "nullable": True},
                        {"name": "weight", "type": "float", "nullable": True},
                        {"name": "symptoms_code", "type": "array<string>", "nullable": True},
                        {"name": "chronic_diseases", "type": "array<string>", "nullable": True},
                        {"name": "_corrupt_record", "type": "string", "nullable": True},
                    ],
                }
            },
        },
        "silver": {
            "catalog": "iceberg",
            "schema": "silver",
            "tables": {
                "visits": {
                    "status": "active",
                    "merge_keys": ["visit_date", "snils", "disease_code"],
                    "fields": [
                        {"name": "id", "type": "string", "nullable": False},
                        {"name": "visit_date", "type": "timestamp", "nullable": True},
                        {"name": "age", "type": "integer", "nullable": True},
                        {"name": "temperature", "type": "float", "nullable": True},
                        {"name": "snils", "type": "string", "nullable": True},
                        {"name": "disease_code", "type": "string", "nullable": True},
                        {"name": "height", "type": "integer", "nullable": True},
                        {"name": "weight", "type": "float", "nullable": True},
                        {"name": "bmi", "type": "float", "nullable": True},
                        {"name": "created_at", "type": "timestamp", "nullable": True},
                        {"name": "updated_at", "type": "timestamp", "nullable": True},
                    ],
                },
                "departments": {
                    "status": "active",
                    "merge_keys": ["id"],
                    "fields": [
                        {"name": "id", "type": "integer", "nullable": False},
                        {"name": "name", "type": "string", "nullable": False},
                    ],
                },
                "professions": {
                    "status": "active",
                    "merge_keys": ["id"],
                    "fields": [
                        {"name": "id", "type": "integer", "nullable": False},
                        {"name": "name", "type": "string", "nullable": False},
                    ],
                },
                "visits_symptoms": {
                    "status": "active",
                    "fields": [
                        {"name": "visit_id", "type": "string", "nullable": False},
                        {"name": "symptoms_code", "type": "string", "nullable": False},
                        {"name": "visit_date", "type": "timestamp", "nullable": False},
                        {"name": "created_at", "type": "timestamp", "nullable": True},
                        {"name": "updated_at", "type": "timestamp", "nullable": True},
                    ],
                },
                "visits_chronic": {
                    "status": "active",
                    "fields": [
                        {"name": "visit_id", "type": "string", "nullable": False},
                        {"name": "chronic_diseases", "type": "string", "nullable": False},
                        {"name": "visit_date", "type": "timestamp", "nullable": False},
                        {"name": "created_at", "type": "timestamp", "nullable": True},
                        {"name": "updated_at", "type": "timestamp", "nullable": True},
                    ],
                },
            },
        },
        "gold": {
            "catalog": "iceberg",
            "schema": "gold",
            "tables": {
                "visits": {
                    "status": "active",
                    "merge_keys": ["id", "visit_date"],
                    "fields": [
                        {"name": "id", "type": "string", "nullable": False},
                        {"name": "visit_date", "type": "timestamp", "nullable": True},
                        {"name": "age", "type": "integer", "nullable": True},
                        {"name": "temperature", "type": "float", "nullable": True},
                        {"name": "snils", "type": "string", "nullable": True},
                        {"name": "disease_code", "type": "string", "nullable": True},
                        {"name": "height", "type": "integer", "nullable": True},
                        {"name": "weight", "type": "float", "nullable": True},
                        {"name": "bmi", "type": "float", "nullable": True},
                        {"name": "symptoms_list", "type": "array<string>", "nullable": True},
                        {"name": "chronic_list", "type": "array<string>", "nullable": True},
                        {"name": "department_name", "type": "string", "nullable": True},
                        {"name": "profession_name", "type": "string", "nullable": True},
                        {"name": "created_at", "type": "timestamp", "nullable": True},
                        {"name": "updated_at", "type": "timestamp", "nullable": True},
                    ],
                },
            },
        },
    },
}


@pytest.fixture(scope="session", autouse=True)
def spark_session():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def registry():
    return DataCatalogRegistry(SAMPLE_SCHEMAS_YAML)


@pytest.fixture
def sample_bronze_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("visit_date", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("temperature", FloatType(), True),
        StructField("snils", StringType(), True),
        StructField("disease_code", StringType(), True),
        StructField("height", IntegerType(), True),
        StructField("weight", FloatType(), True),
        StructField("symptoms_code", ArrayType(StringType()), True),
        StructField("chronic_diseases", ArrayType(StringType()), True),
        StructField("_corrupt_record", StringType(), True),
    ])


@pytest.fixture
def sample_bronze_data(spark_session, sample_bronze_schema):
    data = [
        (1, "2024-01-15 10:30:00", 35, 36.6, "123-456-789 00", "J00", 175, 75.0,
         ["R05", "R06"], ["I10"], None),
        (2, "2024-01-15 11:00:00", 150, 36.6, "987-654-321 00", "A00", 160, 60.0,
         ["R10"], [], None),
        (3, "2024-01-15 11:30:00", 25, 45.0, "111-222-333 00", "B00", 180, 80.0,
         [], ["E11"], None),
        (4, "invalid_date", 50, 37.0, "444-555-666 00", "C00", 170, 70.0,
         ["R05"], ["J45"], None),
        (5, "15.01.2024 12:00:00", 10, 36.0, "777-888-999 00", "D00", 120, 40.0,
         ["R06"], [], None),
    ]
    return spark_session.createDataFrame(data, schema=sample_bronze_schema)


@pytest.fixture
def sample_dq_config():
    return {
        "min_age": 0,
        "max_age": 120,
        "min_temp": 34.0,
        "max_temp": 43.0,
    }
