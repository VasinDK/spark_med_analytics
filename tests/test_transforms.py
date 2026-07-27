import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    TimestampType, ArrayType
)
from src.transforms import cast_bronze, cast_visit_date, add_id, add_bmi
from src.core.data_catalog_registry import DataCatalogRegistry

class TestCastBronze:
    def _full_bronze_df(self, spark_session, overrides: dict = None):
        """Создаёт DataFrame со всеми полями bronze схемы для cast_bronze"""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("visit_date", StringType(), True),
            StructField("age", StringType(), True),
            StructField("temperature", StringType(), True),
            StructField("snils", StringType(), True),
            StructField("disease_code", StringType(), True),
            StructField("height", StringType(), True),
            StructField("weight", StringType(), True),
            StructField("symptoms_code", StringType(), True),
            StructField("chronic_diseases", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ])
        default = ("1", "2024-01-15 10:30:00", "35", "36.6", "123-456-789 00", "J00", "175", "75.0", '["R05","R06"]', '["I10"]', None)
        if overrides:
            default = tuple(overrides.get(f.name, default[i]) for i, f in enumerate(schema.fields))
        data = [default]
        return spark_session.createDataFrame(data, schema)

class TestCastVisitDate:
    def test_null_date(self, spark_session):
        schema = StructType([StructField("visit_date", StringType(), True)])
        data = [(None,)]
        df = spark_session.createDataFrame(data, schema)

        result_df = cast_visit_date(df)
        row = result_df.collect()[0]
        assert row["visit_date"] is None


class TestAddId:
    def test_add_id_creates_md5(self, spark_session):
        schema = StructType([
            StructField("visit_date", StringType(), True),
            StructField("snils", StringType(), True),
            StructField("disease_code", StringType(), True),
        ])
        data = [("2024-01-15 10:30:00", "123-456-789 00", "J00")]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_id(df)
        row = result_df.collect()[0]
        assert row["id"] is not None
        assert isinstance(row["id"], str)
        assert len(row["id"]) == 32

    def test_add_id_deterministic(self, spark_session):
        schema = StructType([
            StructField("visit_date", StringType(), True),
            StructField("snils", StringType(), True),
            StructField("disease_code", StringType(), True),
        ])
        data = [
            ("2024-01-15 10:30:00", "123-456-789 00", "J00"),
            ("2024-01-15 10:30:00", "123-456-789 00", "J00"),
        ]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_id(df)
        rows = result_df.collect()
        assert rows[0]["id"] == rows[1]["id"]

    def test_add_id_different_data_different_id(self, spark_session):
        schema = StructType([
            StructField("visit_date", StringType(), True),
            StructField("snils", StringType(), True),
            StructField("disease_code", StringType(), True),
        ])
        data = [
            ("2024-01-15 10:30:00", "123-456-789 00", "J00"),
            ("2024-01-16 10:30:00", "987-654-321 00", "A00"),
        ]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_id(df)
        rows = result_df.collect()
        assert rows[0]["id"] != rows[1]["id"]

    def test_add_id_with_nulls(self, spark_session):
        schema = StructType([
            StructField("visit_date", StringType(), True),
            StructField("snils", StringType(), True),
            StructField("disease_code", StringType(), True),
        ])
        data = [(None, None, None)]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_id(df)
        row = result_df.collect()[0]
        assert row["id"] is not None
        assert isinstance(row["id"], str)


class TestAddBmi:
    def test_add_bmi_valid(self, spark_session):
        schema = StructType([
            StructField("height", IntegerType(), True),
            StructField("weight", FloatType(), True),
        ])
        data = [(175, 75.0)]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_bmi(df)
        row = result_df.collect()[0]
        assert row["bmi"] is not None
        assert round(row["bmi"], 1) == 24.5

    def test_add_bmi_zero_height(self, spark_session):
        schema = StructType([
            StructField("height", IntegerType(), True),
            StructField("weight", FloatType(), True),
        ])
        data = [(0, 75.0)]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_bmi(df)
        row = result_df.collect()[0]
        assert row["bmi"] is None

    def test_add_bmi_zero_weight(self, spark_session):
        schema = StructType([
            StructField("height", IntegerType(), True),
            StructField("weight", FloatType(), True),
        ])
        data = [(175, 0.0)]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_bmi(df)
        row = result_df.collect()[0]
        assert row["bmi"] is None

    def test_add_bmi_null_values(self, spark_session):
        schema = StructType([
            StructField("height", IntegerType(), True),
            StructField("weight", FloatType(), True),
        ])
        data = [(None, None)]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_bmi(df)
        row = result_df.collect()[0]
        assert row["bmi"] is None

    def test_add_bmi_precision(self, spark_session):
        schema = StructType([
            StructField("height", IntegerType(), True),
            StructField("weight", FloatType(), True),
        ])
        data = [(170, 65.0)]
        df = spark_session.createDataFrame(data, schema)

        result_df = add_bmi(df)
        row = result_df.collect()[0]
        assert row["bmi"] is not None
        bmi_str = str(row["bmi"])
        assert row["bmi"] == 22.5
