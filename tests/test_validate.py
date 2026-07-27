import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, ArrayType
)
from src.utils.validate import validate
from src import constants


class TestValidate:
    def test_valid_data(self, spark_session, sample_dq_config):
        schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("temperature", FloatType(), True),
        ])
        data = [(35, 36.6), (25, 37.0), (0, 34.0), (120, 43.0)]
        df = spark_session.createDataFrame(data, schema)

        result_df = df.transform(validate(sample_dq_config))

        errors = result_df.select("errors").collect()
        for row in errors:
            # Для валидных данных все условия when возвращают null
            assert all(e is None for e in row["errors"])

    def test_invalid_age(self, spark_session, sample_dq_config):
        schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("temperature", FloatType(), True),
        ])
        data = [(-1, 36.6), (150, 37.0), (200, 36.0)]
        df = spark_session.createDataFrame(data, schema)

        result_df = df.transform(validate(sample_dq_config))

        errors = result_df.select("errors").collect()
        for row in errors:
            assert constants.ERR_INVALID_AGE in row["errors"]

    def test_invalid_temperature(self, spark_session, sample_dq_config):
        schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("temperature", FloatType(), True),
        ])
        data = [(30, 33.0), (40, 44.0), (50, 50.0)]
        df = spark_session.createDataFrame(data, schema)

        result_df = df.transform(validate(sample_dq_config))

        errors = result_df.select("errors").collect()
        for row in errors:
            assert constants.ERR_INVALID_TEMP in row["errors"]

    def test_null_values_are_valid(self, spark_session, sample_dq_config):
        schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("temperature", FloatType(), True),
        ])
        data = [(None, None), (35, None), (None, 36.6)]
        df = spark_session.createDataFrame(data, schema)

        result_df = df.transform(validate(sample_dq_config))

        errors = result_df.select("errors").collect()
        for row in errors:
            assert row["errors"] == [None, None, None] or row["errors"] == []

    def test_corrupt_record_detected(self, spark_session, sample_dq_config):
        schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("temperature", FloatType(), True),
            StructField("_corrupt_record", StringType(), True),
        ])
        data = [
            (35, 36.6, None),
            (25, 37.0, "broken json line"),
        ]
        df = spark_session.createDataFrame(data, schema)

        result_df = df.transform(validate(sample_dq_config))

        rows = result_df.select("_corrupt_record", "errors").collect()
        assert rows[0]["_corrupt_record"] is None
        assert rows[1]["_corrupt_record"] is not None
        assert constants.ERR_CORRUPT_JSON in rows[1]["errors"]

    def test_multiple_errors(self, spark_session, sample_dq_config):
        schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("temperature", FloatType(), True),
        ])
        data = [(-5, 50.0)]  # И возраст, и температура невалидны
        df = spark_session.createDataFrame(data, schema)

        result_df = df.transform(validate(sample_dq_config))

        row = result_df.select("errors").collect()[0]
        assert constants.ERR_INVALID_AGE in row["errors"]
        assert constants.ERR_INVALID_TEMP in row["errors"]

    def test_no_corrupt_column(self, spark_session, sample_dq_config):
        schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("temperature", FloatType(), True),
        ])
        data = [(35, 36.6)]
        df = spark_session.createDataFrame(data, schema)

        result_df = df.transform(validate(sample_dq_config))

        assert "_corrupt_record" not in result_df.columns
        errors = result_df.select("errors").collect()[0]
        assert constants.ERR_CORRUPT_JSON not in errors
