from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType
from datetime import datetime
from src.utils.db import get_last_date


class TestGetLastDate:
    def test_get_last_date_with_data(self, spark_session):
        schema = StructType([
            StructField("created_at", TimestampType(), True),
        ])
        data = [
            (datetime(2024, 1, 1, 10, 0, 0),),
            (datetime(2024, 1, 2, 10, 0, 0),),
            (datetime(2024, 1, 3, 10, 0, 0),),
        ]
        df = spark_session.createDataFrame(data, schema)

        result = get_last_date(df)
        assert result == datetime(2024, 1, 3, 10, 0, 0)

    def test_get_last_date_single_row(self, spark_session):
        schema = StructType([
            StructField("created_at", TimestampType(), True),
        ])
        data = [(datetime(2024, 6, 15, 8, 30, 0),)]
        df = spark_session.createDataFrame(data, schema)

        result = get_last_date(df)
        assert result == datetime(2024, 6, 15, 8, 30, 0)

    def test_get_last_date_empty_df(self, spark_session):
        schema = StructType([
            StructField("created_at", TimestampType(), True),
        ])
        df = spark_session.createDataFrame([], schema)

        result = get_last_date(df)
        assert result is None

    def test_get_last_date_all_nulls(self, spark_session):
        schema = StructType([
            StructField("created_at", TimestampType(), True),
        ])
        data = [(None,), (None,)]
        df = spark_session.createDataFrame(data, schema)

        result = get_last_date(df)
        assert result is None
