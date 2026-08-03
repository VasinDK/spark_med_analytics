from unittest.mock import MagicMock, patch
from src.utils.s3 import build_s3_path, read_s3_csv


class TestBuildS3Path:
    def test_basic_path(self):
        config = {"bucket": "my-bucket", "path": "data/file.csv"}
        result = build_s3_path(config)
        assert result == "s3a://my-bucket/data/file.csv"

    def test_path_with_leading_slash(self):
        config = {"bucket": "my-bucket", "path": "/data/file.csv"}
        result = build_s3_path(config)
        assert result == "s3a://my-bucket/data/file.csv"

    def test_bucket_with_trailing_slash(self):
        config = {"bucket": "my-bucket/", "path": "data/file.csv"}
        result = build_s3_path(config)
        assert result == "s3a://my-bucket/data/file.csv"

    def test_both_with_slashes(self):
        config = {"bucket": "my-bucket/", "path": "/data/file.csv"}
        result = build_s3_path(config)
        assert result == "s3a://my-bucket/data/file.csv"

    def test_nested_path(self):
        config = {"bucket": "bucket-name", "path": "level1/level2/file.csv"}
        result = build_s3_path(config)
        assert result == "s3a://bucket-name/level1/level2/file.csv"


class TestReadS3Csv:
    def test_read_s3_csv_defaults(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.schema.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.csv.return_value = MagicMock()

        schema = MagicMock()
        result = read_s3_csv(mock_spark, "s3a://bucket/file.csv", schema)

        mock_reader.schema.assert_called_once_with(schema)
        mock_reader.option.assert_any_call("header", True)
        mock_reader.option.assert_any_call("delimiter", ";")
        mock_reader.csv.assert_called_once_with("s3a://bucket/file.csv")

    def test_read_s3_csv_custom_options(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.schema.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.csv.return_value = MagicMock()

        schema = MagicMock()
        result = read_s3_csv(
            mock_spark, "s3a://bucket/file.csv", schema, has_header=False, delimiter=","
        )

        mock_reader.option.assert_any_call("header", False)
        mock_reader.option.assert_any_call("delimiter", ",")
