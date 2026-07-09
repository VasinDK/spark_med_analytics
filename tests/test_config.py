import os
import pytest
from unittest.mock import MagicMock, patch
from src.config import load_s3_yaml_config


class TestLoadS3YamlConfig:
    def test_load_s3_yaml_config_success(self):
        mock_spark = MagicMock()
        mock_rdd = MagicMock()
        mock_spark.sparkContext.textFile.return_value = mock_rdd
        mock_rdd.collect.return_value = [
            "s3:",
            "  bucket: test-bucket",
            "  key: test-key",
        ]

        result = load_s3_yaml_config(mock_spark, "s3a://bucket/config.yaml")

        mock_spark.sparkContext.textFile.assert_called_once_with("s3a://bucket/config.yaml")
        assert result == {"s3": {"bucket": "test-bucket", "key": "test-key"}}

    def test_load_s3_yaml_config_empty(self):
        mock_spark = MagicMock()
        mock_rdd = MagicMock()
        mock_spark.sparkContext.textFile.return_value = mock_rdd
        mock_rdd.collect.return_value = []

        result = load_s3_yaml_config(mock_spark, "s3a://bucket/config.yaml")
        assert result is None

    def test_load_s3_yaml_config_invalid_yaml(self):
        mock_spark = MagicMock()
        mock_rdd = MagicMock()
        mock_spark.sparkContext.textFile.return_value = mock_rdd
        mock_rdd.collect.return_value = ["invalid: yaml: ["]

        with pytest.raises(Exception):
            load_s3_yaml_config(mock_spark, "s3a://bucket/config.yaml")
