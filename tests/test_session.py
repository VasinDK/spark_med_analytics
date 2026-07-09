import pytest
from unittest.mock import MagicMock, patch
from src.core.session import get_spark_session
from src.exceptions import ConfigurationNotFoundError


class TestGetSparkSession:
    def test_get_spark_session_success(self):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        mock_config = {"log_level": {"py4j": 40}}

        with patch("src.core.session.SparkSession.builder", mock_builder):
            with patch("src.core.session.configuration.load_s3_yaml_config", return_value=mock_config):
                spark, config = get_spark_session(["app_name", "s3a://bucket/config.yaml"])

                assert spark is not None
                assert config == mock_config
                mock_builder.appName.assert_called_once_with("app_name")
                mock_builder.getOrCreate.assert_called_once()

    def test_get_spark_session_no_args(self):
        with pytest.raises(ConfigurationNotFoundError):
            get_spark_session([])

    def test_get_spark_session_single_arg(self):
        with pytest.raises(ConfigurationNotFoundError):
            get_spark_session(["app_name"])

    def test_get_spark_session_timezone_config(self):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        mock_config = {"log_level": {"py4j": 40}}

        with patch("src.core.session.SparkSession.builder", mock_builder):
            with patch("src.core.session.configuration.load_s3_yaml_config", return_value=mock_config):
                get_spark_session(["app_name", "s3a://bucket/config.yaml"])

                config_calls = [
                    call for call in mock_builder.config.call_args_list
                    if call[0][0] == "spark.sql.session.timeZone"
                ]
                assert len(config_calls) > 0
                assert config_calls[0][0][1] == "UTC"
