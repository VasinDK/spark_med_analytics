import sys
import pytest
from unittest.mock import MagicMock, patch, call
from src.core.session import get_spark_session


class TestGetSparkSession:
    def test_get_spark_session_success(self):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        mock_config = {"log_level": {"py4j": 40}}

        with patch.object(sys, "argv", ["test_script.py"]):
            with patch("src.core.session.SparkSession.builder", mock_builder):
                spark = get_spark_session(mock_config)

                assert spark is not None
                mock_builder.appName.assert_called_once_with("test_script.py")
                mock_builder.getOrCreate.assert_called_once()

    def test_get_spark_session_timezone_config(self):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        mock_config = {"log_level": {"py4j": 40}}

        with patch.object(sys, "argv", ["test_script.py"]):
            with patch("src.core.session.SparkSession.builder", mock_builder):
                get_spark_session(mock_config)

                config_calls = [
                    call for call in mock_builder.config.call_args_list
                    if call[0][0] == "spark.sql.session.timeZone"
                ]
                assert len(config_calls) > 0
                assert config_calls[0][0][1] == "UTC"
