import sys
import pytest
from unittest.mock import MagicMock, patch
from src.utils.errors import handle_job_exception
from src.exceptions import (
    CriticalDataQualityError,
    QuarantineWriteError,
    ColumnNotNullError,
    NoDataGoldError,
    ConfigurationError,
    SyncTableError,
    MergeTableError,
)


class TestHandleJobException:
    def test_analysis_exception(self):
        mock_spark = MagicMock()
        from pyspark.sql.utils import AnalysisException
        exc = AnalysisException("Table not found", "")

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(1)

    def test_critical_data_quality_error(self):
        mock_spark = MagicMock()
        exc = CriticalDataQualityError()

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(2)

    def test_quarantine_write_error(self):
        mock_spark = MagicMock()
        exc = QuarantineWriteError()

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(1)

    def test_column_not_null_error(self):
        mock_spark = MagicMock()
        exc = ColumnNotNullError()

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(1)

    def test_no_data_gold_error(self):
        mock_spark = MagicMock()
        exc = NoDataGoldError()

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(0)

    def test_configuration_error(self):
        mock_spark = MagicMock()
        exc = ConfigurationError()

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(1)

    def test_sync_table_error(self):
        mock_spark = MagicMock()
        exc = SyncTableError()

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(1)

    def test_merge_table_error(self):
        mock_spark = MagicMock()
        exc = MergeTableError()

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(1)

    def test_generic_exception(self):
        mock_spark = MagicMock()
        exc = RuntimeError("Unexpected error")

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(1)

    def test_spark_stop_error(self):
        mock_spark = MagicMock()
        mock_spark.stop.side_effect = RuntimeError("Stop failed")
        exc = RuntimeError("Test error")

        with patch.object(sys, "exit") as mock_exit:
            handle_job_exception(mock_spark, exc)
            mock_exit.assert_called_once_with(1)

    def test_spark_stop_called(self):
        mock_spark = MagicMock()
        exc = RuntimeError("Test error")

        with patch.object(sys, "exit"):
            handle_job_exception(mock_spark, exc)
            mock_spark.stop.assert_called_once()
