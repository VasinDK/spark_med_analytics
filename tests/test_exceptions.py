import pytest
from src.exceptions import (
    MedAnalyticsError,
    ConfigurationNotFoundError,
    ConfigurationError,
    InvalidS3PathError,
    CriticalDataQualityError,
    QuarantineWriteError,
    ColumnNotNullError,
    SyncTableError,
    NoDataGoldError,
    MergeTableError,
)
from src import constants


class TestMedAnalyticsError:
    def test_base_exception(self):
        with pytest.raises(MedAnalyticsError):
            raise MedAnalyticsError("test error")

    def test_inheritance(self):
        assert issubclass(ConfigurationNotFoundError, MedAnalyticsError)
        assert issubclass(ConfigurationError, MedAnalyticsError)
        assert issubclass(InvalidS3PathError, MedAnalyticsError)
        assert issubclass(CriticalDataQualityError, MedAnalyticsError)
        assert issubclass(QuarantineWriteError, MedAnalyticsError)
        assert issubclass(ColumnNotNullError, MedAnalyticsError)
        assert issubclass(SyncTableError, MedAnalyticsError)
        assert issubclass(NoDataGoldError, MedAnalyticsError)
        assert issubclass(MergeTableError, MedAnalyticsError)


class TestConfigurationNotFoundError:
    def test_default_message(self):
        err = ConfigurationNotFoundError()
        assert str(err) == constants.CONFIGURATION_NOT_FOUND_ERROR

    def test_custom_message(self):
        err = ConfigurationNotFoundError("custom message")
        assert str(err) == "custom message"


class TestConfigurationError:
    def test_default_message(self):
        err = ConfigurationError()
        assert str(err) == constants.CONFIGURATION_ERROR

    def test_custom_message(self):
        err = ConfigurationError("custom config error")
        assert str(err) == "custom config error"


class TestInvalidS3PathError:
    def test_with_path(self):
        err = InvalidS3PathError("s3a://bucket/key")
        expected = constants.INVALID_S3_PATH_ERROR.format("s3a://bucket/key")
        assert str(err) == expected

    def test_with_none_path(self):
        err = InvalidS3PathError(None)
        expected = constants.INVALID_S3_PATH_ERROR.format(None)
        assert str(err) == expected


class TestCriticalDataQualityError:
    def test_default_message(self):
        err = CriticalDataQualityError()
        assert str(err) == constants.CRITICAL_ERROR_PERCENT

    def test_custom_message(self):
        err = CriticalDataQualityError("custom quality error")
        assert str(err) == "custom quality error"


class TestQuarantineWriteError:
    def test_default_message(self):
        err = QuarantineWriteError()
        assert str(err) == constants.QUARANTINE_WRITE_ERROR

    def test_custom_message(self):
        err = QuarantineWriteError("custom quarantine error")
        assert str(err) == "custom quarantine error"


class TestColumnNotNullError:
    def test_default_message(self):
        err = ColumnNotNullError()
        assert str(err) == constants.COLUMN_NOT_NULL

    def test_custom_message(self):
        err = ColumnNotNullError("custom not null error")
        assert str(err) == "custom not null error"


class TestSyncTableError:
    def test_default_message(self):
        err = SyncTableError()
        assert str(err) == constants.SYNC_TABLE_ERROR

    def test_custom_message(self):
        err = SyncTableError("custom sync error")
        assert str(err) == "custom sync error"


class TestNoDataGoldError:
    def test_default_message(self):
        err = NoDataGoldError()
        assert str(err) == constants.NO_NEW_CHANGED_DATA_GOLD

    def test_custom_message(self):
        err = NoDataGoldError("custom no data error")
        assert str(err) == "custom no data error"


class TestMergeTableError:
    def test_default_message(self):
        err = MergeTableError()
        assert str(err) == constants.MERGE_KEYS_ERROR

    def test_custom_message(self):
        err = MergeTableError("custom merge error")
        assert str(err) == "custom merge error"
