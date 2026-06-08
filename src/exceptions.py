from src import constants

class MedAnalyticsError(Exception):
    pass

class ConfigurationNotFoundError(MedAnalyticsError):
    def __init__(self, message: str = None):
        default_message = constants.CONFIGURATION_NOT_FOUND_ERROR
        super().__init__(message or default_message)

class InvalidS3PathError(MedAnalyticsError):
    def __init__(self, s3_path: str = None):
        message = constants.INVALID_S3_PATH_ERROR.format(s3_path)
        super().__init__(message)

class CriticalDataQualityError(MedAnalyticsError):
    def __init__(self, message: str = None):
        default_message = constants.CRITICAL_ERROR_PERCENT
        super().__init__(message or default_message)
        
class QuarantineWriteError(MedAnalyticsError):
    def __init__(self, message: str = None):
        default_message = constants.QUARANTINE_WRITE_ERROR
        super().__init__(message or default_message)