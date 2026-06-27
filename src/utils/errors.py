import sys
import logging
import yaml
from src.exceptions import *
from pyspark.sql.utils import AnalysisException
from src import constants

logger = logging.getLogger(__name__)

def handle_job_exception(spark, e: Exception):
    try:
        if isinstance(e, AnalysisException):
            error_desc = getattr(e, "desc", str(e))
            logger.error(constants.SPARK_ANALYSIS_ERROR.format(error_desc))
            sys.exit(1)
            
        elif isinstance(e, CriticalDataQualityError):
            logger.error(constants.CRITICAL_ERROR.format(e))
            sys.exit(2)
            
        elif isinstance(e, QuarantineWriteError):
            logger.exception(constants.S3_ERROR.format(e))
            sys.exit(1)

        elif isinstance(e, yaml.YAMLError):
            logger.exception(constants.ERROR_READING_YAML_FILE.format(e))
            sys.exit(1)

        elif isinstance(e, ColumnNotNullError):
            logger.error(e)
            sys.exit(1)

        elif isinstance(e, NoDataGoldError):
            logger.info(e)
            sys.exit(0)

        elif isinstance(e, ConfigurationError):
            logger.exception(e)
            sys.exit(1)

        elif isinstance(e, SyncTableError):
            logger.error(e)
            sys.exit(1)

        elif isinstance(e, MergeTableError):
            logger.error(e)
            sys.exit(1)

        else:
            logger.exception(constants.CRITICAL_ERROR.format(e))
            sys.exit(1)

    finally:
        try:
            spark.stop() 
        except Exception as e:
            logger.error(constants.SPARK_STOP_ERROR.format(e))