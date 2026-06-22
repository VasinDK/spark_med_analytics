import logging
import time
import yaml
from functools import wraps
from pyspark.sql.utils import AnalysisException
from exceptions import * 
from src import constants

logger = logging.getLogger(__name__)

def monitor_job(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"=== START FUNCTION: {func.__name__} ===")
        start_time = time.perf_counter()

        try:
            return func(*args, **kwargs)
        
        except AnalysisException as e:
            error_desc = getattr(e, "desc", str(e))
            logger.error(constants.SPARK_ANALYSIS_ERROR.format(error_desc))
            raise
        except CriticalDataQualityError as e:
            logger.error(constants.SPARK_ANALYSIS_ERROR.format(e))
        except QuarantineWriteError as e:
            logger.exception(constants.S3_ERROR.format(e))
            raise
        except FileNotFoundError:
            logger.exception(constants.FILE_NOT_FOUND.format(e))
            raise
        except yaml.YAMLError as e:
            logger.exception(constants.ERROR_READING_YAML_FILE.format(e))
            raise
        except ColumnNotNullError as e:
            logger.error(e)
            raise
        except Exception as e: 
            logger.exception(constants.CRITICAL_ERROR.format(e))
            raise

        finally:
            end_time = time.perf_counter()
            minutes, seconds = divmod(int(end_time - start_time), 60)
            logger.info(f"=== END FUNCTION: {func.__name__} ===")
            logger.info(f"=== EXECUTION TIME {func.__name__}: {minutes} min {seconds} sec ===")
            
    return wrapper