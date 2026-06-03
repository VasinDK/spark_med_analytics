import logging
import time
from functools import wraps
from pyspark.sql.utils import AnalysisException
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
            logger.error(constants.SPARK_ANALYSIS_ERROR.format(e.desc))
            raise
        except Exception as e: 
            logger.exception(constants.CRITICAL_ERROR.format(e))
            raise

        finally:
            logger.info(f"=== END FUNCTION: {func.__name__} ===")
            end_time = time.perf_counter()
            minutes, seconds = divmod(int(end_time - start_time), 60)
            logger.info(f"=== EXECUTION TIME {func.__name__}: {minutes} min {seconds} sec ===")
            
    return wrapper