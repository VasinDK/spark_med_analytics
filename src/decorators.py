import logging
import time
from functools import wraps
from exceptions import * 

logger = logging.getLogger(__name__)

def monitor_job(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"=== START FUNCTION: {func.__name__} ===")
        start_time = time.perf_counter()

        try:
            return func(*args, **kwargs)

        finally:
            end_time = time.perf_counter()
            minutes, seconds = divmod(int(end_time - start_time), 60)
            logger.info(f"=== END FUNCTION: {func.__name__} ===")
            logger.info(f"=== EXECUTION TIME {func.__name__}: {minutes} min {seconds} sec ===")
            
    return wrapper