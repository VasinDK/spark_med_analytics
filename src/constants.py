CRITICAL_ERROR = "\n[CRITICAL ERROR] Unexpected failure: {}\n"
SPARK_ANALYSIS_ERROR = "\n[ERROR SPARK] Data or SQL failure: {}"
CONFIGURATION_NOT_FOUND_ERROR = (
    "Missing required positional argument: S3 URI to the configuration YAML file. "
    "Usage: spark-submit <script.py> s3a://<bucket>/<path_to_config>.yaml"
)
INVALID_S3_PATH_ERROR = "The provided S3 path is invalid or inaccessible: {}"

DEFAULT_LOG_FILENAME = "med_analytics_etl.log"
