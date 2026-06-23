CRITICAL_ERROR = "[CRITICAL ERROR] Unexpected failure: {}"
SPARK_ANALYSIS_ERROR = "[SPARK ERROR] Data or SQL failure: {}"
CONFIGURATION_NOT_FOUND_ERROR = (
    "Missing required positional argument: S3 URI to the configuration YAML file. "
    "Usage: spark-submit <script.py> s3a://<bucket>/<path_to_config>.yaml"
)
INVALID_S3_PATH_ERROR = "The provided S3 path is invalid or inaccessible: {}"
ATTENTION_INCORRECT_LINES = "[DATA ERROR] attention: Incorrect lines were detected. Quarantined: {}"

ERR_INVALID_AGE = "INVALID_AGE"
ERR_INVALID_TEMP = "INVALID_TEMPERATURE"

DEFAULT_LOG_FILENAME = "med_analytics_etl.log"
QUALITY_METRICS = "Quality Metrics"
MAX_ERROR_PERCENT = 5.0
DELETING_OUTDATED_COLUMN = "Deleting the outdated column '{}' from {}"
ADDING_NEW_COLUMN = "Adding a new column '{}' ({}) to {}"
CHANGING_COLUMN_TYPE ="Changing the column type '{}' to {}: {} -> {}"
REPORT_SCHEMA_SYNCHRONIZATION = "FINAL SCHEMA SYNCHRONIZATION REPORT"

SPARK_APP_ID_LOG = "Spark Application ID: {}"
EMPTY_INPUT_WARNING = "Input DataFrame is empty. Processing stopped."
QUALITY_METRICS_LOG = "=== Quality Metrics: {} ==="
CRITICAL_ERROR_PERCENT = "[CRITICAL ERROR] Error percent exceeds allowed limit"
CRITICAL_ERROR_PERCENT_DETAILS = "[CRITICAL ERROR] Error percent {:.2f}% exceeds allowed limit of {}%! Processing stopped."
QUARANTINE_WRITE_ERROR = "Failed to route invalid data to quarantine. {}"
S3_ERROR = "s3 error: {}"

CREATING_NEW_TABLE = "Creating a new table: {}"
TABLE_NOT_FOUND_ERROR = "The table '{}' is described in YAML, but its address is not found in the layer configuration."
COLUMN_NOT_NULL = "The column being added to the iceberg cannot be 'not null'"

UPLOADING_FILE = "Uploading the file along the path: {}"
FILE_NOT_FOUND ="The file was not found on the way: {}"
ERROR_READING_YAML_FILE ="The file was not found on the way: {}"

BROKEN_JSON_STRINGS = "Attention! {corrupt_count} broken JSON strings found"
ERR_CORRUPT_JSON = "Malformed JSON record could not be parsed"