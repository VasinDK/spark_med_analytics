QUALITY_METRICS = "Quality Metrics"
MAX_ERROR_PERCENT = 5.0

ERR_INVALID_AGE = "INVALID_AGE"
ERR_INVALID_TEMP = "INVALID_TEMPERATURE"
ERR_CORRUPT_JSON = "Malformed JSON record could not be parsed"


CRITICAL_ERROR = "[CRITICAL ERROR] Unexpected failure: {}"
CRITICAL_ERROR_PERCENT = "[CRITICAL ERROR] Error percent exceeds allowed limit"
CRITICAL_ERROR_PERCENT_DETAILS = "[CRITICAL ERROR] Error percent {:.2f}% exceeds allowed limit of {}%! Processing stopped."


EMPTY_INPUT_WARNING = "[SPARK WARN] Input DataFrame is empty. Processing stopped."
SPARK_ANALYSIS_ERROR = "[SPARK ERROR] Data or SQL failure: {}"
TYPE_COULD_NOT_BE_RECOGNIZED = "[SPARK ERROR] The type '{}' could not be recognized"
SPARK_STOP_ERROR = "[SPARK ERROR] spark stop error: {}"


REPORT_SCHEMA_SYNCHRONIZATION = "[ICEBERG INFO] FINAL SCHEMA SYNCHRONIZATION REPORT"
CREATING_NEW_TABLE = "[ICEBERG INFO] Creating a new table: {}"
ADDING_NEW_COLUMN = "[ICEBERG INFO] Adding a new column '{}' ({}) to {}"
DELETING_OUTDATED_COLUMN = "[ICEBERG INFO] Deleting the outdated column '{}' from {}"
CHANGING_COLUMN_TYPE = "[ICEBERG INFO] Changing the column type '{}' to {}: {} -> {}"
TABLE_NOT_FOUND_ERROR = "[ICEBERG ERROR] The table '{}' is described in YAML, but its address is not found in the layer configuration."
COLUMN_NOT_NULL = "[ICEBERG ERROR] The column being added to the iceberg cannot be 'not null'"
SYNC_TABLE_ERROR = "[ICEBERG ERROR] Table synchronization error"
MERGE_KEYS_ERROR = "[ICEBERG ERROR] In the schemas configuration.yaml does not set 'merge_keys' for {}.{}"


QUALITY_METRICS_LOG = "[DATA INFO] === Quality Metrics: {} ==="
INCREMENT_POINT = "[DATA INFO] The increment point (Watermark): {}"
NO_NEW_CHANGED_DATA_GOLD = "[DATA INFO] There is no new or changed data to update the Gold layer"
BROKEN_JSON_STRINGS = "[DATA WARN] Attention! {corrupt_count} broken JSON strings found"
QUARANTINE_WRITE_ERROR = "[DATA ERROR] Failed to route invalid data to quarantine. {}"


INVALID_S3_PATH_ERROR = "[STORAGE ERROR] The provided S3 path is invalid or inaccessible: {}"
S3_ERROR = "[STORAGE ERROR] s3 error: {}"


CONFIGURATION_NOT_FOUND_ERROR = (
    "[CONFIG ERROR] Missing required positional argument: S3 URI to the configuration YAML file. "
    "Usage: spark-submit <script.py> s3a://<bucket>/<path_to_config>.yaml"
)
CONFIGURATION_ERROR = "[CONFIG ERROR] Configuration error"
ERROR_READING_YAML_FILE = "[CONFIG ERROR] Error reading YAML file or file not found: {}"
LAYER_IS_NOT_DESCRIBED = "[CONFIG ERROR] The '{}' layer is not described in the schemas.yaml configuration"
TABLE_NOT_FOUND = "[CONFIG ERROR] The table '{}' was not found in the layer '{}'"