import json
import logging
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


@dataclass
class MetricsValidate:
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    error_percent: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    def action(self, spark: SparkSession, config: dict):
        self.write_metrics_s3(spark, config)

    def write_metrics_s3(self, spark: SparkSession, config: dict) -> None:
        try:
            silver_bucket = config["cfg"]["s3"]["silver"]
            metrics_path = config["cfg"]["infrastructure"]["metrics_path"]
            ds = config["ds"]
            metrics_s3_dir = f"s3a://{silver_bucket}/{metrics_path}/{ds}/"

            metrics_json_str = json.dumps(self.to_dict())

            spark.read.json(
                spark.sparkContext.parallelize([metrics_json_str])
            ).coalesce(1).write.mode("overwrite").json(metrics_s3_dir)

            logger.info(f"Metrics successfully sent to S3: {metrics_s3_dir}")
        except Exception as e:
            logger.error(f"Failed to send metrics via action(): {e}")
