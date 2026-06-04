import os
import yaml
from dotenv import load_dotenv

load_dotenv()

spark_env = os.getenv("SPARK_ENV", "dev")
config_path = f"config/{spark_env}_config.yaml"

def load_s3_yaml(spark, s3_uri):
    yaml_text = "\n".join(spark.sparkContext.textFile(s3_uri).collect())
    return yaml.safe_load(yaml_text)
