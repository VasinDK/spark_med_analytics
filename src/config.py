import os
import yaml
from dotenv import load_dotenv

load_dotenv()

spark_env = os.getenv("SPARK_ENV", "dev")

def load_s3_yaml_config(spark, s3_uri) -> dict:
    yaml_text = "\n".join(spark.sparkContext.textFile(s3_uri).collect())
    return yaml.safe_load(yaml_text)
