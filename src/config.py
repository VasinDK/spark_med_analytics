import os
from dotenv import load_dotenv

load_dotenv()

ENV = os.getenv(f"SPARK_USER_PROP_SPARK_ENV")

# S3_STORAGE = os.getenv("S3_STORAGE")
# BRONZE_DEPARTMENTS_CSV = os.getenv("S3_DEPARTMENTS_CSV")
# BRONZE_PROFESSIONS_CSV = os.getenv("S3_PROFESSIONS_CSV")
# SILVER_WAREHOUSE = os.getenv("S3_SILVER_WAREHOUSE")
# QUARANTINE_PATH = os.getenv("S3_QUARANTINE_PATH")

# DB_CATALOG = os.getenv("DB_CATALOG", "yandex")
# DB_SCHEMA = os.getenv("DB_SCHEMA", "silver")

# TABLE_VISITS = f"{DB_CATALOG}.{DB_SCHEMA}.{os.getenv('TABLE_NAME_VISITS')}"
# TABLE_DEPARTMENTS = f"{DB_CATALOG}.{DB_SCHEMA}.{os.getenv('TABLE_NAME_DEPARTMENTS')}"
# TABLE_PROFESSIONS = f"{DB_CATALOG}.{DB_SCHEMA}.{os.getenv('TABLE_NAME_PROFESSIONS')}"
# TABLE_SYMPTOMS = f"{DB_CATALOG}.{DB_SCHEMA}.{os.getenv('TABLE_NAME_SYMPTOMS')}"
# TABLE_CHRONIC = f"{DB_CATALOG}.{DB_SCHEMA}.{os.getenv('TABLE_NAME_CHRONIC')}"

# SESSION_REFERENCES = os.getenv("SESSION_REFERENCES")
# SESSION_SILVER = os.getenv("SESSION_SILVER")
# SESSION_GOLD = os.getenv("SESSION_GOLD")

# DQ_MIN_AGE = int(os.getenv("DQ_MIN_AGE", 0))
# DQ_MAX_AGE = int(os.getenv("DQ_MAX_AGE", 120))
# DQ_MIN_TEMP = float(os.getenv("DQ_MIN_TEMP", 34.0))
# DQ_MAX_TEMP = float(os.getenv("DQ_MAX_TEMP", 43.0))
