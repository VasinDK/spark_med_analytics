from src.logging_config import setup_logging
from src.decorators import monitor_job
import sys
from src.utils import get_spark_session, build_s3_path

@monitor_job
def run_etl_gold():
    spark, config = get_spark_session(sys.argv)

    try:
        db_schema = f"{config['db']['catalog']}.{config['db']['schema']}"
        tables = {
            "visits": f"{db_schema}.{config['db']['table']['visits']}",
            "visits_symptoms": f"{db_schema}.{config['db']['table']['symptoms']}",
            "visits_chronic_dis": f"{db_schema}.{config['db']['table']['chronic']}",
            "departments": f"{db_schema}.{config['db']['table']['visidepartmentsts']}",
            "professions": f"{db_schema}.{config['db']['table']['professions']}",
        }

        df_visits = spark.read.table(tables["visits"])
        df_symptoms = spark.read.table(tables["visits_symptoms"])
        df_chronic = spark.read.table(tables["visits_chronic_dis"])
        df_departments = spark.read.table(tables["departments"])
        df_professions = spark.read.table(tables["professions"])

        df_total = (df_visits
            .join(df_symptoms, on=["id", "visit_date"], how="left")
            .join(df_chronic, on=["id", "visit_date"], how="left")
            .join(df_departments, on=["id"], how="left")
            .join(df_professions, on=["id"], how="left")
        )

    finally:
        spark.stop()

# DOTO
# - Получить данные из silver. Собераем в плоскую таблицу
# - Кладем в iceberg и clickhous

if __name__ == "__main__":
    setup_logging()
    run_etl_gold()