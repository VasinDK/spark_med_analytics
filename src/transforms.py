from pyspark.sql import DataFrame
from pyspark.sql.functions import round as _round, col, coalesce, lit, to_timestamp, md5, concat_ws, when
from src.core.data_catalog_registry import DataCatalogRegistry

def cast_bronze(registry: DataCatalogRegistry):
    def _inner(df: DataFrame) -> DataFrame:
        select_exprs = []
        for field in registry.get_fields("bronze", "visits_raw"):
            name = field['name']
            target_type = field['type']
            
            if "list" in target_type or "array" in target_type:
                select_exprs.append(f"cast({name} as array<string>) as {name}")
            elif name == "_corrupt_record":
                select_exprs.append(f"cast({name} as string) as {name}")
            else:
                select_exprs.append(f"cast({name} as {target_type}) as {name}")

        return df.selectExpr(*select_exprs)
    return _inner

def cast_visit_date(df: DataFrame) -> DataFrame:
    return df.withColumn("visit_date", coalesce(
                to_timestamp(col("visit_date"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("visit_date"), "yyyy-MM-dd"),
                to_timestamp(col("visit_date"), "dd.MM.yyyy HH:mm:ss"),
                to_timestamp(col("visit_date"), "dd.MM.yyyy")
            ))

def add_id(df: DataFrame) -> DataFrame:
    return df.withColumn("id", md5(concat_ws("-", 
                coalesce(col("visit_date"), lit("EMPTY")), 
                coalesce(col("snils"), lit("EMPTY")), 
                coalesce(col("disease_code"), lit("EMPTY"))
            )))


def add_bmi(df: DataFrame) -> DataFrame:
    return df.withColumn("bmi", 
        when (
            (col("height") > 0) & (col("weight") > 0),
            _round(col("weight") / ((col("height") / 100) **2) ,1)
        )
    )
