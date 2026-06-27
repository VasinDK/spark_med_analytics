import os
import yaml
from typing import Dict, Any, List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from src import constants
from src.exceptions import ConfigurationError

class DataCatalogRegistry:
    def __init__(self, yaml_content: dict):
        self._config = yaml_content
        self._env = os.getenv("SPARK_ENV", "dev").lower()
        self._databases = self._config.get("databases", {})

    @classmethod
    def from_s3_yaml_file(cls, spark: SparkSession, s3_uri: str) -> "DataCatalogRegistry":
        yaml_text = "\n".join(spark.sparkContext.textFile(s3_uri).collect())
        return cls(yaml.safe_load(yaml_text))

    def _get_layer_meta(self, layer: str) -> Dict[str, Any]:
        if layer not in self._databases:
            raise ConfigurationError(constants.LAYER_IS_NOT_DESCRIBED.format(layer))
        return self._databases[layer]
    
    def get_catalog_schema(self, layer: str) -> Tuple[str, str]:
        layer_meta = self._get_layer_meta(layer)
        return layer_meta['catalog'], layer_meta['schema']

    def get_table_address(self, layer: str, table_key: str) -> str:
        layer_meta = self._get_layer_meta(layer)
        tables = layer_meta.get("tables", {})
        
        if table_key not in tables:
            raise ConfigurationError(constants.TABLE_NOT_FOUND.format(table_key, layer))
        
        catalog = layer_meta["catalog"]
        schema = layer_meta["schema"]
        
        return f"{catalog}.{schema}.{table_key}"

    def get_table_metadata(self, layer: str, table_key: str) -> Dict[str, Any]:
        layer_meta = self._get_layer_meta(layer)
        tables = layer_meta.get("tables", {})
        
        if table_key not in tables:
            raise ConfigurationError(constants.TABLE_NOT_FOUND.format(table_key, layer))
            
        return tables[table_key]

    def get_fields(self, layer: str, table_key: str) -> List[Dict[str, Any]]:
        meta = self.get_table_metadata(layer, table_key)
        return meta.get("fields", [])

    def get_active_tables(self, layer: str) -> List[str]:
        layer_meta = self._get_layer_meta(layer)
        tables = layer_meta.get("tables", {})
        return [k for k, v in tables.items() if v.get("status") == "active"]

    def get_spark_schema(self, spark, layer: str, table_key: str) -> StructType:
        yaml_fields = self.get_fields(layer, table_key)
    
        struct_fields = []
        for field in yaml_fields:
            col_name = field['name'].lower()
            norm_type = field['type'].lower()

            spark_type_object = spark.sessionState.sqlParser().parseDataType(norm_type)
            is_nullable = field.get('nullable', True)
            struct_fields.append(StructField(col_name, spark_type_object, is_nullable))
        
        return StructType(struct_fields)
    
    def get_merge_keys(self, layer: str, table_key: str) -> list:
        meta = self.get_table_metadata(layer, table_key)
        return meta.get("merge_keys", [])