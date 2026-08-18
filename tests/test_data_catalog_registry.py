import pytest
from unittest.mock import MagicMock, patch
from src.core.data_catalog_registry import DataCatalogRegistry
from src.exceptions import ConfigurationError


class TestDataCatalogRegistry:
    def test_init(self, registry):
        assert registry is not None

    def test_get_catalog_schema(self, registry):
        catalog, schema = registry.get_catalog_schema("bronze")
        assert catalog == "iceberg"
        assert schema == "bronze"

        catalog, schema = registry.get_catalog_schema("silver")
        assert catalog == "iceberg"
        assert schema == "silver"

    def test_get_catalog_schema_invalid_layer(self, registry):
        with pytest.raises(ConfigurationError):
            registry.get_catalog_schema("nonexistent")

    def test_get_table_address(self, registry):
        address = registry.get_table_address("bronze", "visits_raw")
        assert address == "iceberg.bronze.visits_raw"

        address = registry.get_table_address("silver", "visits")
        assert address == "iceberg.silver.visits"

    def test_get_table_address_invalid_table(self, registry):
        with pytest.raises(ConfigurationError):
            registry.get_table_address("bronze", "nonexistent")

    def test_get_table_address_invalid_layer(self, registry):
        with pytest.raises(ConfigurationError):
            registry.get_table_address("nonexistent", "visits_raw")

    def test_get_table_metadata(self, registry):
        meta = registry.get_table_metadata("silver", "visits")
        assert "fields" in meta
        assert "merge_keys" in meta
        assert meta["merge_keys"] == ["visit_date", "snils", "disease_code"]

    def test_get_fields(self, registry):
        fields = registry.get_fields("bronze", "visits_raw")
        assert len(fields) > 0
        assert fields[0]["name"] == "id"
        assert fields[0]["type"] == "bigint"

    def test_get_active_tables(self, registry):
        active_tables = registry.get_active_tables("silver")
        assert "visits" in active_tables
        assert "departments" in active_tables
        assert "professions" in active_tables

    def test_get_active_tables_invalid_layer(self, registry):
        with pytest.raises(ConfigurationError):
            registry.get_active_tables("nonexistent")

    def test_get_merge_keys(self, registry):
        keys = registry.get_merge_keys("silver", "visits")
        assert keys == ["visit_date", "snils", "disease_code"]

        keys = registry.get_merge_keys("silver", "departments")
        assert keys == ["id"]

    def test_get_merge_keys_empty(self, registry):
        keys = registry.get_merge_keys("bronze", "visits_raw")
        assert keys == []

    def test_get_spark_schema(self, spark_session, registry):
        schema = registry.get_spark_schema(spark_session, "silver", "departments")
        assert schema is not None
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "name" in field_names

    def test_from_s3_yaml_file(self):
        schemas = {
            "databases": {
                "test": {
                    "catalog": "test_catalog",
                    "schema": "test_schema",
                    "tables": {},
                }
            }
        }

        registry = DataCatalogRegistry.from_dict(schemas)
        assert registry is not None
        catalog, schema = registry.get_catalog_schema("test")
        assert catalog == "test_catalog"
        assert schema == "test_schema"
