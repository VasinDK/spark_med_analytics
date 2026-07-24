import pytest
from unittest.mock import MagicMock, patch, call
from src.core.schema_manager import (
    create_database,
    create_table_ice,
    sync_table_columns,
    sync_single_table,
    get_s3_url_schemas,
    getSparkType,
)
from src.exceptions import ColumnNotNullError, SyncTableError
from src.utils.metrics_validate import StatsTableSync


class TestCreateDatabase:
    def test_create_database(self):
        mock_spark = MagicMock()
        create_database(mock_spark, "iceberg", "silver")
        mock_spark.sql.assert_called_once_with(
            "CREATE DATABASE IF NOT EXISTS iceberg.silver"
        )


class TestCreateTableIce:
    def test_create_table_basic(self):
        mock_spark = MagicMock()
        table_meta = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
            ]
        }
        create_table_ice(mock_spark, "iceberg.silver.departments", table_meta)

        sql_call = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS iceberg.silver.departments" in sql_call
        assert "id string NOT NULL" in sql_call
        assert "name string" in sql_call
        assert "USING iceberg" in sql_call

    def test_create_table_with_partition(self):
        mock_spark = MagicMock()
        table_meta = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "visit_date", "type": "timestamp", "nullable": True},
            ],
            "partition_by": ["days(visit_date)"],
        }
        create_table_ice(mock_spark, "iceberg.silver.visits", table_meta)

        sql_call = mock_spark.sql.call_args[0][0]
        assert "PARTITIONED BY (days(visit_date))" in sql_call

    def test_create_table_with_properties(self):
        mock_spark = MagicMock()
        table_meta = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
            ],
            "tbl_properties": {
                "write.format.default": "parquet",
                "write.metadata.compression-codec": "gzip",
            },
        }
        create_table_ice(mock_spark, "iceberg.silver.test", table_meta)

        sql_call = mock_spark.sql.call_args[0][0]
        assert "TBLPROPERTIES" in sql_call
        assert "'write.format.default'='parquet'" in sql_call
        assert "'write.metadata.compression-codec'='gzip'" in sql_call


class TestSyncTableColumns:
    def test_add_new_column(self):
        mock_spark = MagicMock()
        mock_column = MagicMock()
        mock_column.name = "id"
        mock_column.dataType = "integer"
        mock_spark.catalog.listColumns.return_value = [mock_column]

        stats = StatsTableSync()
        yaml_fields = [
            {"name": "id", "type": "integer", "nullable": True},
            {"name": "name", "type": "string", "nullable": True},
        ]

        sync_table_columns(mock_spark, "iceberg.silver.test", yaml_fields, stats)

        add_calls = [
            call_args for call_args in mock_spark.sql.call_args_list
            if "ADD COLUMN" in call_args[0][0]
        ]
        assert len(add_calls) == 1
        assert "name" in add_calls[0][0][0]
        assert stats.columns_added == 1

    def test_delete_outdated_column(self):
        mock_spark = MagicMock()
        mock_col1 = MagicMock()
        mock_col1.name = "id"
        mock_col1.dataType = "integer"
        mock_col2 = MagicMock()
        mock_col2.name = "old_column"
        mock_col2.dataType = "string"
        mock_spark.catalog.listColumns.return_value = [mock_col1, mock_col2]

        stats = StatsTableSync()
        yaml_fields = [
            {"name": "id", "type": "integer", "nullable": True},
        ]

        sync_table_columns(mock_spark, "iceberg.silver.test", yaml_fields, stats)

        drop_calls = [
            call_args for call_args in mock_spark.sql.call_args_list
            if "DROP COLUMN" in call_args[0][0]
        ]
        assert len(drop_calls) == 1
        assert "old_column" in drop_calls[0][0][0]
        assert stats.columns_deleted == 1

    def test_column_not_null_raises_error(self):
        mock_spark = MagicMock()
        mock_column = MagicMock()
        mock_column.name = "id"
        mock_column.dataType = "integer"
        mock_spark.catalog.listColumns.return_value = [mock_column]

        stats = StatsTableSync()
        yaml_fields = [
            {"name": "id", "type": "integer", "nullable": True},
            {"name": "new_col", "type": "string", "nullable": False},
        ]

        with pytest.raises(ColumnNotNullError):
            sync_table_columns(mock_spark, "iceberg.silver.test", yaml_fields, stats)


class TestSyncSingleTable:
    def test_create_new_table(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False

        mock_registry = MagicMock()
        mock_registry.get_table_address.return_value = "iceberg.silver.visits"
        mock_registry.get_table_metadata.return_value = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
            ]
        }

        stats = StatsTableSync()
        sync_single_table(mock_spark, mock_registry, "silver", "visits", stats)

        assert stats.tables_created == 1
        assert stats.columns_added == 1

    def test_sync_existing_table(self):
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_column = MagicMock()
        mock_column.name = "id"
        mock_column.dataType = "integer"
        mock_spark.catalog.listColumns.return_value = [mock_column]

        mock_registry = MagicMock()
        mock_registry.get_table_address.return_value = "iceberg.silver.visits"
        mock_registry.get_fields.return_value = [
            {"name": "id", "type": "integer", "nullable": True},
        ]

        stats = StatsTableSync()
        sync_single_table(mock_spark, mock_registry, "silver", "visits", stats)

        assert stats.tables_checked == 1
        assert stats.tables_created == 0


class TestGetS3UrlSchemas:
    def test_get_s3_url_schemas(self):
        config = {
            "infrastructure": {
                "code_bucket": "scripts-bucket",
                "schemas": "config/schemas.yaml",
            }
        }
        url = get_s3_url_schemas(config)
        assert url == "s3a://scripts-bucket/config/schemas.yaml"

    def test_get_s3_url_schemas_with_slashes(self):
        config = {
            "infrastructure": {
                "code_bucket": "/scripts-bucket/",
                "schemas": "/config/schemas.yaml",
            }
        }
        url = get_s3_url_schemas(config)
        assert url == "s3a://scripts-bucket/config/schemas.yaml"


class TestGetSparkType:
    def test_get_spark_type_valid(self):
        spark_type = getSparkType("string")
        assert spark_type is not None
        assert spark_type.typeName() == "string"

        spark_type = getSparkType("integer")
        assert spark_type.typeName() == "integer"

        spark_type = getSparkType("float")
        assert spark_type.typeName() == "float"

        spark_type = getSparkType("timestamp")
        assert spark_type.typeName() == "timestamp"

    def test_get_spark_type_invalid(self):
        with pytest.raises(SyncTableError):
            getSparkType("nonexistent_type")
