import pytest
from unittest.mock import MagicMock, patch
from src.core.writer import merge_table_from_view, upsert_array_relation, add_quarantine
from src.exceptions import MergeTableError, QuarantineWriteError


class TestMergeTableFromView:
    def test_merge_table_success(self):
        mock_spark = MagicMock()
        mock_registry = MagicMock()
        mock_registry.get_table_address.return_value = "iceberg.silver.visits"
        mock_registry.get_fields.return_value = [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "visit_date", "type": "timestamp", "nullable": True},
            {"name": "age", "type": "integer", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": True},
            {"name": "updated_at", "type": "timestamp", "nullable": True},
        ]
        mock_registry.get_merge_keys.return_value = [
            "visit_date",
            "snils",
            "disease_code",
        ]

        merge_table_from_view(
            mock_spark, mock_registry, "silver", "visits", "temp_view"
        )

        sql_call = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO iceberg.silver.visits t" in sql_call
        assert "USING temp_view td" in sql_call
        assert "WHEN MATCHED THEN UPDATE" in sql_call
        assert "WHEN NOT MATCHED THEN" in sql_call

    def test_merge_table_no_merge_keys(self):
        mock_spark = MagicMock()
        mock_registry = MagicMock()
        mock_registry.get_table_address.return_value = "iceberg.silver.visits"
        mock_registry.get_fields.return_value = []
        mock_registry.get_merge_keys.return_value = []

        with pytest.raises(MergeTableError):
            merge_table_from_view(
                mock_spark, mock_registry, "silver", "visits", "temp_view"
            )

    def test_merge_table_update_columns(self):
        mock_spark = MagicMock()
        mock_registry = MagicMock()
        mock_registry.get_table_address.return_value = "iceberg.silver.visits"
        mock_registry.get_fields.return_value = [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "visit_date", "type": "timestamp", "nullable": True},
            {"name": "age", "type": "integer", "nullable": True},
            {"name": "created_at", "type": "timestamp", "nullable": True},
            {"name": "updated_at", "type": "timestamp", "nullable": True},
        ]
        mock_registry.get_merge_keys.return_value = [
            "visit_date",
            "snils",
            "disease_code",
        ]

        merge_table_from_view(
            mock_spark, mock_registry, "silver", "visits", "temp_view"
        )

        sql_call = mock_spark.sql.call_args[0][0]
        update_section = sql_call.split("WHEN MATCHED THEN UPDATE")[1].split(
            "WHEN NOT MATCHED"
        )[0]
        assert "t.created_at" not in update_section
        assert "t.updated_at = current_timestamp()" in sql_call


class TestUpsertArrayRelation:
    def test_upsert_with_data(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            ("2024-01-01", "2024-01-31")
        ]

        target = {
            "table_address": "iceberg.silver.visits_symptoms",
            "raw_col": "symptoms_code",
            "target_col": "symptoms_code",
            "all_columns": ["visit_id", "symptoms_code", "visit_date"],
        }

        upsert_array_relation(mock_spark, target, "temp_view")

        assert mock_spark.sql.call_count == 3

    def test_upsert_without_dates(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [(None, None)]

        target = {
            "table_address": "iceberg.silver.visits_symptoms",
            "raw_col": "symptoms_code",
            "target_col": "symptoms_code",
            "all_columns": ["visit_id", "symptoms_code", "visit_date"],
        }

        upsert_array_relation(mock_spark, target, "temp_view")

        assert mock_spark.sql.call_count == 2

    def test_upsert_uses_current_timestamp_for_tech_columns(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            ("2024-01-01", "2024-01-31")
        ]

        # all_columns из schemas.yaml для visits_symptoms включает служебные колонки,
        # которых нет в source-view (df_silver) — см. bronze_to_silver.py.
        target = {
            "table_address": "iceberg.silver.visits_symptoms",
            "raw_col": "symptoms_code",
            "target_col": "symptoms_code",
            "all_columns": [
                "visit_id",
                "symptoms_code",
                "visit_date",
                "created_at",
                "updated_at",
            ],
        }

        upsert_array_relation(mock_spark, target, "temp_view")

        # Вызовы spark.sql: 1) min/max дат, 2) DELETE-мердж, 3) INSERT-запрос
        assert mock_spark.sql.call_count == 3
        insert_sql = mock_spark.sql.call_args_list[-1][0][0]

        assert "SELECT id AS visit_id" in insert_sql
        assert "explode(symptoms_code) AS symptoms_code" in insert_sql
        # В source-view нет created_at/updated_at -> должны генерироваться
        assert "current_timestamp()" in insert_sql
        assert "created_at" in insert_sql
        assert "updated_at" in insert_sql


class TestAddQuarantine:
    def test_add_quarantine_success(self):
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.save.return_value = None

        add_quarantine(mock_df, "s3a://bucket/quarantine/")

        mock_writer.mode.assert_called_once_with("append")
        mock_writer.save.assert_called_once_with("s3a://bucket/quarantine/")

    def test_add_quarantine_failure(self):
        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.save.side_effect = RuntimeError("S3 write failed")

        with pytest.raises(QuarantineWriteError):
            add_quarantine(mock_df, "s3a://bucket/quarantine/")
