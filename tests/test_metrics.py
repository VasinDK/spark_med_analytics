from src.utils.metrics_validate import MetricsValidate, StatsTableSync


class TestMetricsValidate:
    def test_default_values(self):
        metrics = MetricsValidate()
        assert metrics.total_rows == 0
        assert metrics.valid_rows == 0
        assert metrics.invalid_rows == 0
        assert metrics.error_percent == 0.0

    def test_custom_values(self):
        metrics = MetricsValidate(
            total_rows=100,
            valid_rows=90,
            invalid_rows=10,
            error_percent=10.0,
        )
        assert metrics.total_rows == 100
        assert metrics.valid_rows == 90
        assert metrics.invalid_rows == 10
        assert metrics.error_percent == 10.0

    def test_to_dict(self):
        metrics = MetricsValidate(
            total_rows=100,
            valid_rows=90,
            invalid_rows=10,
            error_percent=10.0,
        )
        d = metrics.to_dict()
        assert d == {
            "total_rows": 100,
            "valid_rows": 90,
            "invalid_rows": 10,
            "error_percent": 10.0,
        }

    def test_str_representation(self):
        metrics = MetricsValidate(total_rows=50, valid_rows=40, invalid_rows=10, error_percent=20.0)
        s = str(metrics)
        assert "total_rows=50" in s
        assert "valid_rows=40" in s
        assert "invalid_rows=10" in s
        assert "error_percent=20.0" in s


class TestStatsTableSync:
    def test_default_values(self):
        stats = StatsTableSync()
        assert stats.tables_created == 0
        assert stats.columns_added == 0
        assert stats.columns_deleted == 0
        assert stats.tables_checked == 0

    def test_custom_values(self):
        stats = StatsTableSync(
            tables_created=2,
            columns_added=10,
            columns_deleted=1,
            tables_checked=5,
        )
        assert stats.tables_created == 2
        assert stats.columns_added == 10
        assert stats.columns_deleted == 1
        assert stats.tables_checked == 5

    def test_to_dict(self):
        stats = StatsTableSync(
            tables_created=1,
            columns_added=5,
            columns_deleted=0,
            tables_checked=3,
        )
        d = stats.to_dict()
        assert d == {
            "tables_created": 1,
            "columns_added": 5,
            "columns_deleted": 0,
            "tables_checked": 3,
        }

    def test_increment_operations(self):
        stats = StatsTableSync()
        stats.tables_created += 1
        stats.columns_added += 3
        stats.columns_deleted += 1
        stats.tables_checked += 2

        assert stats.tables_created == 1
        assert stats.columns_added == 3
        assert stats.columns_deleted == 1
        assert stats.tables_checked == 2
