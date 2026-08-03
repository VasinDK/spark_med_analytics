import pytest
import logging
from unittest.mock import patch
from src.decorators import monitor_job


class TestMonitorJob:
    def test_monitor_job_logs_start_and_end(self, caplog):
        caplog.set_level(logging.INFO)

        @monitor_job
        def dummy_function():
            return 42

        result = dummy_function()

        assert result == 42
        assert "=== START FUNCTION: dummy_function ===" in caplog.text
        assert "=== END FUNCTION: dummy_function ===" in caplog.text
        assert "=== EXECUTION TIME dummy_function:" in caplog.text

    def test_monitor_job_with_args(self, caplog):
        caplog.set_level(logging.INFO)

        @monitor_job
        def add(a, b):
            return a + b

        result = add(3, 5)
        assert result == 8
        assert "=== START FUNCTION: add ===" in caplog.text

    def test_monitor_job_preserves_function_name(self):
        @monitor_job
        def my_test_func():
            pass

        assert my_test_func.__name__ == "my_test_func"

    def test_monitor_job_logs_execution_time(self, caplog):
        caplog.set_level(logging.INFO)

        @monitor_job
        def slow_function():
            import time

            time.sleep(0.01)
            return "done"

        result = slow_function()
        assert result == "done"
        assert "EXECUTION TIME" in caplog.text
