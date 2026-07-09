import logging
from src.logging_config import setup_logging


class TestLoggingConfig:
    def test_setup_logging(self):
        setup_logging()
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO
        assert len(root_logger.handlers) > 0

    def test_logger_has_console_handler(self):
        setup_logging()
        root_logger = logging.getLogger()
        handler_types = [type(h).__name__ for h in root_logger.handlers]
        assert "StreamHandler" in handler_types

    def test_logger_format(self):
        setup_logging()
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if hasattr(handler, "formatter") and handler.formatter:
                fmt = handler.formatter._fmt
                assert "%(asctime)s" in fmt
                assert "%(levelname)s" in fmt
                assert "%(filename)s" in fmt
                assert "%(message)s" in fmt
