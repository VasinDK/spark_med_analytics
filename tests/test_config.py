import json
import sys
import pytest
from unittest.mock import MagicMock, patch
from src.config import get_config
from src.exceptions import ConfigurationNotFoundError


@pytest.fixture(autouse=True)
def clear_config_cache():
    """Очищаем кеш get_config перед каждым тестом"""
    get_config.cache_clear()
    yield


class TestGetConfig:
    def test_get_config_success(self):
        test_config = {"key": "value", "number": 42}
        test_args = ["script.py", "--config_json", json.dumps(test_config)]

        with patch.object(sys, "argv", test_args):
            result = get_config()

        assert result == test_config

    def test_get_config_invalid_json(self):
        test_args = ["script.py", "--config_json", "not valid json"]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(ConfigurationNotFoundError):
                get_config()

    def test_get_config_cached(self):
        """Проверяем, что результат кешируется (lru_cache)"""
        test_config = {"cached": True}
        test_args = ["script.py", "--config_json", json.dumps(test_config)]

        with patch.object(sys, "argv", test_args):
            result1 = get_config()
            result2 = get_config()

        assert result1 is result2  # один и тот же объект из кеша
        assert result1 == test_config
