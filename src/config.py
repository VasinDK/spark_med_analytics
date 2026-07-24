import argparse
import json
from functools import lru_cache
from src.exceptions import ConfigurationNotFoundError

@lru_cache(maxsize=1)
def get_config():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config_json", required=True)
        args = parser.parse_args()
   
        return json.loads(args.config_json)
    except json.JSONDecodeError as e:
        raise ConfigurationNotFoundError()