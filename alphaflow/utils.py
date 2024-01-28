import json
from typing import Any

def load_config() -> Any:
    """ Loads configuration from config.json """
    config_path = "./config.json"
    with open(config_path, 'r') as file:
        data = json.load(file)
    return data