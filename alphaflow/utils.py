import json
from typing import Any


def load_config() -> Any:
    """ Loads configuration from config.json """
    config_path = "config.json"
    with open(config_path, 'r') as file:
        data = json.load(file)
    return data


def printc(text: str, color: str) -> None:
    """ Prints the specified text in the specified color """
    colors = {
        'red': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'purple': '\033[95m',
        'cyan': '\033[96m',
        'white': '\033[97m'
    }
    end_color = '\033[0m'
    if color in colors:
        print(f"{colors[color]}{text}{end_color}")
    else:
        print(text)
