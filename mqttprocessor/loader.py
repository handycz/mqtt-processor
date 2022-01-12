from typing import TextIO

import yaml

from mqttprocessor.models import ConfigModel


def load_config(stream: TextIO) -> ConfigModel:
    data = yaml.load(stream, yaml.CLoader)
    return ConfigModel(**data)
