from io import StringIO
from typing import TextIO

import pytest

from src.mqttprocessor.loader import load_config


@pytest.mark.parametrize(
    "config_file_stream",
    [
        "simple_config.yaml"
    ],
    indirect=["config_file_stream"]
)
def test_load_simple_config(config_file_stream: TextIO):
    load_config(config_file_stream)
