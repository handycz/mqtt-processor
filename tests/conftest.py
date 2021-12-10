from importlib import reload
from pathlib import Path
from typing import TextIO

import pytest

import src.mqttprocessor.processors


@pytest.fixture(scope="function")
def rule() -> type(src.mqttprocessor.processors.rule):
    reload(src.mqttprocessor.processors)

    return src.mqttprocessor.processors.rule


@pytest.fixture(scope="function")
def converter() -> type(src.mqttprocessor.processors.converter):
    reload(src.mqttprocessor.processors)

    return src.mqttprocessor.processors.converter


@pytest.fixture(scope="function")
def config_file_stream(filename: str) -> TextIO:
    path = Path("testcase_files/config/" + filename)
    with open(path, "r") as f:
        yield f
