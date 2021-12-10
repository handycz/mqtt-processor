from importlib import reload

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
