from io import StringIO
from typing import TextIO

import pytest
from pydantic import ValidationError

from src.mqttprocessor.loader import load_config


@pytest.mark.parametrize(
    "config_file_stream",
    [
        "config_simple.yaml",
        "config_multiple_processors.yaml",
        "config_multiple_source_topics.yaml",
        "config_multiple_functions.yaml",
        "config_missing_sink.yaml",
        "config_complex_topic_name.yaml"
    ],
    indirect=True
)
def test_load_valid_config(config_file_stream: TextIO):
    load_config(config_file_stream)


@pytest.mark.parametrize(
    "config_file_stream",
    [
        "config_empty.yaml",
        "config_multiple_sink_topics.yaml",
        "config_missing_source.yaml",
        "config_missing_function.yaml",
        "config_malformed_topic_name.yaml"
    ],
    indirect=True
)
def test_load_invalid_config(config_file_stream: TextIO):
    with pytest.raises(ValidationError):
        load_config(config_file_stream)
