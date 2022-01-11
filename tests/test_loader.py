from io import StringIO
from typing import TextIO

import pytest
from pydantic import ValidationError

from src.mqttprocessor.loader import load_config


@pytest.mark.parametrize(
    "config_file_stream",
    [
        "config_simple.yaml",
        "config_with_input_format.yaml",
        "config_multiple_processors.yaml",
        "config_multiple_source_topics.yaml",
        "config_multiple_functions.yaml",
        "config_missing_sink.yaml",
        "config_complex_topic_name.yaml",
        "config_expanded_function.yaml",
        "config_simple_with_name.yaml"
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
    with pytest.raises((ValidationError, AttributeError)):
        load_config(config_file_stream)


@pytest.mark.parametrize(
    "config_file_stream,expected_name",
    [
        ("config_simple.yaml", "some_function"),
        ("config_multiple_functions.yaml", "some_function"),
        ("config_simple_with_name.yaml", "my-processor"),
    ],
    indirect=["config_file_stream"]
)
def test_load_function_name(config_file_stream: TextIO, expected_name: str):
    config = load_config(config_file_stream)
    assert config.processors[0].name[:len(expected_name)] == expected_name
