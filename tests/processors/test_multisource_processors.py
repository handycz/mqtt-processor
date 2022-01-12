from typing import List

import pytest

from mqttprocessor.messages import TopicName, Message
from mqttprocessor.functions import ProcessorFunction
from mqttprocessor.routing import Processor


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_concat1"]
    ], indirect=True
)
def test_processor_matches(processor_functions: List[ProcessorFunction]):
    proc = Processor(
        "multi-processor", processor_functions,
        [
            TopicName("source/room1/dev1"),
            TopicName("source/room1/dev2"),
            TopicName("source/{w1}/sensor1")
        ],
        TopicName("default/sink")
    )

    assert proc.process_message("source/room1/dev1", "") != []
    assert proc.process_message("source/room1/dev2", "") != []


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_concat1"]
    ], indirect=True
)
def test_processor_not_matches(processor_functions: List[ProcessorFunction]):
    proc = Processor(
        "multi-processor", processor_functions,
        [
            TopicName("source/room1/dev1"),
            TopicName("source/room1/dev2")
        ],
        TopicName("default/sink")
    )

    assert proc.process_message("source/room1/dev3", "") == []
    assert proc.process_message("something/dev3", "") == []
    assert proc.process_message("source/room1/dev2/", "") == []


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_concat1"]
    ], indirect=True
)
def test_processor_matching_dynamic_sink(processor_functions: List[ProcessorFunction]):
    proc = Processor(
        "multi-processor", processor_functions,
        [
            TopicName("source/{w1}/dev1"),
            TopicName("reports/room1/dev2")
        ],
        TopicName("default/sink/{w1}")
    )

    expected = [
        Message(
            TopicName("default/sink/room100"), "<concat1>"
        )
    ]

    assert proc.process_message("source/room100/dev1", "") == expected
