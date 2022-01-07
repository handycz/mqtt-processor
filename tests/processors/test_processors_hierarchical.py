from typing import List

import pytest

from src.mqttprocessor.messages import TopicName, Message
from src.mqttprocessor.functions import ProcessorFunction

from tests.processors.common import _create_single_source_processor


@pytest.mark.parametrize(
    "processor_functions_hierarchical",
    [
        ["dummy_routed_dict_hierarchical"]
    ], indirect=True
)
def test_hierarchical_message_dict(processor_functions_hierarchical: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-dict", processor_functions_hierarchical
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )

    expected = [
        Message(
            TopicName("dict/routed/destination/topic"), "base-message<dict-hierarchical1>"
        ),
        Message(
            TopicName("dict/routed/destination/topic"), "base-message<dict-hierarchical2>"
        )
    ]

    assert actual == expected


@pytest.mark.parametrize(
    "processor_functions_hierarchical",
    [
        ["dummy_routed_dict_multiple_hierarchical"]
    ], indirect=True
)
def test_hierarchical_message_dict_multiple_routes(processor_functions_hierarchical: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-dict-multiple", processor_functions_hierarchical
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )
    expected = [
        Message(
            TopicName("multiroute-dict/routed/destination/topic1"), "base-message<hierarchical-dict-multiple1-1>"
        ),
        Message(
            TopicName("multiroute-dict/routed/destination/topic1"), "base-message<hierarchical-dict-multiple1-2>"
        ),
        Message(
            TopicName("multiroute-dict/routed/destination/topic2"), "base-message<hierarchical-dict-multiple2-1>"
        ),
        Message(
            TopicName("multiroute-dict/routed/destination/topic2"), "base-message<hierarchical-dict-multiple2-2>"
        )
    ]

    assert actual == expected
