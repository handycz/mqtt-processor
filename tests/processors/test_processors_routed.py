from typing import List

import pytest

from src.mqttprocessor.messages import TopicName, Message
from src.mqttprocessor.functions import ProcessorFunction

from tests.processors.common import _create_single_source_processor


@pytest.mark.parametrize(
    "processor_functions_routed",
    [
        ["dummy_routed_dict"]
    ], indirect=True
)
def test_routed_message_dict(processor_functions_routed: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-dict", processor_functions_routed
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )

    assert len(actual) == 1
    assert actual[0].message_body == "base-message<dict-routed>"
    assert actual[0].sink_topic == TopicName("dict/routed/destination/topic")


@pytest.mark.parametrize(
    "processor_functions_routed",
    [
        ["dummy_routed_dict_multiple"]
    ], indirect=True
)
def test_routed_message_dict_multiple_routes(processor_functions_routed: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-dict-multiple", processor_functions_routed
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )
    expected = [
        Message(
            TopicName("multiroute-dict/routed/destination/topic1"), "base-message<multiroute-dict1>"
        ),
        Message(
            TopicName("multiroute-dict/routed/destination/topic2"), "base-message<multiroute-dict2>"
        ),
        Message(
            TopicName("multiroute-dict/routed/destination/topic3"), "base-message<multiroute-dict3>"
        ),
    ]

    assert actual == expected


@pytest.mark.parametrize(
    "processor_functions_routed",
    [
        ["dummy_routed_list"]
    ], indirect=True
)
def test_routed_message_list(processor_functions_routed: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-list", processor_functions_routed
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )
    expected = [
        Message(
            TopicName("routed-message-list/sink"), "base-message<routed_list-msg1>"
        ),
        Message(
            TopicName("routed-message-list/sink"), "base-message<routed_list-msg2>"
        ),
        Message(
            TopicName("routed-message-list/sink"), "base-message<routed_list-msg3>"
        ),
    ]

    assert actual == expected


@pytest.mark.parametrize(
    "processor_functions_routed",
    [
        ["dummy_routed_tuple_containing_multiple"]
    ], indirect=True
)
def test_routed_message_tuple_containing_list(processor_functions_routed: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-list", processor_functions_routed
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )
    expected = [
        Message(
            TopicName("tuple-of-lists/routed/destination/topic"), "base-message<routed_tuple_of_lists-msg1>"
        ),
        Message(
            TopicName("tuple-of-lists/routed/destination/topic"), "base-message<routed_tuple_of_lists-msg2>"
        ),
        Message(
            TopicName("tuple-of-lists/routed/destination/topic"), "base-message<routed_tuple_of_lists-msg3>"
        ),
    ]

    assert actual == expected


@pytest.mark.parametrize(
    "processor_functions_routed",
    [
        ["dummy_routed_tuple_containing_single"]
    ], indirect=True
)
def test_routed_message_tuple_containing_single(processor_functions_routed: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-list", processor_functions_routed
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )
    expected = [
        Message(
            TopicName("tuple/routed/destination/topic"), "base-message<routed-tuple>"
        ),
    ]

    assert actual == expected
