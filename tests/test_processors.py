from typing import Dict, List, Tuple

import pytest

from src.mqttprocessor.messages import TopicName
from src.mqttprocessor.functions import ProcessorFunction

# TODO:
#  - single processor test
#     - for all possible RoutedMessage configurations
#     - for hierarchical-level RoutedMessage configurations
#     - for all possible sink/source variations
#     - static and dynamic routed sinks
#     - static and dynamic constant sinks
#  - multi processor test
#     - topic matching
#     - message processing for normal and single routed message
from src.mqttprocessor.routing import SingleSourceProcessor


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_concat1"]
    ], indirect=True
)
def test_single_processor_matches(processor_functions: List[ProcessorFunction]):
    processor, source, _ = _create_single_source_processor(
        "single-processor_matches", processor_functions
    )

    assert processor.source_topic_matches(
        source
    )


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_rule_true"]
    ], indirect=True
)
def test_single_filter_passing(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "single-processing-function", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert len(msgs) == 1
    assert msgs[0].message_body == "base-message"
    assert msgs[0].sink_topic == sink


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_rule_false"]
    ], indirect=True
)
def test_single_filter_non_passing(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "single-processing-function", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert msgs == []


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_concat1"]
    ], indirect=True
)
def test_single_processing_function(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "single-processing-function", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert len(msgs) == 1
    assert msgs[0].message_body == "base-message<concat1>"
    assert msgs[0].sink_topic == sink


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_concat1", "dummy_str_concat2"]
    ], indirect=True
)
def test_multiple_processing_functions(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "multiple-processing-functions", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert len(msgs) == 1
    assert msgs[0].message_body == "base-message<concat1><concat2>"
    assert msgs[0].sink_topic == sink


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_concat1", "dummy_routed_dict"]
    ], indirect=True
)
def test_routed_function_after_plain_processing_function(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-after-plain-processing-function", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert len(msgs) == 1
    assert msgs[0].message_body == "base-message<concat1><dict-routed>"
    assert msgs[0].sink_topic == TopicName("dict/routed/destination/topic")


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_routed_dict", "dummy_str_concat1"]
    ], indirect=True
)
def test_plain_function_after_routed_function_processing_function(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-after-plain-normal-processing-function", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert msgs == []


@pytest.mark.parametrize(
    "processor_functions",
    [
        [("dummy_str_concat_with_params", 5, 10)]
    ], indirect=True
)
def test_parametrized_processing_function(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "parametrized-processing-function", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert len(msgs) == 1
    assert msgs[0].message_body == "base-message<concat-a+b=5+10=15>"


@pytest.mark.parametrize(
    "processor_functions",
    [
        [("dummy_str_concat_with_params", 5, 10)]
    ], indirect=True
)
def test_parametrized_processing_function(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "parametrized-processing-function", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert len(msgs) == 1
    assert msgs[0].message_body == "base-message<concat-a+b=5+10=15>"


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_routed_dict"]
    ], indirect=True
)
def test_routed_message_dict(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-dict", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert len(msgs) == 1
    assert msgs[0].message_body == "base-message<dict-routed>"
    assert msgs[0].sink_topic == TopicName("dict/routed/destination/topic")


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_routed_dict_multiple"]
    ], indirect=True
)
def test_routed_message_dict_multiple_routes(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "routed-message-dict-multiple", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "base-message"
    )

    assert len(msgs) == 3
    print(msgs)
    for msg_number in range(1, 4):
        # TODO: nemusi byt serazene! asi by bylo dobry to nejak radit interne!
        assert msgs[msg_number].message_body == "base-message<multiroute-dict{}>".format(msg_number)
        assert msgs[msg_number].sink_topic == "multiroute-dict/routed/destination/topic{}".format(msg_number)


def _create_single_source_processor(
        name: str, functions: List[ProcessorFunction],
        source_topic: str = None, sink_topic: str = None
) -> Tuple[SingleSourceProcessor, TopicName, TopicName]:
    if source_topic is None:
        source_topic = name + "/source"

    if sink_topic is None:
        sink_topic = name + "/sink"

    source_topic = TopicName(source_topic)
    sink_topic = TopicName(sink_topic)

    processor = SingleSourceProcessor(
        name, functions,
        source_topic, sink_topic
    )

    return processor, source_topic, sink_topic
