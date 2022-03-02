from typing import List

import pytest

from mqttprocessor.messages import TopicName, Message
from mqttprocessor.functions import ProcessorFunction

from tests.processors.common import _create_single_source_processor


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

    actual = processor.process_message(
        source.rule, "base-message"
    )

    expected = [
        Message(
            sink, "base-message"
        )
    ]

    assert actual == expected


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

    actual = processor.process_message(
        source.rule, "base-message"
    )

    assert actual == []


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

    actual = processor.process_message(
        source.rule, "base-message"
    )

    expected = [
        Message(sink, "base-message<concat1>")
    ]

    assert actual == expected


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

    actual = processor.process_message(
        source.rule, "base-message"
    )

    expected = [
        Message(
            sink, "base-message<concat1><concat2>"
        )
    ]

    assert actual == expected


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

    actual = processor.process_message(
        source.rule, "base-message"
    )

    expected = [
        Message(
            TopicName("dict/routed/destination/topic"), "base-message<concat1><dict-routed>"
        )
    ]

    assert actual == expected


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

    actual = processor.process_message(
        source.rule, "base-message"
    )

    assert actual == []


@pytest.mark.parametrize(
    "processor_functions",
    [
        [("dummy_str_concat_with_params", {"a": 5, "b": 10})]
    ], indirect=True
)
def test_parametrized_processing_function(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "parametrized-processing-function", processor_functions
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )

    expected = [
        Message(
            sink, "base-message<concat-a+b=5+10=15>"
        )
    ]

    assert actual == expected


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_failing"]
    ], indirect=True
)
def test_failing_function(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "single-processing-function", processor_functions
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )

    assert actual == []


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_rule_failing"]
    ], indirect=True
)
def test_failing_rule(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "single-processing-function", processor_functions
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )

    assert actual == []


@pytest.mark.parametrize(
    "processor_functions",
    [
        ["dummy_str_concat1"]
    ], indirect=True
)
def test_normal_message_without_default_sink(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "no-sink-specified", processor_functions,
        sink_topic=None, allow_sink_none=True
    )

    actual = processor.process_message(
        source.rule, "base-message"
    )

    expected = [
        Message(
            None, "base-message<concat1>"
        )
    ]

    assert actual == expected
