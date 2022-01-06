from typing import Dict, List, Tuple

import pytest

from src.mqttprocessor.messages import TopicName
from src.mqttprocessor.functions import ProcessorFunctionDefinition, ProcessorFunction, create_functions

# TODO:
#  - single processor test
#     - single processing function
#     - several processing functions
#     - chaining rich after normal function
#     - chaining normal after rich function
#     - function with params
#     - for all possible RichMessage configurations
#     - for all possible sink/source variations
#     - static and dynamic rich sinks
#     - static and dynamic constant sinks
#  - multi processor test
#     - just test the topic matching
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
        ["dummy_str_concat1"]
    ], indirect=True
)
def test_single_processing_function(processor_functions: List[ProcessorFunction]):
    processor, source, sink = _create_single_source_processor(
        "single-processing-function", processor_functions
    )

    msgs = processor.process_message(
        source.rule, "basemessage"
    )

    assert isinstance(msgs, list)
    assert len(msgs) == 1
    assert msgs[0].message_body == "basemessage<concat1>"
    assert msgs[0].sink_topic == source


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
