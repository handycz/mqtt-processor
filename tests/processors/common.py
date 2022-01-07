from typing import List, Tuple

from src.mqttprocessor.functions import ProcessorFunction
from src.mqttprocessor.messages import TopicName
from src.mqttprocessor.routing import SingleSourceProcessor


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


# TODO:
#  - single processor test
#     - for all possible sink/source variations
#     - static and dynamic routed sinks
#     - static and dynamic constant sinks
#  - multi processor test
#     - topic matching
#     - message processing for normal and single routed message