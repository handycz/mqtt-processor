from typing import List, Tuple

from src.mqttprocessor.functions import ProcessorFunction
from src.mqttprocessor.messages import TopicName
from src.mqttprocessor.routing import SingleSourceProcessor


def _create_single_source_processor(
        name: str, functions: List[ProcessorFunction],
        source_topic: str = None, sink_topic: str = None, allow_sink_none: bool = False
) -> Tuple[SingleSourceProcessor, TopicName, TopicName]:
    if source_topic is None:
        source_topic = name + "/source"

    if sink_topic is None and not allow_sink_none:
        sink_topic = name + "/sink"

    source_topic = TopicName(source_topic)

    if sink_topic is not None:
        sink_topic = TopicName(sink_topic)

    processor = SingleSourceProcessor(
        name, functions,
        source_topic, sink_topic
    )

    return processor, source_topic, sink_topic
