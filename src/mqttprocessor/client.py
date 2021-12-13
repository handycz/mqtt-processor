from typing import List

from src.mqttprocessor.definitions import ProcessorFunctionType
from src.mqttprocessor.processors import ProcessorFunctionDefinition


class TopicName:
    def __init__(self, rule: str, allow_complex: bool = True):
        ...

    def matches(self, topic_rule: str):
        ...


class Processor:
    _topics: List[TopicName]
    _functions: List[ProcessorFunctionDefinition]
    _sink_topic: TopicName

    def process_message(self, topic: str, message: ...) -> ...:
        if not self._topic_registered(topic):
            return None

        return self._process_message(message)

    def _topic_registered(self, topic_rule: str) -> bool:
        for topic in self._topics:
            if topic.matches(topic_rule):
                return True

        return False

    def _process_message(self, message: ...) -> ...:
        for function in self._functions:
            if function.ptype == ProcessorFunctionType.RULE:
                ...
            else:
                ...
                message = function.callback(message)