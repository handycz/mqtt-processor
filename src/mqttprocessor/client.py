import logging
from typing import List

from src.mqttprocessor.definitions import ProcessorFunctionType
from src.mqttprocessor.messages import MultiMessage, TopicName, Message
from src.mqttprocessor.models import ProcessorConfigModel
from src.mqttprocessor.processors import ProcessorFunction


class Processor:
    __name__: str
    _logger: logging.Logger
    _topics: List[TopicName]
    _functions: List[ProcessorFunction]
    _sink_topic: TopicName

    def __init__(self, processor_config: ProcessorConfigModel):
        self._logger = logging.getLogger(__name__ + "=" + processor_config.name)
        # todo: verify function signature
        # todo: plug in config arguments to the functions
        ...

    def process_message(self, source_topic: str, message: ...) -> List[Message]:
        if not self._source_topic_registered(source_topic):
            return []

        output_message_body = self._process_message_body(message)
        return self._create_message_with_destination(source_topic, output_message_body)

    def _source_topic_registered(self, topic_rule: str) -> bool:
        for topic in self._topics:
            if topic.matches(topic_rule):
                return True

        return False

    def _process_message_body(self, input_message: ...) -> ...:
        message = input_message
        for function in self._functions:
            if function.ptype == ProcessorFunctionType.RULE:
                if not function.callback(message):
                    return None
            else:
                message = function.callback(message)

                if isinstance(message, MultiMessage):
                    self._logger.error(
                        "Ignoring multimessage produced by `%s`, because it's followed by another function",
                        function.callback.__name__
                    )
                    return None

        return message

    def _create_message_with_destination(self, source_topic: str, output_message_body: ...) -> List[Message]:
        if output_message_body is None:
            return []
        elif isinstance(output_message_body, MultiMessage):
            return self._decompose_multi_message(source_topic, output_message_body)
        else:
            return [Message(
                self._sink_topic.compose_static_topic_name(source_topic),
                output_message_body
            )]

    def _decompose_multi_message(self, source_topic: str, multi_message: MultiMessage):
        if multi_message.is_list:
            return [
                Message(
                    self._sink_topic.compose_static_topic_name(source_topic),
                    body
                )
                for body in multi_message.payload
            ]
        elif multi_message.is_dict:
            return [
                Message(
                    TopicName(sink_topic).compose_static_topic_name(source_topic),
                    body
                )
                for sink_topic, body in multi_message.payload
            ]
        else:
            self._logger.warning("Plain message marked as multimessage")
            return Message(
                self._sink_topic.compose_static_topic_name(source_topic),
                multi_message.payload
            )
