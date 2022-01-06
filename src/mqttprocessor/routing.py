import logging
import inspect
from functools import wraps
from typing import List, Optional, Dict, Any

from src.mqttprocessor.definitions import ProcessorFunctionType
from src.mqttprocessor.messages import RoutedMessage, TopicName, Message, MessageBody
from src.mqttprocessor.models import ProcessorConfigModel, ExtendedFunctionModel
from src.mqttprocessor.functions import ProcessorFunction, create_processor_register, ProcessorFunctionDefinition, \
    create_functions


class SingleSourceProcessor:
    __name__: str
    _logger: logging.Logger
    _source_topic_rule: TopicName
    _functions: List[ProcessorFunction]
    _default_sink_topic: Optional[TopicName]

    def __init__(
            self, name: str, functions: List[ProcessorFunction],
            source_topic_rule: TopicName, default_sink_topic: TopicName
    ):
        self._logger = logging.getLogger(__name__ + "=" + name + "@" + source_topic_rule.rule)
        self._functions = functions
        self._source_topic_rule = source_topic_rule
        self._default_sink_topic = default_sink_topic

    def process_message(self, actual_source_topic: str, message: MessageBody) -> List[Message]:
        actual_source_topic = TopicName(actual_source_topic)

        if not self.source_topic_matches(actual_source_topic):
            return []

        output_message_body = self._process_message_content(message)
        return self._create_message_with_destination(actual_source_topic, output_message_body)

    def source_topic_matches(self, topic_rule: TopicName) -> bool:
        return self._source_topic_rule.matches(topic_rule)

    def _process_message_content(self, input_message: MessageBody) -> MessageBody:
        message = input_message
        for function in self._functions:
            if isinstance(message, RoutedMessage):
                self._logger.error(
                    "Ignoring routed message produced by `%s`, because it's followed by another function",
                    function.callback.__name__
                )
                return None

            if function.ptype == ProcessorFunctionType.RULE:
                if not function.callback(message):
                    return None
            else:
                message = function.callback(message)

        return message

    def _create_message_with_destination(
            self, actual_source_topic: TopicName, output_message_body: MessageBody
    ) -> List[Message]:
        if output_message_body is None:
            return []
        elif isinstance(output_message_body, RoutedMessage):
            return self._decompose_routed_messages(actual_source_topic, output_message_body)
        else:
            return [
                Message(
                    self._get_sink_topic(actual_source_topic, self._default_sink_topic),
                    output_message_body
                )
            ]

    def _decompose_routed_messages(
            self, actual_source_topic: TopicName, routed_message: RoutedMessage
    ) -> List[Message]:
        outgoing_simple_messages = list()

        if routed_message.is_dict_of_routes_and_messages:
            for sink_topic, body in routed_message.payload.items():
                outgoing_simple_messages += self._create_message(
                    message=body,
                    actual_source_topic=actual_source_topic,
                    sink_topic=TopicName(sink_topic)
                )
        elif routed_message.is_list_of_messages_without_routes:
            for body in routed_message.payload:
                outgoing_simple_messages += self._create_message(
                    message=body,
                    actual_source_topic=actual_source_topic,
                    sink_topic=self._default_sink_topic
                )
        elif routed_message.is_single_route_and_list_of_messages:
            sink_topic: str = routed_message.payload[0]
            messages: List[Any] = routed_message.payload[1]
            for body in messages:
                outgoing_simple_messages += self._create_message(
                    message=body,
                    actual_source_topic=actual_source_topic,
                    sink_topic=TopicName(sink_topic)
                )
        elif routed_message.is_single_route_and_single_message:
            sink_topic: str = routed_message.payload[0]
            body: Any = routed_message.payload[1]

            outgoing_simple_messages += self._create_message(
                message=body,
                actual_source_topic=actual_source_topic,
                sink_topic=TopicName(sink_topic)
            )

        else:
            self._logger.warning("routed message of unknown type, ignoring")
            return []

        return outgoing_simple_messages

    def _create_message(
            self, message: RoutedMessage | MessageBody, actual_source_topic: TopicName, sink_topic: TopicName
    ) -> List[Message]:
        if isinstance(message, RoutedMessage):
            return self._decompose_routed_messages(actual_source_topic, message)

        return [
            Message(
                self._get_sink_topic(actual_source_topic, sink_topic),
                message_body=message
            )
        ]

    def _get_sink_topic(
            self, actual_source_topic: TopicName | str, selected_sink_topic_rule: TopicName | str
    ) -> TopicName:
        if isinstance(actual_source_topic, str):
            actual_source_topic = TopicName(actual_source_topic)

        if isinstance(selected_sink_topic_rule, str):
            selected_sink_topic_rule = TopicName(selected_sink_topic_rule)

        return self._source_topic_rule.compose_sink_topic_from_source(actual_source_topic, selected_sink_topic_rule)


class Processor:
    __name__: str
    _logger: logging.Logger
    _processors: List[SingleSourceProcessor]

    def __init__(self, name: str, functions: List[ProcessorFunction], sources: List[TopicName], sink: TopicName):
        self._logger = logging.getLogger(__name__ + "=" + name)

        self._processors = [
            SingleSourceProcessor(
                name=name, functions=functions,
                source_topic_rule=topic,
                default_sink_topic=sink

            ) for topic in sources
        ]

    def process_message(self, source_topic: str, message: MessageBody) -> List[Message]:
        matched_processor = self._get_relevant_source_topic(
            TopicName(source_topic)
        )

        if matched_processor is not None:
            return matched_processor.process_message(source_topic, message)

        return []

    def _get_relevant_source_topic(self, topic_rule: TopicName) -> Optional[SingleSourceProcessor]:
        for processor in self._processors:
            if processor.source_topic_matches(topic_rule):
                return processor

        return None


class ProcessorCreator:
    _config: ProcessorConfigModel

    def __init__(self, processor_config: ProcessorConfigModel):
        self._config = processor_config

    def create(self) -> Processor:
        return Processor(
            name=self._config.name,
            functions=create_functions(
                self._config.function
            ),
            sources=[
                TopicName(source.__root__) for source in self._config.source
            ],
            sink=TopicName(self._config.sink.__root__)
        )
