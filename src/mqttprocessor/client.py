import logging
import inspect
from functools import wraps
from typing import List, Optional, Dict, Any

from src.mqttprocessor.definitions import ProcessorFunctionType
from src.mqttprocessor.messages import RoutedMessage, TopicName, Message, MessageBody
from src.mqttprocessor.models import ProcessorConfigModel, ExtendedFunctionModel
from src.mqttprocessor.processors import ProcessorFunction, create_processor_register, ProcessorFunctionDefinition


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
            if function.ptype == ProcessorFunctionType.RULE:
                if not function.callback(message):
                    return None
            else:
                message = function.callback(message)

                if isinstance(message, RoutedMessage):
                    self._logger.error(
                        "Ignoring routed message produced by `%s`, because it's followed by another function",
                        function.callback.__name__
                    )
                    return None

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

    def _decompose_routed_messages(self, actual_source_topic: TopicName, routed_message: RoutedMessage):
        if routed_message.is_dict_of_routes_and_messages:
            return [
                Message(
                    self._get_sink_topic(actual_source_topic, sink_topic),
                    body
                )
                for sink_topic, body in routed_message.payload
            ]
        elif routed_message.is_list_of_messages_without_routes:
            return [
                Message(
                    self._get_sink_topic(actual_source_topic, self._default_sink_topic),
                    body
                )
                for body in routed_message.payload
            ]
        elif routed_message.is_single_route_and_list_of_messages:
            sink_topic: str = routed_message.payload[0]
            messages: List[Any] = routed_message.payload[1]
            return [
                Message(
                    self._get_sink_topic(actual_source_topic, sink_topic),
                    body
                )
                for body in messages
            ]
        elif routed_message.is_single_route_and_single_message:
            sink_topic: str = routed_message.payload[0]
            message: Any = routed_message.payload[1]
            return Message(
                self._get_sink_topic(actual_source_topic, sink_topic),
                message
            )
        else:
            self._logger.warning("routed message of unknown type, ignoring")
            return []

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

    def __init__(self, processor_config: ProcessorConfigModel):
        self._logger = logging.getLogger(__name__ + "=" + processor_config.name)

        functions = self._create_functions(processor_config.function)
        self._processors = [
            SingleSourceProcessor(
                name=processor_config.name, functions=functions,
                source_topic_rule=TopicName(topic.__root__),
                default_sink_topic=TopicName(processor_config.sink.__root__)

            ) for topic in processor_config.source
        ]

    def _create_functions(self, functions_config: List[ExtendedFunctionModel]) -> List[ProcessorFunction]:
        functions = list()
        global_function_store = create_processor_register()

        for function_config in functions_config:
            self._logger.info("Registering function %s", function_config.name)

            if function_config.name not in global_function_store:
                raise ValueError(f"Function `{function_config.name}` undefined")

            function_definition = global_function_store[function_config.name.__root__]

            Processor._verify_function_arguments(function_config, function_definition)
            function = Processor._create_function_representation(function_config, function_definition)

            functions.append(
                function
            )

        return functions

    @staticmethod
    def _verify_function_arguments(
            function_config: ExtendedFunctionModel, function_definition: ProcessorFunctionDefinition
    ):
        function_signature = inspect.signature(function_definition.callback)
        number_of_nondefault_params = [
            param.default for param in function_signature.parameters.values()
        ].count(inspect.Parameter.empty)
        num_args_in_config = len(function_config.arguments)
        num_params_of_function = number_of_nondefault_params - 1
        if num_args_in_config != num_params_of_function:
            raise ValueError(
                f"Config gives {num_args_in_config} arguments for "
                f"a function with {num_params_of_function} + 1 parameters"
            )

    @staticmethod
    def _create_function_representation(
            function_config: ExtendedFunctionModel, function_definition: ProcessorFunctionDefinition
    ) -> ProcessorFunction:
        @wraps(function_definition.callback)
        def _cbk_wrapper(val):
            function_definition.callback(val, *function_config.arguments)

        return ProcessorFunction(
            function_definition.ptype,
            _cbk_wrapper
        )

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
