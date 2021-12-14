import logging
import inspect
from functools import wraps
from typing import List, Optional

from src.mqttprocessor.definitions import ProcessorFunctionType
from src.mqttprocessor.messages import MultiMessage, TopicName, Message
from src.mqttprocessor.models import ProcessorConfigModel, ExtendedFunctionModel
from src.mqttprocessor.processors import ProcessorFunction, create_processor_register, ProcessorFunctionDefinition


class Processor:
    __name__: str
    _logger: logging.Logger
    _source_topics: List[TopicName]
    _functions: List[ProcessorFunction]
    _sink_topic: Optional[TopicName]

    def __init__(self, processor_config: ProcessorConfigModel):
        self._logger = logging.getLogger(__name__ + "=" + processor_config.name)
        self._functions = self._create_functions(processor_config.function)
        self._source_topics = [TopicName(topic.__root__) for topic in processor_config.source]
        self._sink_topic = TopicName(processor_config.sink.__root__) if processor_config.sink is not None else None

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

    # TODO: message type
    def process_message(self, source_topic: str, message: ...) -> List[Message]:
        source_topic = TopicName(source_topic)

        if not self._source_topic_registered(source_topic):
            return []

        output_message_body = self._process_message_content(message)
        return self._create_message_with_destination(source_topic, output_message_body)

    def _source_topic_registered(self, topic_rule: TopicName) -> bool:
        for topic in self._source_topics:
            if topic.matches(topic_rule):
                return True

        return False

    # TODO: 2x message type
    def _process_message_content(self, input_message: ...) -> ...:
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

    # TODO: message type
    def _create_message_with_destination(self, source_topic: TopicName, output_message_body: ...) -> List[Message]:
        if output_message_body is None:
            return []
        elif isinstance(output_message_body, MultiMessage):
            return self._decompose_multi_message(source_topic, output_message_body)
        else:
            return [
                self._compose_message_from_body(output_message_body, source_topic)
            ]

    def _decompose_multi_message(self, source_topic: TopicName, multi_message: MultiMessage):
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
            return self._compose_message_from_body(multi_message.payload)

    # TODO: message type
    def _compose_message_from_body(
            self, body: ..., source_topic: TopicName = None, sink_topic: TopicName = None
    ) -> Message:
        if sink_topic is None and self._sink_topic is not None:
            sink_topic = self._sink_topic
        elif sink_topic is None and self._sink_topic is not None:
            self._logger.error("Unknown sink topic")
            raise ValueError("Unknown sink topic")

        return Message(
            sink_topic.compose_static_topic_name(source_topic),
            body
        )
