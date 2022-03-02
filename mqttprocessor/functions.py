import inspect
import logging
from dataclasses import dataclass
from functools import wraps
from importlib import import_module
from typing import Dict, List

from .definitions import (
    BodyType,
    RawRuleType,
    RawConverterType,
    ProcessorFunctionType,
    ConverterType,
    RuleType,
)
from .models import ExtendedFunctionModel

_SPECIAL_PARAMETERS = ["source_topic", "matches"]

_REGISTERED_PROCESSOR_FUNCTIONS: Dict[str, "ProcessorFunctionDefinition"] = dict()

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=False)
class ProcessorFunctionDefinition:
    name: str
    ptype: ProcessorFunctionType
    callback: RawRuleType | RawConverterType

    def __eq__(self, other) -> bool:
        if other is None:
            return False

        if not isinstance(other, ProcessorFunctionDefinition):
            return False

        if self.name != other.name:
            return False

        if self.ptype != other.ptype:
            return False

        return True


@dataclass(frozen=True)
class ProcessorFunction:
    ptype: ProcessorFunctionType
    callback: RuleType | ConverterType
    expects_matches: bool
    expects_source_topic: bool


def create_functions(
    functions_config: List[ExtendedFunctionModel],
    register: Dict[str, ProcessorFunctionDefinition] = None,
) -> List[ProcessorFunction]:
    import_module(".builtin", "mqttprocessor")

    functions = list()

    if register is None:
        global_function_store = create_processor_register()
    else:
        global_function_store = register

    for function_config in functions_config:
        if function_config.name.__root__ not in global_function_store:
            raise ValueError(f"Function `{function_config.name.__root__}` undefined")

        function_definition = global_function_store[function_config.name.__root__]

        _verify_function_arguments(function_config, function_definition)
        function = _create_function_representation(function_config, function_definition)

        functions.append(function)

    return functions


def _verify_function_arguments(
    function_config: ExtendedFunctionModel,
    function_definition: ProcessorFunctionDefinition,
):
    function_signature = inspect.signature(function_definition.callback)

    non_special_parameters = filter(
        lambda parameter: parameter.name not in _SPECIAL_PARAMETERS,
        function_signature.parameters.values()
    )

    number_of_nondefault_params = [
        param.default for param in non_special_parameters
    ].count(inspect.Parameter.empty)
    num_args_in_config = len(function_config.arguments.items())
    num_params_of_function = number_of_nondefault_params - 1
    if num_args_in_config != num_params_of_function:
        raise ValueError(
            f"Config gives {num_args_in_config} arguments for "
            f"a function with {num_params_of_function} + 1 parameters"
        )


def _create_function_representation(
    function_config: ExtendedFunctionModel,
    function_definition: ProcessorFunctionDefinition,
) -> ProcessorFunction:
    @wraps(function_definition.callback)
    def _cbk_wrapper(val):
        return function_definition.callback(val, **function_config.arguments)

    function_signature = inspect.signature(function_definition.callback)
    function_parameter_names = filter(
        lambda parameter: parameter.name,
        function_signature.parameters.values()
    )
    expects_source_topic = "source_topic" in function_parameter_names
    expects_matches = "matches" in function_parameter_names

    return ProcessorFunction(
        function_definition.ptype, _cbk_wrapper,
        expects_source_topic=expects_source_topic,
        expects_matches=expects_matches
    )


def _register_processor_function(
    name: str, func: RawRuleType | RawConverterType, ptype: ProcessorFunctionType
):
    _logger.info("Registering function %s", name)
    if name in _REGISTERED_PROCESSOR_FUNCTIONS:
        _logger.error("Function '%s' already registered", name)
        raise ValueError("Names must be unique")

    _REGISTERED_PROCESSOR_FUNCTIONS[name] = ProcessorFunctionDefinition(
        name=name, ptype=ptype, callback=func
    )


def create_processor_register() -> Dict[str, ProcessorFunctionDefinition]:
    return dict(_REGISTERED_PROCESSOR_FUNCTIONS)


def rule(original_function=None, *, name: str = None):
    def decorator(func):
        if name is None:
            rule_name = func.__name__
        else:
            rule_name = name
        _register_processor_function(rule_name, func, ProcessorFunctionType.RULE)

        @wraps(func)
        def wrapper(body: BodyType):
            func(body)

        return wrapper

    if original_function is not None:
        return decorator(original_function)
    return decorator


def converter(original_function=None, *, name: str = None):
    def decorator(func):
        if name is None:
            rule_name = func.__name__
        else:
            rule_name = name
        _register_processor_function(rule_name, func, ProcessorFunctionType.CONVERTER)

        @wraps(func)
        def wrapper(body: BodyType):
            func(body)

        return wrapper

    if original_function is not None:
        return decorator(original_function)
    return decorator
