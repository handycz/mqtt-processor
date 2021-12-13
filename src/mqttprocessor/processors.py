import logging
from dataclasses import dataclass
from functools import wraps
from typing import Dict

from .definitions import BodyType, RuleType, ConverterType, ProcessorFunctionType

_REGISTERED_PROCESSORS: Dict[str, 'ProcessorFunctionDefinition'] = dict()

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=False)
class ProcessorFunctionDefinition:
    name: str
    ptype: ProcessorFunctionType
    callback: RuleType | ConverterType

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


def _register_processor(name: str, func: RuleType | ConverterType, ptype: ProcessorFunctionType):
    _logger.info("Registering function %s", name)
    if name in _REGISTERED_PROCESSORS:
        _logger.error("Function '%s' already registered", name)
        raise ValueError("Names must be unique")

    _REGISTERED_PROCESSORS[name] = ProcessorFunctionDefinition(
        name=name,
        ptype=ptype,
        callback=func
    )


def create_processor_register() -> Dict[str, ProcessorFunctionDefinition]:
    return dict(
        _REGISTERED_PROCESSORS
    )


def rule(original_function=None, *, name: str = None):
    def decorator(func):
        if name is None:
            rule_name = func.__name__
        else:
            rule_name = name
        _register_processor(rule_name, func, ProcessorFunctionType.RULE)

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
        _register_processor(rule_name, func, ProcessorFunctionType.CONVERTER)

        @wraps(func)
        def wrapper(body: BodyType):
            func(body)
        return wrapper

    if original_function is not None:
        return decorator(original_function)
    return decorator