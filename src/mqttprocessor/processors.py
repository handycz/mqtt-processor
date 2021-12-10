import logging
from dataclasses import dataclass
from functools import wraps
from typing import Dict

from .definitions import BodyType, RuleType, ConverterType, ProcessorType

_REGISTERED_PROCESSORS: Dict[str, 'Processor'] = dict()

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=False)
class Processor:
    name: str
    ptype: ProcessorType
    callback: RuleType | ConverterType

    def __eq__(self, other) -> bool:
        if other is None:
            return False

        if not isinstance(other, Processor):
            return False

        if self.name != other.name:
            return False

        if self.ptype != other.ptype:
            return False

        return True


def _register_processor(name: str, func: RuleType | ConverterType, ptype: ProcessorType):
    _logger.info("Registering function %s", name)
    if name in _REGISTERED_PROCESSORS:
        _logger.error("Function '%s' already registered", name)
        raise ValueError("Names must be unique")

    _REGISTERED_PROCESSORS[name] = Processor(
        name=name,
        ptype=ptype,
        callback=func
    )


def create_processor_register() -> Dict[str, Processor]:
    return dict(
        _REGISTERED_PROCESSORS
    )


def rule(original_function=None, *, name: str = None):
    def decorator(func):
        if name is None:
            rule_name = func.__name__
        else:
            rule_name = name
        _register_processor(rule_name, func, ProcessorType.RULE)

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
        _register_processor(rule_name, func, ProcessorType.CONVERTER)

        @wraps(func)
        def wrapper(body: BodyType):
            func(body)
        return wrapper

    if original_function is not None:
        return decorator(original_function)
    return decorator