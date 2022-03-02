from enum import Enum
from typing import Union, Protocol, Type, TYPE_CHECKING, Any, Callable, Dict

if TYPE_CHECKING:

    class JSONArray(list[JsonType], Protocol):  # type: ignore
        __class__: Type[list[JsonType]]  # type: ignore

    class JSONObject(dict[str, JsonType], Protocol):  # type: ignore
        __class__: Type[dict[str, JsonType]]  # type: ignore

    JsonType = Union[None, float, str, JSONArray, JSONObject]
else:
    JsonType = Any

BodyType = str | bytes | JsonType
RawRuleType = Callable[..., bool]
RawConverterType = Callable[..., Any]

RuleType = Callable[[Any, Dict[str, Any]], bool]
ConverterType = Callable[[Any, Dict[str, Any]], Any]


class ProcessorFunctionType(Enum):
    RULE = 1
    CONVERTER = 2
