import uuid
from enum import Enum
from typing import List, Optional, Any, Dict

import pydantic


TOPIC_NAME_REGEX_PATTERN = r"^(?:\/?(?:(?:(?:{[^{}\/]+})|(?:[^{}\/]+))+\/?)+)\/?$"


class TopicNameModel(pydantic.BaseModel):
    __root__: pydantic.constr(regex=TOPIC_NAME_REGEX_PATTERN)


class FunctionNameModel(pydantic.BaseModel):
    __root__: str


class MessageFormat(Enum):
    BINARY = "binary"
    STRING = "string"
    JSON = "json"


class ExtendedFunctionModel(pydantic.BaseModel):
    name: FunctionNameModel
    arguments: Optional[Dict[str, Any]]

    @pydantic.root_validator(pre=True)
    def args_set(cls, values):
        if values.get("arguments", None) is None:
            values["arguments"] = []

        return values


class ProcessorConfigModel(pydantic.BaseModel):
    name: Optional[str]
    source: List[TopicNameModel]
    sink: Optional[TopicNameModel]
    function: List[ExtendedFunctionModel]
    input_format: Optional[MessageFormat] = MessageFormat.JSON

    @pydantic.root_validator(pre=True)
    def unify_function_format(cls, values):
        if "function" not in values:
            return values

        function = values["function"]
        if isinstance(function, list):
            values["function"] = [
                cls._normalize_single_function(func) for func in function
            ]
        else:
            values["function"] = [cls._normalize_single_function(function)]

        return values

    @classmethod
    def _normalize_single_function(
        cls, function: str | Dict[str, Any]
    ) -> Dict[str, Any]:
        if isinstance(function, str):
            return {"name": function}
        else:
            return function

    @pydantic.root_validator(pre=True)
    def source_to_list(cls, values):
        if "source" not in values:
            return values

        if isinstance(values["source"], str):
            values["source"] = [values["source"]]

        return values

    @pydantic.root_validator
    def set_default_name(cls, values):
        name, function = values.get("name"), values.get("function")
        if name is not None:
            return values

        if function is None:
            function_name = "UnnamedFunction"
        else:
            function_name = function[0].name.__root__

        values["name"] = function_name + str(uuid.uuid1().time_low)
        return values


class ConfigModel(pydantic.BaseModel):
    processors: List[ProcessorConfigModel]
