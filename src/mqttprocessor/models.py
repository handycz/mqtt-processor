import uuid
from typing import List, Optional, Any, Union

import pydantic


class TopicNameModel(pydantic.BaseModel):
    __root__: pydantic.constr(regex=r"^(?:\/?(?:(?:(?:{[^{}\/]+})|(?:[^{}\/]+))+\/?)+)\/?$")


class FunctionNameModel(pydantic.BaseModel):
    __root__: str


class ExtendedFunctionModel(pydantic.BaseModel):
    name: FunctionNameModel
    arguments: Optional[List[Any]]


class ProcessorConfigModel(pydantic.BaseModel):
    name: Optional[str]
    source: TopicNameModel | List[TopicNameModel]
    sink: Optional[TopicNameModel]
    function: List[Union[FunctionNameModel, ExtendedFunctionModel]] | ExtendedFunctionModel | FunctionNameModel

    @pydantic.root_validator
    def function_set_config(cls, values):
        if "function" not in values:
            return values

        function = values["function"]
        if isinstance(function, list):
            values["function"] = [cls._normalize_single_function(func) for func in function]
        else:
            values["function"] = [cls._normalize_single_function(function)]

        return values

    @classmethod
    def _normalize_single_function(
            cls, function: Union[FunctionNameModel, ExtendedFunctionModel]
    ) -> ExtendedFunctionModel:
        if isinstance(function, FunctionNameModel):
            return ExtendedFunctionModel(name=function.__root__)
        elif isinstance(function, ExtendedFunctionModel):
            return function

        raise ValueError("unknown function definition")

    @pydantic.root_validator
    def name_set_config(cls, values):
        name, function = values.get('name'), values.get('function')
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
