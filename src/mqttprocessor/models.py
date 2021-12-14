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
    def email_set_config(cls, values):
        name, function = values.get('name'), values.get('function')
        if name is not None:
            return values

        if isinstance(function, list):
            function = function[0]

        if isinstance(function, FunctionNameModel):
            function_name = function.__root__
        else:
            function_name = function.name.__root__

        values["name"] = function_name + str(uuid.uuid1().time_low)
        return values


class ConfigModel(pydantic.BaseModel):
    processors: List[ProcessorConfigModel]
