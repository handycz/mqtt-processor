from typing import List, Optional, Any, Union

import pydantic


class TopicNameModel(pydantic.BaseModel):
    __root__: pydantic.constr(regex=r"^(?:\/?(?:(?:(?:{[^{}\/]+})|(?:[^{}\/]+))+\/?)+)\/?$")


class FunctionNameModel(pydantic.BaseModel):
    __root__: str


class ExtendedFunctionModel(pydantic.BaseModel):
    name: FunctionNameModel
    arguments: Optional[List[Any]]


class FunctionDescriptionModel(pydantic.BaseModel):
    __root__: Union[FunctionNameModel, ExtendedFunctionModel]  # https://github.com/samuelcolvin/pydantic/issues/3300


class ProcessorConfigModel(pydantic.BaseModel):
    source: TopicNameModel | List[TopicNameModel]
    sink: Optional[TopicNameModel]
    function: List[FunctionDescriptionModel] | ExtendedFunctionModel | FunctionNameModel


class ConfigModel(pydantic.BaseModel):
    processors: List[ProcessorConfigModel]
