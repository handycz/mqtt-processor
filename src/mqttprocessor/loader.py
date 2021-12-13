from typing import List, Optional, TextIO, Any, Tuple, Set, Union

import pydantic
import yaml

TopicName = pydantic.constr(
    regex=r"^(?:\/?(?:(?:(?:{[^{}\/]+})|(?:[^{}\/]+))+\/?)+)\/?$"
)


class FunctionName(pydantic.BaseModel):
    __root__: str


class ExtendedFunction(pydantic.BaseModel):
    name: FunctionName
    arguments: Optional[List[Any]]


class FunctionDescription(pydantic.BaseModel):
    __root__: Union[FunctionName, ExtendedFunction]  # https://github.com/samuelcolvin/pydantic/issues/3300


class ProcessorConfig(pydantic.BaseModel):
    source: TopicName | List[TopicName]
    sink: Optional[TopicName]
    function: List[FunctionDescription] | ExtendedFunction | FunctionName


class Config(pydantic.BaseModel):
    processors: List[ProcessorConfig]


def load_config(stream: TextIO) -> Config:
    data = yaml.load(stream, yaml.CLoader)
    return Config(**data)
