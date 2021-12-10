from typing import List, Optional, TextIO

import pydantic
import yaml

TopicName = pydantic.constr(regex=r"^(\/?((({[^{}\/]+})|([^{}\/]+))+\/)+)|\/?((({[^{}\/]+})|([^{}\/]+))+)$")


class ProcessorConfig(pydantic.BaseModel):
    source: TopicName | List[TopicName]
    sink: Optional[TopicName]
    function: str | List[str]


class Config(pydantic.BaseModel):
    processors: List[ProcessorConfig]


def load_config(stream: TextIO) -> Config:
    data = yaml.load(stream, yaml.CLoader)
    return Config(**data)
