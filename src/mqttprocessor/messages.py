from typing import Dict, Any, Iterable

from src.mqttprocessor.models import TopicNameModel


class TopicName(TopicNameModel):
    def __init__(self, rule: str, allow_complex: bool = True):
        super().__init__(__root__=rule)
        # todo: init regex for matching
        # todo: init regex for argument search and replace
        ...

    def matches(self, topic_rule: 'TopicName') -> bool:
        ...

    def compose_static_topic_name(self, from_topic: 'TopicName') -> 'TopicName':
        ...


class Message:
    def __init__(self, sink_topic: TopicName, message_body: ...):
        ...
    # todo: serialize-like function


class MultiMessage:
    payload: Dict[str, Any] | Iterable[Any]

    @property
    def is_list(self) -> bool:
        return isinstance(self.payload, list) \
               or isinstance(self.payload, tuple) \
               or isinstance(self.payload, set)

    @property
    def is_dict(self) -> bool:
        return isinstance(self.payload, dict)


multimessage = MultiMessage
