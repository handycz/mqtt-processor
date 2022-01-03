import re
from typing import Dict, Any, Iterable, Pattern, List

from src.mqttprocessor.models import TOPIC_NAME_REGEX_PATTERN


class RegexPatternCreator:
    _SINGLE_LEVEL_REGEX = re.compile(r"{(w[0-9]+)}")
    _MULTI_LEVEL_REGEX = re.compile(r"{(W[0-9]+)}")

    _SINGLE_LEVEL_PATTERN = r"[^\/]+"
    _MULTI_LEVEL_PATTERN = r"[^\/]*\/?"

    _rule: List[str]

    def __init__(self, rule: str):
        # rule == "a/{W1}/b/{w1}/c{w4}
        self._rule = rule.split("/")

    def create_pattern_regex_match(self) -> Pattern:
        pattern_levels = []
        for level in self._rule:
            level = self._SINGLE_LEVEL_REGEX.sub(
                lambda m: self._SINGLE_LEVEL_PATTERN,
                level
            )

            level = self._MULTI_LEVEL_REGEX.sub(
                lambda m: self._MULTI_LEVEL_PATTERN,
                level
            )

            pattern_levels.append(
                level
            )

        return self._create_regex_from_levels(pattern_levels)

    def create_pattern_regex_extract_path_segments(self) -> Pattern:
        pattern_levels = []
        for level in self._rule:
            level = self._SINGLE_LEVEL_REGEX.sub(
                lambda m: "(?P<{name}>{pattern})".format(name=m.groups()[0], pattern=self._SINGLE_LEVEL_PATTERN),
                level
            )

            level = self._MULTI_LEVEL_REGEX.sub(
                lambda m: "(?P<{name}>{pattern})".format(name=m.groups()[0], pattern=self._MULTI_LEVEL_PATTERN),
                level
            )

            pattern_levels.append(
                level
            )

        return self._create_regex_from_levels(pattern_levels)

    @staticmethod
    def _create_regex_from_levels(levels: List[str]) -> Pattern:
        print("^" + r"\/".join(levels) + "$")

        return re.compile("^" + r"\/".join(levels) + "$")


class TopicName:
    _regex_topic_name = re.compile(TOPIC_NAME_REGEX_PATTERN)

    _regex_static_topic_matches_rule: Pattern[str]
    _regex_extract_path_segments: Pattern[str]

    def __init__(self, rule: str):
        if self._regex_topic_name.match(rule) is None:
            raise ValueError("Invalid topic name")

        regex_creator = RegexPatternCreator(rule)
        self._regex_static_topic_matches_rule = regex_creator.create_pattern_regex_match()
        self._regex_extract_path_segments = regex_creator.create_pattern_regex_extract_path_segments()

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
