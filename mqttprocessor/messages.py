import re
from dataclasses import dataclass
from typing import Dict, Any, Iterable, Pattern, Set, List, Sequence

from mqttprocessor.models import TOPIC_NAME_REGEX_PATTERN


class PatternGroupCreator:
    _existing_groups: Set[str]

    def __init__(self):
        self._existing_groups = set()

    def get_pattern(self, group_name: str, expected_pattern: str):
        if group_name in self._existing_groups:
            return "(?P={name})".format(name=group_name)
        else:
            self._existing_groups.add(group_name)
            return "(?P<{name}>{pattern})".format(
                name=group_name, pattern=expected_pattern
            )


class RegexPatternCreator:
    _SINGLE_LEVEL_REGEX = re.compile(r"{(w[0-9]+)}")
    _MULTI_LEVEL_REGEX = re.compile(r"{(W[0-9]+)}")

    _SINGLE_LEVEL_PATTERN = r"[^\/]+"
    _MULTI_LEVEL_PATTERN = r"(.+)+?"

    _rule: List[str]

    def __init__(self, rule: str):
        self._rule = rule.split("/")

    def create_regex(self) -> Pattern:
        pattern_creator = PatternGroupCreator()
        pattern_levels = []
        for level in self._rule:
            level = self._SINGLE_LEVEL_REGEX.sub(
                lambda m: pattern_creator.get_pattern(
                    m.groups()[0], self._SINGLE_LEVEL_PATTERN
                ),
                level,
            )

            level = self._MULTI_LEVEL_REGEX.sub(
                lambda m: pattern_creator.get_pattern(
                    m.groups()[0], self._MULTI_LEVEL_PATTERN
                ),
                level,
            )

            pattern_levels.append(level)

        return self._create_regex_from_levels(pattern_levels)

    @staticmethod
    def _create_regex_from_levels(levels: List[str]) -> Pattern:
        return re.compile("^" + r"\/".join(levels) + "$")


class TopicName:
    # TODO: Caching: 1) rule format check, 2) rule regex compilation, 3) rule regex matching, 4) rule regex composing
    _regex_rule_format = re.compile(TOPIC_NAME_REGEX_PATTERN)

    _regex_topic_name_extract: Pattern[str]
    _rule: str
    _rule_is_static: bool

    @property
    def rule(self) -> str:
        return self._rule

    def __init__(self, rule: str):
        self._rule = rule

        if "{" in self._rule:
            self._rule_is_static = False
        else:
            self._rule_is_static = True

        if not self._rule_is_static:
            if self._regex_rule_format.match(self._rule) is None:
                raise ValueError("Invalid topic name")

            regex_creator = RegexPatternCreator(rule)
            self._regex_topic_name_extract = regex_creator.create_regex()

    def convert_rule_to_mqtt_format(self) -> str:
        if self._rule_is_static:
            return self._rule

        rule = self._rule
        search_result = self._regex_topic_name_extract.search(rule)

        groups = search_result.groupdict()
        for group_id, value in groups.items():
            if group_id.startswith("W"):
                rule = rule.replace("{{{0}}}".format(group_id), "#")
            else:
                rule = rule.replace("{{{0}}}".format(group_id), "+")

        return rule

    def matches(self, topic_rule: "TopicName") -> Dict[str, str] | None:
        checked_topic_name = topic_rule.rule

        if self._rule_is_static:
            return {} if checked_topic_name == self._rule else None

        search_result = self._regex_topic_name_extract.search(checked_topic_name)

        if search_result is None:
            return None
        else:
            return search_result.groupdict()

    def compose_sink_topic_from_source(
        self, extract_from: "TopicName", embed_into: "TopicName"
    ) -> "TopicName":
        if self._rule_is_static:
            return embed_into

        search_result = self._regex_topic_name_extract.search(extract_from.rule)
        if search_result is None:
            raise ValueError("Topic `extract_from` does not match the template")

        groups = search_result.groupdict()
        sink_topic = embed_into.rule
        for group_id, value in groups.items():
            sink_topic = sink_topic.replace("{{{0}}}".format(group_id), value)

        return TopicName(sink_topic)

    def __hash__(self):
        return hash(self._rule) ^ hash(self._rule_is_static)

    def __eq__(self, other) -> bool:
        if not isinstance(other, TopicName):
            return False

        if self._rule != other._rule:
            return False

        if self._rule_is_static != other._rule_is_static:
            return False

        return True

    def __repr__(self) -> str:
        return "TopicName(rule={0}, static={1})".format(
            self._rule, self._rule_is_static
        )


@dataclass(frozen=True)
class Message:
    sink_topic: TopicName
    message_body: "MessageBody"


class RoutedMessage:
    payload: Dict[str, Any] | Sequence[Any]

    def __init__(self, payload: Any):
        self.payload = payload

    @property
    def is_dict_of_routes_and_messages(self) -> bool:
        return isinstance(self.payload, dict)

    @property
    def is_list_of_messages_without_routes(self) -> bool:
        return isinstance(self.payload, list)

    @property
    def is_single_route_and_list_of_messages(self) -> bool:
        return (
            isinstance(self.payload, tuple)
            and len(self.payload) == 2
            and isinstance(self.payload[0], str)
            and isinstance(self.payload[1], list)
        )

    @property
    def is_single_route_and_single_message(self) -> bool:
        return (
            isinstance(self.payload, tuple)
            and len(self.payload) == 2
            and isinstance(self.payload[0], str)
            and not isinstance(self.payload[1], list)
        )


MessageBody = Any
routedmessage = RoutedMessage
