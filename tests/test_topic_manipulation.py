import pytest

from src.mqttprocessor.messages import RegexPatternCreator, TopicName


def test_topic_name_invalid_rule():
    with pytest.raises(ValueError):
        TopicName(
            "{w1/a/w2}"
        )


def test_pattern_creator_match():
    pattern_creator = RegexPatternCreator(
        "device1/{w1}/property"
    )

    assert pattern_creator.create_pattern_regex_match().match(r"device1/device/property")
    assert not pattern_creator.create_pattern_regex_match().match(r"/device1/d/property")


def test_pattern_creator_extract_simple():
    expected = {"w1": "device"}
    pattern_creator = RegexPatternCreator(
        "device1/{w1}/property"
    )

    regex = pattern_creator.create_pattern_regex_extract_path_segments()
    actual = regex.search(r"device1/device/property").groupdict()

    assert actual == expected


def test_pattern_creator_extract_complex():
    expected = {"w1": "device"}
    pattern_creator = RegexPatternCreator(
        "device1/{w1}/property" # todo: some complex rule
    )

    regex = pattern_creator.create_pattern_regex_extract_path_segments()
    actual = regex.search(r"device1/device/property").groupdict()

    assert False
