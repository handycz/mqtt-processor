import pytest

from src.mqttprocessor.messages import RegexPatternCreator, TopicName


def test_topic_name_invalid_rule():
    with pytest.raises(ValueError):
        TopicName(
            "{w1/a/w2}"
        )


def test_topic_name_static_match():
    name = TopicName("device1/device")
    assert name.matches(TopicName("device1/device"))


def test_topic_name_static_no_match():
    name = TopicName("device1/device")
    assert not name.matches(TopicName("device1000/device"))


def test_topic_name_dynamic_match():
    name = TopicName("room1/{w1}/temperature")
    assert name.matches(TopicName("room1/device1/temperature"))


def test_topic_name_dynamic_no_match():
    name = TopicName("room1/{w1}/{w1}/temperature")
    assert not name.matches(TopicName("room1/device1/temperature"))


def test_topic_name_dynamic_multilevel_match():
    name = TopicName("{W1}/device1/temperature")
    assert name.matches(TopicName("building1/room1/device1/temperature"))


def test_topic_name_dynamic_multilevel_no_match():
    name = TopicName("{W1}/device1/temperature")
    assert not name.matches(TopicName("device1/temperature"))


def test_topic_name_static_match():
    name = TopicName("device1/device")
    assert False


def test_topic_name_static_no_match():
    name = TopicName("device1/device")
    assert False


def test_topic_name_dynamic_match():
    name = TopicName("room1/{w1}/temperature")
    assert False


def test_topic_name_dynamic_no_match():
    name = TopicName("room1/{w1}/{w1}/temperature")
    assert False


def test_topic_name_dynamic_multilevel_match():
    name = TopicName("{W1}/device1/temperature")
    assert False


def test_topic_name_dynamic_multilevel_no_match():
    name = TopicName("{W1}/device1/temperature")
    assert False


def test_pattern_creator_match_single_level():
    pattern_creator = RegexPatternCreator(
        "device1/{w1}/property"
    )

    assert pattern_creator.create_regex().match(r"device1/device/property")


def test_pattern_creator_match_multi_level():
    pattern_creator = RegexPatternCreator(
        "device1/{W1}/property"
    )

    assert pattern_creator.create_regex().match(r"device1/multilevel/device/property")


def test_pattern_creator_match_repeated():
    pattern_creator = RegexPatternCreator(
        "device1/{w1}/property/{w1}"
    )

    assert pattern_creator.create_regex().match(r"device1/foo/property/foo")


def test_pattern_creator_no_match():
    pattern_creator = RegexPatternCreator(
        "device1/{w1}/property"
    )

    assert pattern_creator.create_regex().match(r"/device1/d/property") is None


def test_pattern_creator_extract_simple():
    expected = {"w1": "device"}
    pattern_creator = RegexPatternCreator(
        "device1/{w1}/property"
    )

    regex = pattern_creator.create_regex()
    actual = regex.search(r"device1/device/property").groupdict()

    assert actual == expected


def test_pattern_creator_extract_complex():
    expected = {"w1": "albert", "w2": "bernard", "W10": "calculus/devil"}
    pattern_creator = RegexPatternCreator(
        "device1/{w1}/foo{w2}/bar/{W10}/property"
    )

    regex = pattern_creator.create_regex()
    actual = regex.search(r"device1/albert/foobernard/bar/calculus/devil/property").groupdict()

    assert actual == expected



