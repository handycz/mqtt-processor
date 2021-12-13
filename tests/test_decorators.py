from typing import Callable

import pytest

from src.mqttprocessor.processors import create_processor_register
from src.mqttprocessor.definitions import ProcessorFunctionType


def test_rule_registration(rule: Callable):
    @rule
    def r():
        pass

    @rule
    def r2():
        pass


def test_rule_registration_explicit_name(rule: Callable):
    @rule(name="r1")
    def r():
        pass

    @rule(name="r2")
    def r():
        pass


def test_rule_registration_duplicate(rule: Callable):
    with pytest.raises(ValueError):
        @rule
        def r():
            pass

        @rule
        def r():
            pass


def test_converter_registration(converter: Callable):
    @converter
    def c():
        pass

    @converter
    def c2():
        pass


def test_converter_registration_explicit_name(converter: Callable):
    @converter(name="c1")
    def c():
        pass

    @converter(name="c2")
    def c():
        pass


def test_converter_registration_duplicate(converter: Callable):
    with pytest.raises(ValueError):
        @converter
        def c():
            pass

        @converter
        def c():
            pass


def test_registration_duplicate(converter: Callable, rule: Callable):
    with pytest.raises(ValueError):
        @converter
        def c():
            pass

        @rule
        def c():
            pass


def test_processor_register(converter: Callable, rule: Callable):
    @converter
    def c():
        pass

    @converter(name="c2")
    def c():
        pass

    @rule
    def r():
        pass

    @rule(name="r2")
    def r():
        pass

    actual = {name: proc.ptype for name, proc in create_processor_register().items()}
    expected = {
        "c": ProcessorFunctionType.CONVERTER,
        "c2": ProcessorFunctionType.CONVERTER,
        "r": ProcessorFunctionType.RULE,
        "r2": ProcessorFunctionType.RULE
    }

    assert actual == expected
