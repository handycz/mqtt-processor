from importlib import reload
from pathlib import Path
from typing import TextIO, List

import pytest
from _pytest.fixtures import SubRequest

import mqttprocessor.functions
from mqttprocessor.functions import ProcessorFunction, create_functions
from mqttprocessor.messages import routedmessage
from mqttprocessor.models import ExtendedFunctionModel


@pytest.fixture(scope="function")
def rule() -> type(mqttprocessor.functions.rule):
    reload(mqttprocessor.functions)

    return mqttprocessor.functions.rule


@pytest.fixture(scope="function")
def converter() -> type(mqttprocessor.functions.converter):
    reload(mqttprocessor.functions)

    return mqttprocessor.functions.converter


@pytest.fixture(scope="function")
def config_file_stream(request: SubRequest) -> TextIO:
    path = Path("tests/testcase_files/config/" + request.param)
    with open(path, "r") as f:
        yield f


@pytest.fixture(scope="function")
def processor_functions(request: SubRequest) -> List[ProcessorFunction]:
    reload(mqttprocessor.functions)
    rule = mqttprocessor.functions.rule
    converter = mqttprocessor.functions.converter

    @rule
    def dummy_rule_false(x):
        return False

    @rule
    def dummy_rule_true(x):
        return True

    @converter
    def dummy_str_concat1(x):
        return x + "<concat1>"

    @converter
    def dummy_str_concat2(x):
        return x + "<concat2>"

    @converter
    def dummy_str_concat_with_params(x, a, b):
        return "{}<concat-a+b={}+{}={}>".format(x, a, b, a+b)

    @converter
    def dummy_routed_dict(x):
        return routedmessage({
            "dict/routed/destination/topic": x + "<dict-routed>"
        })

    @rule
    def dummy_rule_failing(x):
        raise Exception()

    @converter
    def dummy_str_failing(x):
        raise Exception()

    return _create_functions(request)


@pytest.fixture(scope="function")
def processor_functions_routed(request: SubRequest) -> List[ProcessorFunction]:
    reload(mqttprocessor.functions)
    converter = mqttprocessor.functions.converter

    @converter
    def dummy_routed_dict(x):
        return routedmessage({
            "dict/routed/destination/topic": x + "<dict-routed>"
        })

    @converter
    def dummy_routed_dict_multiple(x):
        return routedmessage({
            "multiroute-dict/routed/destination/topic1": x + "<multiroute-dict1>",
            "multiroute-dict/routed/destination/topic2": x + "<multiroute-dict2>",
            "multiroute-dict/routed/destination/topic3": x + "<multiroute-dict3>",
        })

    @converter
    def dummy_routed_list(x):
        return routedmessage([
            x + "<routed_list-msg1>",
            x + "<routed_list-msg2>",
            x + "<routed_list-msg3>"
        ])

    @converter
    def dummy_routed_tuple_containing_multiple(x):
        return routedmessage((
            "tuple-of-lists/routed/destination/topic",
            [
                x + "<routed_tuple_of_lists-msg1>",
                x + "<routed_tuple_of_lists-msg2>",
                x + "<routed_tuple_of_lists-msg3>"
            ]
        ))

    @converter
    def dummy_routed_tuple_containing_single(x):
        return routedmessage((
            "tuple/routed/destination/topic",
            x + "<routed-tuple>"
        ))

    return _create_functions(request)


@pytest.fixture(scope="function")
def processor_functions_hierarchical(request: SubRequest) -> List[ProcessorFunction]:
    reload(mqttprocessor.functions)
    converter = mqttprocessor.functions.converter

    @converter
    def dummy_routed_dict_hierarchical(x):
        return routedmessage({
            "dict/routed/destination/topic": routedmessage(
                [
                    x + "<dict-hierarchical1>",
                    x + "<dict-hierarchical2>",
                ]
            )
        })

    @converter
    def dummy_routed_dict_multiple_hierarchical(x):
        return routedmessage({
            "multiroute-dict/routed/destination/topic1": routedmessage([
                x + "<hierarchical-dict-multiple1-1>",
                x + "<hierarchical-dict-multiple1-2>"
            ]),
            "multiroute-dict/routed/destination/topic2": routedmessage([
                x + "<hierarchical-dict-multiple2-1>",
                x + "<hierarchical-dict-multiple2-2>"
            ]),
        })

    return _create_functions(request)


def _create_functions(request: SubRequest) -> List[ProcessorFunction]:
    register = mqttprocessor.functions.create_processor_register()
    models = _create_function_models(request)

    return create_functions(
        models, register
    )


def _create_function_models(request: SubRequest) -> List[ExtendedFunctionModel]:
    if not hasattr(request, "param"):
        raise AttributeError("Parameters not set")
    if not isinstance(request.param, list):
        raise AttributeError("The parameter has to be a list")
    functions = request.param

    models = list()

    for func in functions:
        if isinstance(func, tuple) and len(func) == 2:
            name = func[0]
            args = func[1]
        else:
            name = func
            args = None

        models.append(
            ExtendedFunctionModel(
                name=name,
                arguments=args
            )
        )

    return models
