from importlib import reload
from pathlib import Path
from typing import TextIO, Dict, List

import pytest
from _pytest.fixtures import SubRequest

import src.mqttprocessor.functions
from src.mqttprocessor.functions import ProcessorFunction, create_functions, ProcessorFunctionDefinition
from src.mqttprocessor.messages import routedmessage
from src.mqttprocessor.models import ExtendedFunctionModel, FunctionNameModel


@pytest.fixture(scope="function")
def rule() -> type(src.mqttprocessor.functions.rule):
    reload(src.mqttprocessor.functions)

    return src.mqttprocessor.functions.rule


@pytest.fixture(scope="function")
def converter() -> type(src.mqttprocessor.functions.converter):
    reload(src.mqttprocessor.functions)

    return src.mqttprocessor.functions.converter


@pytest.fixture(scope="function")
def config_file_stream(request: SubRequest) -> TextIO:
    path = Path("testcase_files/config/" + request.param)
    with open(path, "r") as f:
        yield f


@pytest.fixture(scope="function")
def processor_functions(request: SubRequest) -> List[ProcessorFunction]:
    reload(src.mqttprocessor.functions)
    rule = src.mqttprocessor.functions.rule
    converter = src.mqttprocessor.functions.converter

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
    def dummy_str_concat_routed_dict(x):
        return routedmessage({
            "concat/routed/destination/topic": x + "<concat_routed>"
        })

    @converter
    def dummy_str_concat_routed_dict_multiple(x):
        return routedmessage({
            "concat/routed/destination/topic": x + "<concat_routed1>",
            "concat/routed/destination/topic": x + "<concat_routed2>",
            "concat/routed/destination/topic": x + "<concat_routed3>",
        })

    @converter
    def dummy_str_concat_routed_list(x):
        return routedmessage({
            "concat/routed/destination/topic": x + "<concat_routed>"
        })

    @converter
    def dummy_str_concat_routed_tuple_containing_multiple(x):
        return routedmessage({
            "concat/routed/destination/topic": x + "<concat_routed>"
        })

    @converter
    def dummy_str_concat_routed_tuple_containing_single(x):
        return routedmessage({
            "concat/routed/destination/topic": x + "<concat_routed>"
        })

    register = src.mqttprocessor.functions.create_processor_register()
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
        if isinstance(func, tuple) and len(func) > 1:
            name = func[0]
            args = func[1:]
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
