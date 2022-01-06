from importlib import reload
from pathlib import Path
from typing import TextIO, Dict, List

import pytest
from _pytest.fixtures import SubRequest

import src.mqttprocessor.functions
from src.mqttprocessor.functions import ProcessorFunction, create_functions, ProcessorFunctionDefinition
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

    @rule
    def dummy_str_concat1(x):
        return x + "<concat1>"

    @rule
    def dummy_str_concat2(x):
        return x + "<concat2>"

    @rule
    def dummy_str_concat_with_param(x, val):
        return x + "<concat-" + val + ">"

    @rule
    def dummy_str_concat_rich_dict(x):
        return {
            "destination/topic": x + "<contact_rich>"
        }

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
