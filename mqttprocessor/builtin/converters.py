import json

from mqttprocessor.functions import converter


@converter
def binary_to_string(binary: bytes, encoding="utf8"):
    return binary.decode(encoding=encoding)


@converter
def binary_to_json(binary: bytes):
    return json.loads(binary)


@converter
def json_to_binary(json_data: bytes):
    return json.dumps(json_data)


@converter
def string_to_binary(string: bytes, encoding="utf8"):
    return string.decode(encoding=encoding)



