import os

from dataclasses import dataclass
from typing import List

from src.mqttprocessor.loader import load_config
from src.mqttprocessor.routing import ProcessorCreator, Processor


@dataclass(frozen=True)
class EnvParameters:
    @dataclass(frozen=True)
    class Mqtt:
        host: str
        port: str
        username: str
        password: str

    mqtt: Mqtt
    config_file_path: str


def _load_env() -> EnvParameters:
    return EnvParameters(
        mqtt=EnvParameters.Mqtt(
            host=os.getenv("MQTT_HOST"),
            port=os.getenv("MQTT_PORT", 1883),
            username=os.getenv("MQTT_USERNAME"),
            password=os.getenv("MQTT_PASSWORD")
        ),
        config_file_path=os.getenv("CONFIG_FILE", "config.yaml")
    )


def _create_processors(config_file_path: str) -> List[Processor]:
    with open(config_file_path, "r") as f:
        config = load_config(f)

    return [ProcessorCreator(proc).create() for proc in config.processors]


def _create_mqtt_client(processors: List[Processor], mqtt_config: EnvParameters.Mqtt) -> ...:
    # TODO: create and connect MQTT client
    # TODO: decide how to receive, process and send messages
    pass


def run():
    env = _load_env()
    processors = _create_processors(env.config_file_path)
    mqtt = _create_mqtt_client(processors, env.mqtt)

    # TODO: run client infinitely
