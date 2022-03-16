import logging
import os
import random

from dataclasses import dataclass
from queue import SimpleQueue
from typing import List

from paho.mqtt.client import Client, MQTTMessage

from .loader import load_config
from .messages import Message
from .routing import ProcessorCreator, Processor

_logger: logging.Logger = logging.getLogger(__name__)
_ingress_queue: SimpleQueue[MQTTMessage] = SimpleQueue[MQTTMessage]()


@dataclass(frozen=True)
class EnvParameters:
    @dataclass(frozen=True)
    class Mqtt:
        client_id: str
        host: str
        port: str
        username: str
        password: str

    mqtt: Mqtt
    config_file_path: str
    log_level: str


def _load_env() -> EnvParameters:
    return EnvParameters(
        mqtt=EnvParameters.Mqtt(
            client_id=os.getenv(
                "MQTT_CLIENT_ID", f"MqttProcessor-{random.randint(0, 1000)}"
            ),
            host=os.getenv("MQTT_HOST"),
            port=os.getenv("MQTT_PORT", 1883),
            username=os.getenv("MQTT_USERNAME"),
            password=os.getenv("MQTT_PASSWORD"),
        ),
        config_file_path=os.getenv("CONFIG_FILE", "config.yaml"),
        log_level=os.getenv("LOG_LEVEL", "WARNING").upper()
    )


def _create_processors(config_file_path: str) -> List[Processor]:
    with open(config_file_path, "r") as f:
        config = load_config(f)

    return [ProcessorCreator(proc).create() for proc in config.processors]


def _create_mqtt_client(
    processors: List[Processor], mqtt_config: EnvParameters.Mqtt
) -> Client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            _logger.info("MQTT client connected!")
        else:
            _logger.error("MQTT client connection failed with code %s", rc)

    def on_message(client, userdata, message: MQTTMessage):
        _ingress_queue.put(message)

    client = Client(mqtt_config.client_id)
    if mqtt_config.username is not None and mqtt_config.password is not None:
        client.username_pw_set(mqtt_config.username, mqtt_config.password)

    client.on_connect = on_connect
    client.connect(mqtt_config.host, mqtt_config.port)
    client.on_message = on_message

    for processor in processors:
        for topic in processor.source_topics:
            client.subscribe(topic.convert_rule_to_mqtt_format())

    client.loop_start()

    return client


def _process_messages(processors: List[Processor], mqtt_client: Client):
    output_messages: List[Message] = list()

    while True:
        received_message = _ingress_queue.get()
        _logger.debug("Received message at %s", received_message.topic)

        for processor in processors:
            output_messages += processor.process_message(
                received_message.topic, received_message.payload
            )

        while len(output_messages) > 0:
            msg = output_messages.pop()
            _logger.debug("Sending message to %s", msg.sink_topic.rule)

            mqtt_client.publish(
                msg.sink_topic.rule,
                msg.message_body,
                qos=received_message.qos,
                retain=received_message.retain,
            )


def run():
    env = _load_env()
    logging.basicConfig(level=logging.getLevelName(env.log_level))
    processors = _create_processors(env.config_file_path)
    mqtt = _create_mqtt_client(processors, env.mqtt)
    _process_messages(processors, mqtt)
