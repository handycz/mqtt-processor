import logging

from src.mqttprocessor.app import run
from src.mqttprocessor.functions import converter


logging.basicConfig(level=logging.DEBUG)


@converter
def dostuff(x):
    x['val'] = x['val'] * 10

    return x


run()
