import logging

from mqttprocessor.app import run
from mqttprocessor.functions import converter


logging.basicConfig(level=logging.DEBUG)


@converter
def dostuff(x):
    x['val'] = x['val'] * 10

    return x


run()
