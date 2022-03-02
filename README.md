# MQTT Processor
The library serves to create an app that change or filters MQTT messages with ease. 

It consists of YAML configuration file and rules and converters, functions denominated by Python decorators.

## Installation
The library currently requires [Poetry](https://github.com/python-poetry/poetry). With Poetry installed, it can be 
installed by running `poetry add git+https://github.com/handycz/mqtt-processor`.

## Working principles
- Configuration file in YAML format consists of list of processors.
- Each processor is required to have one or more source topics (i.e, where do we receive messages).
- Each processor may have fixed sink topic (i.e., where to send processed messages). If it has not, the topic 
  is dynamically defined by individual processing functions.
- Source topics can use wildcards. Values masked by wildcards can be used in sink topics. Example
  [here](#wildcard-routing)
  - Use `{w1}`, `{w2}`, ..., `{wNNN}` for single-level wildcard, i.e., `+` character. 
  - Use `{W1}`, `{W2}`, ..., `{WNNN}` for multi-level wildcard, i.e., `#` character.
  - In the sink topic, the same wildcard can be used to insert masked value.
- Every processor consists of one or more functions. Example [here](#chained-configuration)
  - The functions are Python functions denominated by appropriate decorators. 
    Example [here](#writing-converters-and-rules)  
  - The function can be either a rule or converter.
  - Functions can be chained together.
  - Rules are functions returning a boolean based on message contents. If the result is true, the message is passed 
    to next function in the processor. 
  - Converters are functions that produce an outgoing message based on and ingoing message. 
    - A converter can return multiple messages, that can be sent to single or several sink topics. These are called 
    routed messages.
- Input messages can be automatically formatted from `bytes` to `string` (utf8 encoding) or `json`. 
  Example [here](#simple-configuration). Output messages are automatically encoded from `string` and `json`.

## Configuration examples
### Simple configuration
This configuration reads messages from `src/topic`, converts it by `my_processing_function` and sends 
it to `sink/topic`.
```yaml
processors:
  - source: src/topic
    sink: sink/topic
    input_format: json # possible values: binary, string, json (default - json)
    function: my_processing_function
  - name: my-processor # you can optionally specify processor's name 
    source: device1/raw
    sink: device1/humanreadable
    function: raw_to_human_readble
```

### Chained configuration
Here, we receive messages from multiple topics and process it by two chained functions. First the message is converted
by `convert_from_binary`, then the value produced by that function is processed by `convert_to_degrees`. 

Notice the missing `sink:` key. This means there is no default sink topic, thus the last function 
in the chain has to produce a routed message that has the sink topic embedded. 
```yaml
processors:
  - source: 
      - device1/binary_temperature
      - device2/binary_temperature
    function: 
      - convert_from_binary
      - convert_to_degrees
```

### Wildcard routing
It is possible to use wildcard source topics and use the values masked by wildcards in the sink topic.
In the example below, the app subscribes to topic `+/binary_temperature`. For message received from 
`device123/binary_temperature` it sends the processed message to `device123/formatted_temperature`. The same can be 
used with topics given by routed messages.
```yaml
processors:
  - source: {w1}/binary_temperature
    sink: {w1}/formatted_temperature
    function:
      ... # omitted for simplicity
```

### Function arguments
To allow definition of generalized functions, it is possible to supply constant arguments. If the rule defined has
more than the single "message" argument, arguments from config file are passed to the function as kwargs. So for function
`def is_in_between(message, lower_bound, upper_bound)`, the below config can be used to define two different filters
based on single rule function.
```yaml
processors:
  - source: device1/temperature
    sink: device1/filtered_temperature
    function: 
      name: is_in_between
      arguments:
        lower_bound: 0 # lower bound
        upper_bound: 10 # upper bound
  - source: device2/temperature
    sink: device2/filtered_temperature
    function: 
      name: is_in_between
      arguments:
        lower_bound: -10 # lower bound
        upper_bound:  25 # upper bound
```

Of course, this can be also used when chaining functions, so with functions `def is_greater_than(message, bound)` and
`def is_lower_than(message, bound)` we can achieve similar behavior by the example below.
```yaml
processors:
  - source: device1/temperature
    sink: device1/filtered_temperature
    function:
      - name: is_greater_than
        arguments:
          bound: 0
      - name: is_lower_than
        arguments:
          bound: 25
```

## Writing converters and rules
The functions can be implemented by standard python functions taking at least one argument. Functions have to be
decorated by either `@rule` or `@converter`. Then, the function can be addressed in the YAML file by its name, or by 
name override given by `@rule(name="my_rule")` or `@converter(name="my_converter")`. 

Any of these functions may accept more than one parameter. Values from `arguments` structure in the YAML
config are then passed to the function.

### Rule
Rule function is expected to return boolean. For example:
```python
from mqttprocessor.functions import rule

@rule
def temp_greater_than(message, bound):
  return int(message['temperature']) > bound
```


### Converter
The converter can either return `bytes`, `str`, a structure serializable to `json` or a `RoutedMessage` consisting 
of the other three data formats. For example:

```python
from mqttprocessor.functions import converter

@converter
def temp_kelvin_to_celsius(message):
  message['temperature'] = message['temperature'] - 273.15
  return message
```

Or for a `RoutedMessage`, where a message is split to two messages and sent to two separate topics
`device1/temperature` and `device1/pressure`.

```python
from mqttprocessor.functions import converter
from mqttprocessor.messages import routedmessage

@converter
def decode_binary(message: bytes):
  
  return routedmessage({
    "device1/temperature": str(message[0]),
    "device1/pressure": str(message[1]),
  })
```

### Special parameters
Every rule or converter can be passed the source topic of the message and wildcard matches just by adding `source_topic` and/or `matches` parameters to the respective function implementation. So, for example, you could use function with `def convert_temperature(original_temp: float, source_topic: str)` signature to access name of the topic the message was delivered to or `def convert_temperature(original_temp: float, source_topic: str, matches: Dict[str, Any])` to access the topic and the wildcard matches (if any). Arguments defined in the yaml file can be used as usual. 


### Routed messages
Routed messages allow you to send one or more messages to one or more topics. Routed messages are of type `list`, `dict`
or `tuple` and wrapped by `routedmessage()`. The object is then split to individual messages with different sink topics.
In case of list, where the topic is not given by the routed message, default topic from the configuration file is used.
 
#### Multiple messages to single topic
If it's needed to send _multiple messages to the default topic_, `list` of messages can be used. Here, the sink topic 
is given by the configuration file. Snippet below produces three separate messages sent to the default sink.
```python
from mqttprocessor.functions import converter
from mqttprocessor.messages import routedmessage

@converter
def demo_converter(message):
    body_of_message_1 = ...
    body_of_message_2 = ...
    body_of_message_3 = ...
  
    return routedmessage([
        body_of_message_1, body_of_message_2, body_of_message_3
    ])
```

#### Single message to single topic
To send a _single message to a non-default topic_, `tuple` consisting of topic name and message itself can be used.
Function produces a message that is sent to `destination/topic`.
```python
from mqttprocessor.functions import converter
from mqttprocessor.messages import routedmessage

@converter
def demo_converter(message):
    body_of_message = ...
  
    return routedmessage((
      "destination/topic", body_of_message
    ))
```

#### Multiple messages to multiple unique topics
When there is a need to send _multiple messages to multiple unique topics_, `dict` can be used. Following function
produces three messages, each sent to one of topics `topic1`, `topic2`, `topic3`.
```python
from mqttprocessor.functions import converter
from mqttprocessor.messages import routedmessage

@converter
def demo_converter(message):
    body_of_message_1 = ...
    body_of_message_2 = ...
    body_of_message_3 = ...
  
    return routedmessage({
      "topic1": body_of_message_1,
      "topic2": body_of_message_2,
      "topic3": body_of_message_3
    })
```
#### Nested routed messages
The body of the message can be another `routedmessage` object. If the topic is not given by the routed
message, it is inherited from a parent. 

#### Multiple messages to non-unique topics 
Because `routedmessages` can be nested, we can send multiple messages to different topics as a `dict` of `routedmessage`
or as a `list` of `tuple`. This would produce three messages. Message one and two would be sent to `topic1`, while
third message would be sent to `topic10`.

```python
from mqttprocessor.functions import converter
from mqttprocessor.messages import routedmessage

@converter
def demo_converter(message):
    body_of_message_1 = ...
    body_of_message_2 = ...
    body_of_message_3 = ...
  
    return routedmessage({
      "topic1": routedmessage([
        body_of_message_1,
        body_of_message_2
      ]),
      "topic10": body_of_message_3,
    })
```

In similar way, one could achieve the same result by using the snippet consisting of list of tuples:
```python
from mqttprocessor.functions import converter
from mqttprocessor.messages import routedmessage

@converter
def demo_converter(message):
    body_of_message_1 = ...
    body_of_message_2 = ...
    body_of_message_3 = ...
  
    return routedmessage([
      routedmessage(("topic1", body_of_message_1)),
      routedmessage(("topic1", body_of_message_2)),
      routedmessage(("topic10", body_of_message_3))
    ])
```

#### Wildcards
Wildcards can be used in the same way as when writing rules in the configuration file. If the application subscribes to
topic `devices/{w1}/values/{w2}` and message arrives to `devices/deviceA/values/temperature`, a routed message
given by`routedmessage(("values/{w2}"))` would be routed to `values/temperature`.

### Creating runnable application
To create a simple app, it is necessary to define the rules and converters and call `run()` function from 
`from mqttprocessor.app import run` at the bottom of the file. Parameters of the application are passed by environmental
variables.

| Name           | Default                            | Description                        |
|----------------|------------------------------------|------------------------------------|
| CONFIG_FILE    | `config.yaml`                      | Path to the configuration file     |
| MQTT_HOST      | Required                           | Hostname or IP of the MQTT broker  |
| MQTT_PORT      | 1883                               | Port of the MQTT broker            |
| MQTT_USERNAME  | Ignored if empty                   | Username to access the MQTT broker | 
| MQTT_PASSWORD  | Ignored if empty                   | Password to access the MQTT broker |  
 | MQTT_CLIENT_ID | `MqttProcessor-{randint(0, 1000)}` | MQTT client ID                     |
