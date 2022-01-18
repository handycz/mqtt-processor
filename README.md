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
  - Use `{w1}`, `{w2}`, ..., `{wNNN}` for single-level wildcard, i.e., `*` character. 
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


## Configuration examples
### Simple configuration
This configuration reads messages from `src/topic`, converts it by `my_processing_function` and sends 
it to `sink/topic`.
```yaml
processors:
  - source: src/topic
    sink: sink/topic
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
It is even possible to use wildcard source topics and use the values masked by wildcards in the sink topic.
In the example below, the app subscribes to topic `*/binary_temperature`. For message received from 
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
more than the single "message" argument, arguments from config file are passed to the function. So for function
`def is_in_between(message, lower_bound, upper_bound)`, the below config can be used to define two different filters
based on single rule function.
```yaml
processors:
  - source: device1/temperature
    sink: device1/filtered_temperature
    function: 
      name: is_in_between
      arguments:
        - 0 # lower bound
        - 10 # upper bound
  - source: device2/temperature
    sink: device2/filtered_temperature
    function: 
      name: is_in_between
      arguments:
        - -10 # lower bound
        -  25 # upper bound
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
          - 0
      - name: is_lower_than
        arguments:
          - 25
```


### Writing converters and rules
The Python functions
### Routed messages


### Creating runnable application
