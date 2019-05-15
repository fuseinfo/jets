JETS
====

Jet Enhanced Transformation System (Jets) is a real-time data processing software, 
driven by configuration and code snippets for defining business rules.

Jets can use Kafka Streams or Apache Beam as the underlining engine to run the data flow.
Therefore, the same configuration can be re-used if the user decided to switch the execution engine
from/to Kafka Streams to/from Runners of Apache Beam.

Jets uses YAML as the main configuration format.
We can use YAML to define the data flow as well as data mappings. 
Jets uses Avro schema in JSON format to define record type after data transformation.


## Table of Contents ##

- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Run App](#run-app)
  + [Using Kafka Streams](#kafka)
  + [Using Apache Beam](#beam)
- [Advanced Topics](#advanced)
- [Examples](#examples)

## Installation ##

We can download the pre-built jar for Kafka Streams and Apache Beam.
In addition, we can download the pre-built jar of Jets-dashboard for monitoring and management.

Optionally, we may also use maven to build the jets project and use the generated jar-with-dependencies.
 

## Basic Usage ##

The main yaml file is either KafkaFlowBuilder.yaml to run on Kafka Streams or BeamFlowBuilder.yaml to run on Apache Beam.  
In the YAML file, we can specify a section of "DAG" to describe the data flow.
In the configuration YAML, we can either directly provide the value of a node, or specific a reference.
A reference is denoted as a child text node `$ref`. 
The value of the node can be an http(s) URL, a file loaded in the classpath, or a local file (Kafka Streams only).

### DAG ###

A streaming data processing consists 4 types of components: Source, Transformer, Sink and Store.
Each child section of the DAG is a component.  
The key of a child section is the name of the component.
Each message contains a value and optionally a key. 
All data used for transformation should be stored in the value portion. 
key can be used for partitioning when we use Kafka Streams.
All messages with the same key would be delivered to the same partition.
When we use Source to read from the partition, the messages with the same key will be processed by 
the same Kafka Streams instance. When we preserve state data using Kafka Streams, 
each instance can only read the state data from the same partition.
Key can be used for storing and retrieving state data.
 

### Source ###
The first component of a DAG is always a source, which indicates where the data come from.
When we use Kafka Streams, only KafkaSource is supported.  
To specify a source type, we can provide a property `_source` 
with the value as the class name in the package of either `com.fuseinfo.jets.kafka.source` or `com.fuseinfo.jets.beam.source`.
We should also provide a `valueParser` section to parse the message to Avro Generic Record for later processing.
If the source message contains a Key portion, we may provide a `keyParser` section.
Under the `valueParser` and `keyParser` section, we should provide a property of `_parser` with
the value as the class name of the parser under package `com.fuseinfo.jets.kafka.parser` or `com.fuseinfo.jets.beam.parser`.
All other properties will be supplied to the Parser class as the configuration.
When a parser is supplied, we should also provide `valueSchema`, and `keySchema` if `keyParser` was provided.
Other properties under the source section will be provided as the configuration to the Source class.

***Implemented Sources:***
* KafkaSource: Requires the following configuration items: `bootstrap.servers`, `group.id`, `topic`, 
* TextSource: Requires mandatory config: `path`

***Implemented Parsers:***
* JsonParser
* SchemaRegistryParser: Requires mandatory config `schema.registry.url`
* AvroParser


### Transformer ###

We can transform streaming data using transformers. We can provide a property `_function` 
with the value as the class name in the package of either `com.fuseinfo.jets.kafka.function` or `com.fuseinfo.jets.beam.function`.
All other properties will be provided as the configuration to this function.

***Implemented Transformers:***
* **ScalaKeyMapper**: This function would allow user to construct the key portion
* **ScalaPredicate**: This function would allow user to filter messages
* **ScalaTransformer**: This function would allow user to transform the value portion of the data. 
Stores can be used to support stateful transformation.

#### ScalaKeyMapper ####
***Parameters:***
* **schema**: The Avro Schema of the key portion after transformation
* **keyMapping**: Supply the mapping rules for fields of the output key.
All fields from the value portion of the input are declared as final variables.
We can use Scala code snippets to transform from the fields to construct each key field.


#### ScalaPredicate ####
***Parameters:***
* **test**: A function to return a boolean value to decide if the message should be passed to the next processor.

#### ScalaTransformer ####
***Parameters:***
* **schema**: The Avro Schema of the value portion after transformation
* **valueMapping**: Supply the mapping rules for fields of the output key.
All fields from the value portion of the input are declared as final variables.
We can use Scala code snippets to transform from the fields to construct each key field.
* stores: A list of stores to be used by this processor.
* timers: A Timer Function to run periodically.
* keyMapping: Define how to reconstruct the key portion
* timerMapping: Optionally set a unique key for RetryTimer


### Sink ###
We can use a Sink component to write transformed messages to a destination.
When we use Kafka Streams, only KafkaSink and NullSink are supported.  
To specify a sink type, we can provide a property `_sink`
with the value as the class name in the package of either `com.fuseinfo.jets.kafka.sink` or `com.fuseinfo.jets.beam.sink`.
We should also provide a `valueFormatter` section to format the message from Avro Generic Record to the desired format.
We can optionally provide a `keyFormatter` section to format the key portion.  
Under the `valueFormatter` and `keyFormatter` section, we should provide a property of `_formatter` with
the value as the class name of the formatter under package `com.fuseinfo.jets.kafka.formatter` or `com.fuseinfo.jets.beam.formatter`.
All other properties will be supplied to the Formatter class as the configuration.
Simiarlly, all other properties under the sink section will be provided as the configuration to the Sink class. 

***Implemented Sink:***
* KafkaSink: Requires the following configuration items: `bootstrap.servers`, `topic`
* TextSink: Requires mandatory config: `path`

***Implemented Formatters***
* JsonFormatter
* SchemaRegistryFormatter: Requires mandatory config `schema.registry.url`
* AvroFormatter

### Store ###


