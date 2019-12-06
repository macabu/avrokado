# Avrokado

:avocado: A Kafka client and Avro (de)serializer library  

[![npm](https://img.shields.io/npm/v/avrokado.svg?style=flat)](https://www.npmjs.org/package/avrokado)
[![codecov](https://codecov.io/gh/macabu/avrokado/branch/master/graph/badge.svg)](https://codecov.io/gh/macabu/avrokado)
[![CircleCI](https://img.shields.io/circleci/project/github/macabu/avrokado.svg?style=flat)](https://circleci.com/gh/macabu/avrokado)
[![GitHub](https://img.shields.io/github/license/macabu/avrokado.svg?style=flat)](https://github.com/macabu/avrokado/blob/master/LICENSE)

---

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
  - [SchemaRegistry](#SchemaRegistry)
  - [AvroConsumer](#AvroConsumer)
  - [AvroProducer](#AvroProducer)
- [Tests](#tests)
- [TODO](#TODO)

---

## Installation
To install, use your favourite dependency manager.  
The package name is `avrokado`.
```sh
npm i avrokado --save

yarn add avrokado --exact
```

---

## Usage
For examples, please refer to the [examples folder](examples/).

### SchemaRegistry
This will fetch the `key` and `value` schemas for a `topicName`.

#### Class Signature
```js
new SchemaRegistry (
  endpoint: string,
  topics: ReadonlyArray<string> | string,
  version: number | 'latest' | 'all'
) => SchemaRegistry;
```
Where:
- **endpoint**: Endpoint for your Schema Registry;
- **topics**: Name of the topics (`Array`) or topic (`String`) you want to retrieve the schemas for;
- **version**: It can be either:
  - A `number`, which will then force the function to only fetch that version;
  - `all`, which means it will fetch `all` versions of the schemas;
  - `latest`, which will fetch only the `latest` schema versions.

#### Fields
- **schemas**: Object containing the loaded schemas. 

#### Methods

**load**
```js
async load() => Promise<void>;
```
The `load` method will load all the schemas selected to memory, and can be accessed through the `schemas` field from the instanced class. 

#### Best Practices
It is recommended to load the schemas **BEFORE** creating your Consumer or Producer.

---

### AvroConsumer
This will create a consumer stream using [node-rdkafka](https://github.com/Blizzard/node-rdkafka).  
  
Please check their [**DOCUMENTATION**](https://github.com/Blizzard/node-rdkafka) since most of the options are from this library.

#### Class Signature
```js
new AvroConsumer(
  conf: Object,
  topicConf: Object,
  schemas: TopicsSchemas
) => AvroConsumer;
```
Where:
- **consumerConfiguration**: `librdkafka`'s consumer-specific configuration;
- **defaultTopicConfiguration**: `librdkafka`'s default topic configuration;
- **streamOpts**: `librdkafka`'s read stream options;
- **schemas**: An object with all `key` and `value` schemas (return from `loadSchemas`).

Returns a `AvroConsumer`, which extends from `Readable` stream.

#### Fields
- **stream**: This is a `ConsumerStream` object from `node-rdkafka`, which has another field `consumer` for the `KafkaConsumer` itself (yes it's ugly).

#### Events Emitted
| Event name    | Trigger/Description                                   |
|---------------|-------------------------------------------------------|
| `avro`        | Whenever a message is parsed with Avro                |
| `ready`       | When the Consumer Stream is created                   |
| `event.error` | Wraps `ConsumerStream.consumer`'s `event.error` event |

And any other event emitted by a `ConsumerStream` from `node-rdkafka`.
  
#### API
Specifically for `avro` event emitted, it should be expected a `AvroMessage` type, which contains:  

| Variable        | Description                             |
|-----------------|-----------------------------------------|
| `value`         | The raw value buffer                    |
| `key`           | The raw key buffer                      |
| `size`          | Size in bytes of the raw message        |
| `topic`         | Name of the topic                       |
| `offset`        | Offset in which the message is          |
| `partition`     | Partition from the topic                |
| `timestamp`     | When the message was retrieved          |
| `valueSchemaId` | Schema ID for the value                 |
| `keySchemaId`   | Schema ID for the key                   |
| `parsedValue`   | Avro-deserialized value (from value)    |
| `parsedKey`     | Avro-deserialized key (from key)        |  

#### Notes
- To use the `KafkaConsumer` methods, for now you will need to do `AvroConsumer.stream.consumer`.

---

### AvroProducer
This will create a producer using [node-rdkafka](https://github.com/Blizzard/node-rdkafka).  
  
Please check their [**DOCUMENTATION**](https://github.com/Blizzard/node-rdkafka) since most of the options are from this library.

#### Class Signature
```js
new AvroProducer(
  conf: Object,
  topicConf: Object,
  schemas: TopicsSchemas
) => AvroProducer;
```
Where:
- **conf**: `librdkafka`'s producer-specific configuration;
- **topicConf?**: `librdkafka`'s default topic configuration;
- **schemas**: An object with all `key` and `value` schemas (return from `loadSchemas`).

Returns a `AvroProducer`, which extends from `Producer`.   

#### Methods

**connect**
```js
connect(
  metadataOption: Object = {}
) => Promise<true | Error>;
```
The `connect` method will connect to the Kafka broker and `await` until a connection is successfully made or an error is thrown.  
  
**produce**
```js
produce(
  topic: string,
  partition?: number,
  message?: unknown,
  key?: unknown,
  sendRaw?: boolean,
  timestamp?: number,
  opaque?: unknown
) => void;
```
The `produce` method will produce a message to Kafka. If `sendRaw` is set to `true`, the message WILL NOT be avro encoded.
  
**disconnect**
```js
disconnect(
  timeout: number = 5000
) => Promise<true | Error>;
```
The `disconnect` method will disconnect from the Kafka broker and `await` until it is gracefully interrupted.

---

## Tests
1. Install `Docker`;
2. Install `docker-compose`;
3. Start up the images with `docker-compose up -d` and make sure zookeeper, kafka and schema-registry are all running;
4. Run `npm run test` or `yarn test`.

---

## TODO
- Improve in-code documentation.
