# Avrokado

:avocado: A Kafka client and Avro (de)serializer library  
  
[![npm](https://img.shields.io/npm/v/avrokado.svg?style=flat)](https://www.npmjs.org/package/avrokado)
![CircleCI](https://img.shields.io/circleci/project/github/macabu/avrokado.svg?style=flat)
![GitHub](https://img.shields.io/github/license/macabu/avrokado.svg?style=flat)

---

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
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

## Usage
There are two functions currently exported by the package:  

- [loadSchemas](#loadSchemas)
- [consumerStream](#consumerStream)

### loadSchemas
This will fetch the `key` and `value` schemas for a `topicName`.    
#### Function Signature
```js
async loadSchemas(
  schemaRegistryEndpoint,
  topicName,
  schemaVersions
): Map<string, Map<number, SchemaMap>>;
```
Where:
- **schemaRegistryEndpoint**: Endpoint for your Schema Registry;
- **topicName**: Name of the topic you want to retrieve the schemas for;
- **schemaVersions**: It can be either:
  - A `number`, which will then force the function to only fetch that version;
  - `all`, which means it will fetch `all` versions of the schemas;
  - `latest`, which will fetch only the `latest` schema versions.
  
Returns a `Map<string, Map<number, SchemaMap>>`, and the keys to the first `Map` will be either `key` or `value`.  
  
The keys of `Map<number, SchemaMap>` are the IDs of the schemas in question.  

SchemaMap definition:
```ts
export interface SchemaMap {
  version: number;
  schema: Type;
}
```
Where `Type` comes from [avsc](https://github.com/mtth/avsc) (it's basically the type used to define an Avro schema from an `Object`).
  
#### Example
```js
import { loadSchemas } from 'avrokado';

(async () => {
  const sr = 'http://schema-registry:8081';
  const topic = 'my-great-topic';
  const version = 'latest';

  const schemas = await loadSchemas(sr, topic, version);
})();
```

#### Best Practices
It is recommended to load the schemas **BEFORE** creating your Consumer or Producer.
  
### consumerStream
This will create a consumer stream using [node-rdkafka](https://github.com/Blizzard/node-rdkafka).  
  
Please check their [**DOCUMENTATION**](https://github.com/Blizzard/node-rdkafka) since most of the options are from this library.

#### Function Signature
```js
consumerStream(
  consumerConfiguration: Object = {},
  defaultTopicConfiguration: Object = {},
  streamOptions: Object = {},
  schemas: Map<string, Map<number, SchemaMap>>,
  readStream: CreateReadStream = createReadStream
): ConsumerStream;
```
Where:
- **consumerConfiguration**: `librdkafka`'s consumer-specific configuration;
- **defaultTopicConfiguration**: `librdkafka`'s default topic configuration;
- **streamOptions**: `librdkafka`'s read stream options;
- **schemas**: A `Map` containing a `Map` of all `key` and `value` schemas (return from `loadSchemas`);
- **readStream?**: The actual `librdkafka` `createReadStream` function. Optional.

Returns a `ConsumerStream`, which extends from `Readable` stream.

#### Events Emitted
- `avro`: Whenever a message is parsed with Avro;
- `ready`: When the Consumer Stream is created;
- `event.error`: Wraps `ConsumerStream.consumer`'s `event.error` event;
- any other event emitted by a `ConsumerStream` from `node-rdkafka`.
  
#### API
Specifically for `avro` event emitted, it should be expected a `KafkaMessage` type, which contains:
- rawValue: The raw value buffer;
- rawKey: The raw key buffer;
- size: Size in bytes of the original message;
- topic: Name of the topic;
- offset: Offset in which the message is;
- partition: Partition from the topic;
- timestamp: WHen the message was retrieved;
- valueSchemaId: Schema ID for the value;
- keySchemaId: Schema ID for the key;
- value: Avro-deserialized value (from rawValue);
- key: Avro-deserialized key (from rawKey).
  
#### Example
```js
import { loadSchemas consumerStream, KafkaMessage } from 'avrokado';

const consumerOpts = {
  'metadata.broker.list': 'kafka:9092',
  'group.id': 'my-group-id',
  'socket.nagle.disable': true,
  'socket.keepalive.enable': true,
  'enable.auto.commit': false,
  'enable.auto.offset.store': true,
  'log.connection.close': false,
};

const consumerOffset = {
  'auto.offset.reset': 'earliest',
};

const streamOptions = {
  topics: ['simple-consumer-topic'],
};

(async () => {
  const schemas = await loadSchemas(
    'schema-registry:8081',
    'simple-consumer-topic',
    'latest'
  );

  const stream = consumerStream(
    consumerOpts,
    consumerOffset,
    streamOptions,
    schemas
  );

  stream.on('avro', (data: KafkaMessage) => {
    console.log(`Received Message! (Offset: ${data.offset})`);
    console.log(`Value: ${data.value}`);
    console.log(`Key: ${data.key}`);

    stream.consumer.commitMessage(data);
  });
})();
```

## Tests
To run tests, you can run `npm test` or `yarn test`.

## TODO
- Producer wrapper;
- Writing tests for Consumer.
