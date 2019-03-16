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
- [producerStream](#producerStream)
- [produce](#produce)

### loadSchemas
This will fetch the `key` and `value` schemas for a `topicName`.

#### Function Signature
```js
async loadSchemas (
  schemaRegistryEndpoint: SchemaRegistryEndpoint,
  topics: TopicName[] | TopicName,
  schemaVersions: SchemaVersionsRequested
) => TopicsSchemas;
```
Where:
- **schemaRegistryEndpoint**: Endpoint for your Schema Registry;
- **topics**: Name of the topics (`Array`) or topic (`String`) you want to retrieve the schemas for;
- **schemaVersions**: It can be either:
  - A `number`, which will then force the function to only fetch that version;
  - `all`, which means it will fetch `all` versions of the schemas;
  - `latest`, which will fetch only the `latest` schema versions.
  
Returns all the schemas fetched (`TopicsSchemas`), that have the format:
```js
type TopicsSchemas = { [topicName: string]: Schemas };
```

Where `Schemas` is an interface that contains the `value` and `key` schemas:
```js
interface Schemas {
  valueSchema: TypeSchemas;
  keySchema: TypeSchemas;
}
```

And where `TypeSchemas` is an object of `SchemaObject`s:
```js
type TypeSchemas = { [schemaId: number]: SchemaObject };
```

SchemaObject definition:
```ts
export interface SchemaObject {
  version: number;
  schema: Type;
}
```
Where `Type` comes from [avsc](https://github.com/mtth/avsc) (it's basically the type used to define an Avro schema from a JavaScript `Object`).
  
#### Example
```js
import { loadSchemas } from 'avrokado';

(async () => {
  const sr = 'http://schema-registry:8081';
  const topic = ['my-great-topic', 'my-so-so-topic'];
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
  schemas: TopicsSchemas,
  readStream: CreateReadStream = createReadStream
): ConsumerStream;
```
Where:
- **consumerConfiguration**: `librdkafka`'s consumer-specific configuration;
- **defaultTopicConfiguration**: `librdkafka`'s default topic configuration;
- **streamOptions**: `librdkafka`'s read stream options;
- **schemas**: An object with all `key` and `value` schemas (return from `loadSchemas`);
- **readStream?**: The actual `librdkafka` `createReadStream` function. Will be created if not specified.

Returns a `ConsumerStream`, which extends from `Readable` stream.

#### Events Emitted
| Event name    | Trigger/Description                                   |
|---------------|-------------------------------------------------------|
| `avro`        | Whenever a message is parsed with Avro                |
| `ready`       | When the Consumer Stream is created                   |
| `event.error` | Wraps `ConsumerStream.consumer`'s `event.error` event |  

And any other event emitted by a `ConsumerStream` from `node-rdkafka`.
  
#### API
Specifically for `avro` event emitted, it should be expected a `KafkaMessage` type, which contains:  

| Variable        | Description                             |
|-----------------|-----------------------------------------|
| `rawValue`      | The raw value buffer                    |
| `rawKey`        | The raw key buffer                      |
| `size`          | Size in bytes of the raw message        |
| `topic`         | Name of the topic                       |
| `offset`        | Offset in which the message is          |
| `partition`     | Partition from the topic                |
| `timestamp`     | When the message was retrieved          |
| `valueSchemaId` | Schema ID for the value                 |
| `keySchemaId`   | Schema ID for the key                   |
| `value`         | Avro-deserialized value (from rawValue) |
| `key`           | Avro-deserialized key (from rawKey)     |  
  
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

### producerStream
This will create a producer stream using [node-rdkafka](https://github.com/Blizzard/node-rdkafka).  
  
Please check their [**DOCUMENTATION**](https://github.com/Blizzard/node-rdkafka) since most of the options are from this library.

#### Function Signature
```js
producerStream = (
  producerConfiguration: Object = {},
  defaultTopicConfiguration: Object = {},
  streamOptions: Object = {},
  writeStream: CreateWriteStream = createWriteStream
) => ProducerStream;
```
Where:
- **producerConfiguration**: `librdkafka`'s producer-specific configuration;
- **defaultTopicConfiguration**: `librdkafka`'s default topic configuration;
- **streamOptions**: `librdkafka`'s read stream options;
- **writeStream?**: The actual `librdkafka` `createWriteStream` function. Will be created if not specified.

Returns a `ProducerStream`, which extends from `Writable` stream.

#### Events Emitted
| Event name    | Trigger/Description                                   |
|---------------|-------------------------------------------------------|
| `ready`       | When the Producer Stream is created                   |

And any other event emitted by a `ProducerStream` from `node-rdkafka`.

#### Example
```js
import { loadSchemas, producerStream } from 'avrokado';

const producerOpts = {
  'metadata.broker.list': 'kafka:9092',
  'socket.nagle.disable': true,
  'socket.keepalive.enable': true,
  'log.connection.close': false,
};

(async () => {
  const schemas = await loadSchemas(
    'schema-registry:8081',
    'simple-producer-topic',
    'latest'
  );

  const stream = producerStream(producerOpts, {}, {});
})();
```

### produce
This will produce a message to Kafka using a `ProducerStream`, with the `value` and `key` encoded with `Avro`.  

#### Function Signature
```js
produce = (
  writeStream: ProducerStream,
  schemas: TopicsSchemas,
  topic: TopicName,
  value?: unknown,
  key?: unknown,
  partition: number = DEFAULT_PARTITION,
  timestamp?: number,
  opaque?: Object
) => boolean;
```
Where:
- **writeStream**: The actual `librdkafka` `createWriteStream` function. This is needed, since `avrokado` doesn't keep global state of created objects;
- **schemas**: An object with all `key` and `value` schemas (return from `loadSchemas`);
- **topic**: Name of the topic to which the message will be produced to;
- **value?**: Value for the Kafka message. If `null`, will not be serialized;
- **key?**: Key for the Kafka message. If `null`, will not be serialized;
- **partition?**: The topic partition to which the message will be produced to. If not sent, `DEFAULT_PARTITION` (`-1`) will be used;
- **timestamp?**: Timestamp for the creation of the message. If not sent, will default to `now`;
- **opaque?**: Additional object or any data to be sent with the message. If not sent, will be `null`.

If the `key` or `value` fail serialization (i.e. schema not found), their raw data will be sent instead.

Returns a `true` in case the message has been written to the stream. `false` in case of any errors.

#### Example
```js
import { loadSchemas, producerStream } from 'avrokado';

const producerOpts = {
  'metadata.broker.list': 'kafka:9092',
  'socket.nagle.disable': true,
  'socket.keepalive.enable': true,
  'log.connection.close': false,
};

(async () => {
  const schemas = await loadSchemas(
    'schema-registry:8081',
    'simple-producer-topic',
    'latest'
  );

  const stream = producerStream(producerOpts, {}, {});

  produce(
    stream,
    schemas,
    'simple-producer-topic',
    value,
    key,
    DEFAULT_PARTITION
  );

  stream.close();
})();
```

## Tests
To run tests, you can run `npm test` or `yarn test`.

## TODO
- Add standard Producer.  
- Remove `axios` dependency.
- Improve in-code documentation.
- Write tests for Producer.
- Write tests for Consumer.
