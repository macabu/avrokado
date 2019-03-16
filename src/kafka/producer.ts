import { ProducerStream, createWriteStream } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry/load-schemas';
import { encodeAvroChunk } from '../schema-registry/avro-format';

type CreateWriteStream = (conf: Object, topicConf: Object, streamOptions: Object) => ProducerStream;
type TopicName = string;

interface KafkaProducerMessage {
  topic: TopicName;
  partition: number;
  value: Buffer;
  key: Buffer;
  timestamp?: number;
  opaque?: Object;
}

export const DEFAULT_PARTITION = -1;

export const producerStream = (
  producerConfiguration: Object = {},
  defaultTopicConfiguration: Object = {},
  streamOptions: Object = {},
  writeStream: CreateWriteStream = createWriteStream
) => {
  const producerStream = writeStream(
    producerConfiguration,
    defaultTopicConfiguration,
    Object.assign(streamOptions, { objectMode: true })
  );

  return producerStream;
};

export const produce = (
  writeStream: ProducerStream,
  schemas: TopicsSchemas,
  topic: TopicName,
  value?: unknown,
  key?: unknown,
  partition: number = DEFAULT_PARTITION,
  timestamp?: number,
  opaque?: Object
) => {
  let encodedValue = Buffer.allocUnsafe(0);
  let encodedKey = Buffer.allocUnsafe(0);

  if (schemas[topic]) {
    const valueSchema = schemas[topic].valueSchema;
    const keySchema = schemas[topic].keySchema;

    encodedValue = encodeAvroChunk(valueSchema, value);
    encodedKey = encodeAvroChunk(keySchema, key);
  }

  let sendValue = <unknown>encodedValue;
  if (!encodedValue.length) {
    sendValue = value;
    if (value) {
      sendValue = Buffer.isBuffer(value)
        ? value
        : Buffer.from(JSON.stringify(value));
    }
  }

  let sendKey = <unknown>encodedKey;
  if (!encodedKey.length) {
    sendKey = key;
    if (key) {
      sendKey = Buffer.isBuffer(key)
        ? key
        : Buffer.from(JSON.stringify(key));
    }
  }

  const message = <KafkaProducerMessage>{
    topic,
    partition,
    value: sendValue,
    key: sendKey,
  };

  if (timestamp) {
    message.timestamp = Date.now();
  }

  if (opaque) {
    message.opaque = opaque;
  }

  const sent = writeStream.write(message);

  return sent;
};
