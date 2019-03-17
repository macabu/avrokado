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
  producerConfiguration: Object,
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
  fallback: boolean = false,
  timestamp?: number,
  opaque?: Object
) => {
  let encodedValue = Buffer.alloc(0);
  let encodedKey = Buffer.alloc(0);

  if (schemas[topic]) {
    const valueSchema = schemas[topic].valueSchema;
    const keySchema = schemas[topic].keySchema;

    try {
      encodedValue = encodeAvroChunk(valueSchema, value);
      encodedKey = encodeAvroChunk(keySchema, key);
    } catch (error) {
      if (!fallback) {
        throw error;
      }
    }
  }

  let sendValue = encodedValue;
  if (!encodedValue.length) {
    sendValue = value
      ? Buffer.isBuffer(value)
        ? value
        : Buffer.from(JSON.stringify(value))
      : Buffer.alloc(0);
  }

  let sendKey = encodedKey;
  if (!encodedKey.length) {
    sendKey = key
      ? Buffer.isBuffer(key)
        ? key
        : Buffer.from(JSON.stringify(key))
      : Buffer.alloc(0);
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
