import { ProducerStream, createWriteStream } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry/load-schemas';
import { encodeAvroChunk } from '../schema-registry/avro-format';

interface KafkaProducerMessage {
  topic: string;
  partition: number;
  value: Buffer;
  key: Buffer;
  timestamp?: number;
  opaque?: Object;
}

export const DEFAULT_PARTITION = -1;

export const encodeWithSchema = (
  schemas: TopicsSchemas,
  topic: string,
  type: 'key' | 'value',
  data?: unknown,
  fallback?: boolean
) => {
  let encoded = Buffer.alloc(0);

  if (schemas && schemas[topic]) {
    try {
      const schema = type === 'key'
        ? schemas[topic].keySchema
        : schemas[topic].valueSchema;

      encoded = encodeAvroChunk(schema, data);
    } catch {
      // We swallow the error here, since if it fails
      // `encoded` will be unchanged, and we handle
      // that down below.
    }
  }

  let send = encoded;
  if (!encoded.length) {
    if (!fallback) {
      throw new TypeError('Schema not found to serialize data');
    }

    send = data
      ? Buffer.isBuffer(data)
        ? data
        : Buffer.from(JSON.stringify(data))
      : Buffer.alloc(0);
  }

  return send;
};

export const producerStream = (
  producerConfiguration: Object,
  defaultTopicConfiguration: Object = {},
  streamOptions: Object = {}
) => {
  const producerStream = createWriteStream(
    producerConfiguration,
    defaultTopicConfiguration,
    Object.assign(streamOptions, { objectMode: true })
  );

  return producerStream;
};

export const produce = (
  writeStream: ProducerStream,
  schemas: TopicsSchemas,
  topic: string,
  value?: unknown,
  key?: unknown,
  partition: number = DEFAULT_PARTITION,
  fallback: boolean = false,
  timestamp?: number,
  opaque?: Object
) => {
  const sendValue = encodeWithSchema(schemas, topic, 'value', value, fallback);
  const sendKey = encodeWithSchema(schemas, topic, 'key', key, fallback);

  const message = <KafkaProducerMessage>{
    topic,
    partition,
    value: sendValue,
    key: sendKey,
  };

  message.timestamp = timestamp || Date.now();

  if (opaque) {
    message.opaque = opaque;
  }

  const sent = writeStream.write(message);

  return sent;
};
