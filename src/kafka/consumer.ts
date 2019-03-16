import { ConsumerStream, createReadStream } from 'node-rdkafka';

import { SchemaObject } from '../schema-registry/load-schemas';
import { decodeAvroChunk, DecodedAvroChunk } from '../schema-registry/avro-format';

interface Chunk {
  value: Buffer;
  key: Buffer;
  size: number;
  topic: string;
  offset: number;
  partition: number;
  timestamp: number;
}

export interface KafkaMessage {
  rawValue: Buffer;
  rawKey: Buffer;
  size: number;
  topic: string;
  offset: number;
  partition: number;
  timestamp: number;
  valueSchemaId: number;
  keySchemaId: number;
  value: JSON & string & number;
  key: JSON & string & number;
}

type CreateReadStream = (conf: Object, topicConf: Object, streamOptions: Object) => ConsumerStream;

export const consumerStream = (
  consumerConfiguration: Object = {},
  defaultTopicConfiguration: Object = {},
  streamOptions: Object = {},
  schemas: Map<string, SchemaObject[]>,
  readStream: CreateReadStream = createReadStream
) => {
  const consumerStream = readStream(
    consumerConfiguration,
    defaultTopicConfiguration,
    streamOptions
  );

  consumerStream.on('data', async (chunk: Chunk) => {
    const {
      decoded: value,
      schemaId: valueSchemaId,
    } = <DecodedAvroChunk>decodeAvroChunk(schemas, 'value', chunk.value);

    const {
      decoded: key,
      schemaId: keySchemaId,
    } = <DecodedAvroChunk>decodeAvroChunk(schemas, 'key', chunk.key);

    consumerStream.emit('avro', <KafkaMessage>{
      ...chunk,
      value,
      key,
      valueSchemaId,
      keySchemaId,
      rawValue: chunk.value,
      rawKey: chunk.key,
    });
  });

  consumerStream.consumer.on(
    'event.error',
    (error: Error) => consumerStream.emit('event.error', error)
  );

  consumerStream.emit('ready');

  return consumerStream;
};
