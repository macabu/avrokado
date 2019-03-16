import { ConsumerStream, createReadStream } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry/load-schemas';
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
  schemas: TopicsSchemas,
  readStream: CreateReadStream = createReadStream
) => {
  const consumerStream = readStream(
    consumerConfiguration,
    defaultTopicConfiguration,
    streamOptions
  );

  consumerStream.on('data', async (chunk: Chunk) => {
    if (schemas[chunk.topic]) {
      const valueSchema = schemas[chunk.topic].valueSchema;
      const keySchema = schemas[chunk.topic].keySchema;

      const {
        decoded: value,
        schemaId: valueSchemaId,
      } = <DecodedAvroChunk>decodeAvroChunk(valueSchema, chunk.value);

      const {
        decoded: key,
        schemaId: keySchemaId,
      } = <DecodedAvroChunk>decodeAvroChunk(keySchema, chunk.key);

      consumerStream.emit('avro', <KafkaMessage>{
        ...chunk,
        value,
        key,
        valueSchemaId,
        keySchemaId,
        rawValue: chunk.value,
        rawKey: chunk.key,
      });
    }
  });

  consumerStream.consumer.on(
    'event.error',
    (error: Error) => consumerStream.emit('event.error', error)
  );

  consumerStream.emit('ready');

  return consumerStream;
};
