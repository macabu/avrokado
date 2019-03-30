import { createReadStream } from 'node-rdkafka';

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
  value: Buffer;
  key: Buffer;
  size: number;
  topic: string;
  offset: number;
  partition: number;
  timestamp: number;
}

export interface AvrokadoMessage extends KafkaMessage {
  valueSchemaId: number;
  keySchemaId: number;
  parsedValue: JSON & string & number;
  parsedKey: JSON & string & number;
}

export const consumerStream = (
  consumerConfiguration: Object = {},
  defaultTopicConfiguration: Object = {},
  streamOptions: Object = {},
  schemas: TopicsSchemas
) => {
  const consumerStream = createReadStream(
    consumerConfiguration,
    defaultTopicConfiguration,
    streamOptions
  );

  consumerStream.on('data', async (chunk: Chunk) => {
    if (schemas[chunk.topic]) {
      const valueSchema = schemas[chunk.topic].valueSchema;
      const keySchema = schemas[chunk.topic].keySchema;

      const {
        decoded: parsedValue,
        schemaId: valueSchemaId,
      } = <DecodedAvroChunk>decodeAvroChunk(valueSchema, chunk.value);

      const {
        decoded: parsedKey,
        schemaId: keySchemaId,
      } = <DecodedAvroChunk>decodeAvroChunk(keySchema, chunk.key);

      consumerStream.emit('avro', <AvrokadoMessage>{
        ...chunk,
        parsedValue,
        parsedKey,
        valueSchemaId,
        keySchemaId,
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
