import { createReadStream } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry/load-schemas';
import { decodeAvroChunk, DecodedAvroChunk } from '../schema-registry/avro-format';
import { Chunk, AvroMessage } from './message';

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

      consumerStream.emit('avro', <AvroMessage>{
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
