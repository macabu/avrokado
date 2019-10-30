import { Readable } from 'stream';
import { createReadStream, ConsumerStream } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry/load-schemas';
import { decodeAvroChunk, DecodedAvroChunk } from '../schema-registry/avro-format';
import { Chunk, AvroMessage } from './message';

export class AvroConsumer extends Readable {
  private schemas: TopicsSchemas;
  private stream: ConsumerStream;

  constructor(conf: Object, topicConf: Object, streamOpts: Object, schemas: TopicsSchemas) {
    super();

    this.stream = createReadStream(conf, topicConf, streamOpts);
    this.schemas = schemas;

    this.emit('ready');

    this.stream.on('data', (chunk: Chunk) => this.decoder(chunk));
    this.stream.on('error', (err: Error) => this.emit('error', err));
    this.stream.consumer.on('event.error', (err: Error) => this.emit('event.error', err));
    this.stream.on('end', () => this.emit('end'));
  }

  public close() {
    this.stream.close();
  }

  private decoder(chunk: Chunk) {
    if (this.schemas[chunk.topic]) {
      const valueSchema = this.schemas[chunk.topic].valueSchema;
      const keySchema = this.schemas[chunk.topic].keySchema;

      const {
        decoded: parsedValue,
        schemaId: valueSchemaId,
      } = <DecodedAvroChunk>decodeAvroChunk(valueSchema, chunk.value);

      const {
        decoded: parsedKey,
        schemaId: keySchemaId,
      } = <DecodedAvroChunk>decodeAvroChunk(keySchema, chunk.key);

      this.emit('avro', <AvroMessage>{
        ...chunk,
        parsedValue,
        parsedKey,
        valueSchemaId,
        keySchemaId,
      });
    }
  }
}
