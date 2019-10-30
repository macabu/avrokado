import { Readable } from 'stream';
import { createReadStream, ConsumerStream } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry';
import { decodeAvroChunk, DecodedAvroChunk } from '../avro/avro-format';
import { Chunk, AvroMessage } from './message';

// Need to extend from ConsumerStream, however it is not exported as a class,
// but an interface. But I don't want to implement its methods, only inherit.
export class AvroConsumer extends Readable {
  private schemas: TopicsSchemas;
  public stream: ConsumerStream;

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

  public close(cb?: Function) {
    this.stream.close(cb);
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
