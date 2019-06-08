import { Producer } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry/load-schemas';
import { encodeAvroChunk } from '../schema-registry/avro-format';

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

export class AvroProducer extends Producer {
  private schemas: TopicsSchemas;
  private fallback: boolean;
  private oldProduce: (
    topic: string,
    partition?: number,
    message?: unknown,
    key?: unknown,
    timestamp?: number,
    opaque?: unknown
  ) => void;
  private oldConnect: (
    metadataOptions?: Object,
    cb?: ((err: Error, data: unknown) => void) | undefined
  ) => unknown;
  private oldDisconnect: (
    metadataOptions?: Object,
    cb?: ((err: Error, data: unknown) => void) | undefined
  ) => unknown;

  constructor(conf: Object, topicConf: Object, schemas: TopicsSchemas, fallback?: boolean) {
    super(conf, topicConf);

    this.schemas = schemas;
    this.fallback = !!fallback;

    this.oldProduce = super.produce;
    this.produce = this.newProduce;

    // FIXME: Find a way to 'force' type signature.
    this.oldConnect = super.connect as any;
    this.connect = this.newConnect as any;

    // FIXME: Find a way to 'force' type signature.
    this.oldDisconnect = super.disconnect as any;
    this.disconnect = this.newDisconnect as any;
  }

  private newProduce(
    topic: string,
    partition?: number,
    message?: unknown,
    key?: unknown,
    timestamp?: number,
    opaque?: unknown
  ) {
    const sendValue = encodeWithSchema(this.schemas, topic, 'value', message, this.fallback);
    const sendKey = encodeWithSchema(this.schemas, topic, 'key', key, this.fallback);

    return this.oldProduce(topic, partition, sendValue, sendKey, timestamp, opaque);
  }

  private newConnect(metadataOption: Object = {}): Promise<true | Error> {
    return new Promise((resolve, reject) => {
      this.on('ready', () => {
        resolve(true);
      });

      this.oldConnect(metadataOption, (err) => {
        reject(err);
      });
    });
  }

  private newDisconnect(timeout: number = 5000): Promise<true | Error> {
    return new Promise((resolve, reject) => {
      this.on('disconnected', () => {
        resolve(true);
      });

      this.oldDisconnect(timeout, (err) => {
        reject(err);
      });
    });
  }
}
