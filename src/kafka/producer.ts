import { Producer } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry';
import { encodeAvroChunk } from '../schema-registry/avro-format';

export const DEFAULT_PARTITION = -1;

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
    this.oldConnect = super.connect as any; // tslint:disable-line: no-any
    this.connect = this.newConnect as any; // tslint:disable-line: no-any

    // FIXME: Find a way to 'force' type signature.
    this.oldDisconnect = super.disconnect as any; // tslint:disable-line: no-any
    this.disconnect = this.newDisconnect as any; // tslint:disable-line: no-any
  }

  private newProduce(
    topic: string,
    partition?: number,
    message?: unknown,
    key?: unknown,
    timestamp?: number,
    opaque?: unknown
  ) {
    const sendValue = this.encode(topic, 'value', message);
    const sendKey = this.encode(topic, 'key', key);

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

  private encode = (topic: string, type: 'key' | 'value', data?: unknown) => {
    let encoded = Buffer.alloc(0);

    if (this.schemas && this.schemas[topic]) {
      try {
        const schema = type === 'key'
          ? this.schemas[topic].keySchema
          : this.schemas[topic].valueSchema;

        encoded = encodeAvroChunk(schema, data);
      } catch (err) {
        if (!this.fallback) {
          throw err;
        }
      }
    }

    let send = encoded;
    if (!encoded.length) {
      if (!this.fallback) {
        throw new TypeError('Schema not found to serialize data');
      }

      send = data
        ? Buffer.isBuffer(data)
          ? data
          : Buffer.from(JSON.stringify(data))
        : Buffer.alloc(0);
    }

    return send;
  }
}
