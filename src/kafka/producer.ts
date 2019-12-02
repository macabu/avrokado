import { Producer } from 'node-rdkafka';

import { TopicsSchemas } from '../schema-registry';
import { encodeAvroChunk } from '../avro/avro-format';

export const DEFAULT_PARTITION = -1;

export class AvroProducer extends Producer {
  private schemas: TopicsSchemas;

  constructor(conf: Object, topicConf: Object, schemas: TopicsSchemas) {
    super(conf, topicConf);

    this.schemas = schemas;
    this.produce = this.newProduce;
    // FIXME: Find a way to 'force' type signature.
    this.connect = this.newConnect as any; // tslint:disable-line: no-any
    this.disconnect = this.newDisconnect as any; // tslint:disable-line: no-any
  }

  private async newProduce(
    topic: string,
    partition?: number,
    message?: unknown,
    key?: unknown,
    timestamp?: number,
    opaque?: unknown
  ) {
    const sendValue = this.encode(topic, 'value', message);
    const sendKey = this.encode(topic, 'key', key);

    return new Promise((resolve, reject) => {
      try {
        const produced = super.produce(topic, partition, sendValue, sendKey, timestamp, opaque);
        resolve(produced);
      } catch (err) {
        reject(err);
      }
    });
  }

  private async newConnect(metadataOption: Object = {}): Promise<true | Error> {
    return new Promise((resolve, reject) => {
      super.on('ready', () => resolve(true));

      return super.connect(metadataOption, (err, data) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(true);
        return;
      });
    });
  }

  private async newDisconnect(timeout: number = 5000): Promise<true | Error> {
    return new Promise((resolve, reject) => {
      super.on('disconnected', () => resolve(true));

      return super.disconnect(timeout, (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(true);
        return;
      });
    });
  }

  private encode = (topic: string, type: 'key' | 'value', data?: unknown) => {
    if (!this.schemas || !this.schemas[topic]) {
      throw new TypeError('Schema not found to serialize data');
    }

    const schema = type === 'key'
      ? this.schemas[topic].keySchema
      : this.schemas[topic].valueSchema;

    return encodeAvroChunk(schema, data);
  }
}
