import { createWriteStream } from 'node-rdkafka';

import { loadSchemasForTopic, cleanupSchemas } from '../../utils/create-schema';
import { loadSchemas } from '../../../src/schema-registry';
import { producerStream, produce, DEFAULT_PARTITION } from '../../../src/kafka/producer';
import { KAFKA_BROKER, SCHEMA_REGISTRY_URL, TOPIC_NAME, TOPIC_VALUES } from '../../utils/constant';

describe('E2E Test : src/kafka/producer.ts', () => {
  beforeAll(async () => {
    await cleanupSchemas();

    const result = await loadSchemasForTopic();
    if (!result) {
      throw new Error('Could not upload schemas to schema registry. Exiting...');
    }

    jest.setTimeout(30000);
  });

  describe('produce', () => {
    test('With valid schemas for both key and value', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = TOPIC_VALUES.shift();

      const key = 'my-key';

      const stream = producerStream(producerOpts, {});

      const result = produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key,
        DEFAULT_PARTITION
      );

      stream.close();

      expect(result).toBeTruthy();
    });

    test('With valid schemas and default options', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = TOPIC_VALUES.shift();

      const key = 'my-key';

      const stream = producerStream(producerOpts);

      const result = produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key
      );

      stream.close();

      expect(result).toBeTruthy();
    });

    test('With buffer for value and key', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = Buffer.from('my-value');

      const key = Buffer.from('my-key');

      const stream = producerStream(producerOpts);

      const result = produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key,
        DEFAULT_PARTITION,
        true
      );

      stream.close();

      expect(result).toBeTruthy();
    });

    test('With buffer for value and no key', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = Buffer.from('my-value');

      const key = null;

      const stream = producerStream(producerOpts);

      const result = produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key,
        DEFAULT_PARTITION,
        true
      );

      stream.close();

      expect(result).toBeTruthy();
    });

    test('With valid schemas and non-default options', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = TOPIC_VALUES.shift();

      const key = 'my-key';

      const stream = producerStream(producerOpts, {}, {}, createWriteStream);

      const result = produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key,
        DEFAULT_PARTITION
      );

      stream.close();

      expect(result).toBeTruthy();
    });

    test('With invalid schema for key with fallback on', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = TOPIC_VALUES.shift();

      const key = {
        invalid: 'yes',
      };

      const stream = producerStream(producerOpts, {}, {});

      const result = produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key,
        DEFAULT_PARTITION,
        true
      );

      stream.close();

      expect(result).toBeTruthy();
    });

    test('With invalid schema for key with fallback off', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = TOPIC_VALUES.shift();

      const key = {
        invalid: 'yes',
      };

      const stream = producerStream(producerOpts, {}, {});

      expect(() => produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key,
        DEFAULT_PARTITION
      )).toThrow();

      stream.close();
    });

    test('With invalid schema for value with fallback on', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = {
        invalid: 'schema',
      };

      const key = 'test';

      const stream = producerStream(producerOpts, {}, {});

      const result = produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key,
        DEFAULT_PARTITION,
        true
      );

      stream.close();

      expect(result).toBeTruthy();
    });

    test('With valid schemas for both key and value with additional metadata', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      const value = TOPIC_VALUES.shift();

      const key = 'my-key';

      const stream = producerStream(producerOpts, {}, {});

      await new Promise(resolve => setTimeout(resolve, 2000));

      const result = produce(
        stream,
        schemas,
        TOPIC_NAME,
        value,
        key,
        DEFAULT_PARTITION,
        true,
        new Date().getTime(),
        {}
      );

      stream.close();

      expect(result).toBeTruthy();
    });
  });

  afterAll(() => cleanupSchemas());
});
