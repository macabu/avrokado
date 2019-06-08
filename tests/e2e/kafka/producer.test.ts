import { loadSchemasForTopic, cleanupSchemas } from '../../utils/create-schema';
import { loadSchemas } from '../../../src/schema-registry';
import { AvroProducer, DEFAULT_PARTITION } from '../../../src/kafka/producer';
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

      const producer = new AvroProducer(producerOpts, {}, schemas);

      await producer.connect();

      const result = producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      );

      await producer.disconnect();

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

      const producer = new AvroProducer(producerOpts, {}, schemas);

      await producer.connect();

      const result = producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      );

      await producer.disconnect();

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

      const producer = new AvroProducer(producerOpts, {}, schemas, true);

      await producer.connect();

      const result = producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      );

      await producer.disconnect();

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

      const producer = new AvroProducer(producerOpts, {}, schemas, true);

      await producer.connect();

      const result = producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      );

      await producer.disconnect();

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

      const producer = new AvroProducer(producerOpts, {}, schemas, true);

      await producer.connect();

      const result = producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      );

      await producer.disconnect();

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

      const fallback = false;

      const producer = new AvroProducer(producerOpts, {}, schemas, fallback);

      await producer.connect();

      expect(() => producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      )).toThrow();

      await producer.disconnect();
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

      const producer = new AvroProducer(producerOpts, {}, schemas, true);

      await producer.connect();

      const result = producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      );

      await producer.disconnect();

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

      const producer = new AvroProducer(producerOpts, {}, schemas, true);

      await producer.connect();

      await new Promise(resolve => setTimeout(resolve, 2000));

      const result = producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key,
        new Date().getTime(),
        {}
      );

      await producer.disconnect();

      expect(result).toBeTruthy();
    });
  });

  afterAll(() => cleanupSchemas());
});
