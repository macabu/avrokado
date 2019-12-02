import { loadSchemasForTopic, cleanupSchemas } from '../../utils/create-schema';
import { SchemaRegistry } from '../../../src/schema-registry';
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

      const sr = new SchemaRegistry(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      await sr.load();

      const value = TOPIC_VALUES.shift();

      const key = 'my-key';

      const producer = new AvroProducer(producerOpts, {}, sr.schemas);

      await producer.connect();

      const result = await producer.produce(
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

      const sr = new SchemaRegistry(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      await sr.load();

      const value = TOPIC_VALUES.shift();

      const key = 'my-key';

      const producer = new AvroProducer(producerOpts, {}, sr.schemas);

      await producer.connect();

      const result = await producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      );

      await producer.disconnect();

      expect(result).toBeTruthy();
    });

    test('With invalid schema for key', async () => {
      const producerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
      };

      const sr = new SchemaRegistry(
        SCHEMA_REGISTRY_URL,
        TOPIC_NAME,
        'latest'
      );

      await sr.load();

      const value = TOPIC_VALUES.shift();

      const key = {
        invalid: 'yes',
      };

      const producer = new AvroProducer(producerOpts, {}, sr.schemas);

      await producer.connect();

      expect(producer.produce(
        TOPIC_NAME,
        DEFAULT_PARTITION,
        value,
        key
      )).rejects.toThrow();

      await producer.disconnect();
    });
  });

  afterAll(() => cleanupSchemas());
});
