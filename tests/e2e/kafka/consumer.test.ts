import { loadSchemas } from '../../../src/schema-registry';
import { consumerStream, AvroMessage } from '../../../src/kafka';
import { KAFKA_BROKER, SCHEMA_REGISTRY_URL } from '../../utils/constant';
import { seedTopic } from '../../utils/seed-topic';

describe('E2E Test : src/kafka/consumer.ts', () => {
  describe('consumerStream', () => {
    test('With valid schemas for both key and value', async () => {
      const topic = await seedTopic();

      const schemas = await loadSchemas(
        SCHEMA_REGISTRY_URL,
        topic,
        'latest'
      );

      const consumerOpts = {
        'metadata.broker.list': KAFKA_BROKER,
        'group.id': `group-id-${Math.random() * 100}`,
        'socket.nagle.disable': true,
        'socket.keepalive.enable': true,
        'enable.auto.commit': false,
        'enable.auto.offset.store': true,
        'log.connection.close': false,
      };

      const consumerOffset = {
        'auto.offset.reset': 'earliest',
      };

      const streamOptions = {
        topics: [topic],
      };

      const stream = consumerStream(consumerOpts, consumerOffset, streamOptions, schemas);

      const consumed = <AvroMessage[]>[];

      consumed.push(await new Promise((resolve, reject) => {
        stream.on('avro', (data) => {
          resolve(data);
        });

        stream.on('error', (err) => {
          reject(err);
        });
      }));

      stream.close();

      const expectedValue = {
        name: 'Hound',
        age: 4,
        breed: 'Beagle',
        owner: {
          string: 'Alex',
        },
      };

      expect(consumed[0].parsedKey).toStrictEqual('my-first-key');
      expect(consumed[0].parsedValue).toMatchObject(expectedValue);
    }, 30000);
  });
});
