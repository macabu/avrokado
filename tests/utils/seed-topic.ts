import { SchemaRegistry } from '../../src/schema-registry';
import { AvroProducer, DEFAULT_PARTITION } from '../../src/kafka';
import {
  KAFKA_BROKER,
  SCHEMA_REGISTRY_URL,
  TOPIC_VALUES,
  TOPIC_KEYS,
} from './constant';
import {
  loadSchemasForTopic,
} from './create-schema';

const producerOpts = {
  'metadata.broker.list': KAFKA_BROKER,
  'socket.nagle.disable': true,
  'socket.keepalive.enable': true,
  'log.connection.close': false,
};

export const seedTopic = async () => {
  const topic = 'random-topic';

  await loadSchemasForTopic(topic);

  const sr = new SchemaRegistry(
    SCHEMA_REGISTRY_URL,
    topic,
    'latest'
  );

  await sr.load();

  const producer = new AvroProducer(producerOpts, {}, sr.schemas);

  await producer.connect();

  for (const value of TOPIC_VALUES) {
    await producer.produce(
      topic,
      DEFAULT_PARTITION,
      value,
      TOPIC_KEYS[0]
    );
  }

  await producer.poll();

  await producer.disconnect();

  return topic;
};
