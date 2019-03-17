import { loadSchemas } from '../../src/schema-registry';
import { producerStream, produce, DEFAULT_PARTITION } from '../../src/kafka';
import {
  KAFKA_BROKER,
  SCHEMA_REGISTRY_URL,
  TOPIC_NAME,
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
  const result = await loadSchemasForTopic();

  if (!result) {
    throw new Error('Could not upload schemas to schema registry. Exiting...');
  }

  const schemas = await loadSchemas(
    SCHEMA_REGISTRY_URL,
    TOPIC_NAME,
    'latest'
  );

  const stream = producerStream(producerOpts, {}, {});

  for (const value of TOPIC_VALUES) {
    produce(
      stream,
      schemas,
      TOPIC_NAME,
      value,
      TOPIC_KEYS[0],
      DEFAULT_PARTITION
    );
  }

  stream.close();
};
