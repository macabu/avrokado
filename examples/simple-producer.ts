import { loadSchemas } from '../src/core/load-schemas';
import { producerStream, produce, DEFAULT_PARTITION } from '../src/kafka/producer';

const producerOpts = {
  'metadata.broker.list': 'kafka:9092',
  'socket.nagle.disable': true,
  'socket.keepalive.enable': true,
  'log.connection.close': false,
};

const startProducer = async () => {
  const schemas = await loadSchemas(
    'schema-registry:8081',
    'simple-producer-topic',
    'latest'
  );

  const value = {
    test: 'hello',
  };

  const key = 'my-key';

  const stream = producerStream(producerOpts, {}, {});

  produce(
    stream,
    schemas,
    'simple-producer-topic',
    value,
    key,
    DEFAULT_PARTITION
  );

  stream.close();
};

startProducer();
