import { loadSchemas } from '../lib/schema-registry';
import { AvroProducer, DEFAULT_PARTITION } from '../lib/kafka';

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

  const message = {
    cat: 'dog',
  };

  const key = 'my-key';

  const producer = new AvroProducer(producerOpts, {}, schemas);

  await producer.connect();

  producer.produce(
    'test',
    DEFAULT_PARTITION,
    message,
    key,
    Date.now()
  );

  await producer.disconnect();
};

startProducer();
