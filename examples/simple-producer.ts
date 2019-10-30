import { SchemaRegistry } from '../lib/schema-registry';
import { AvroProducer, DEFAULT_PARTITION } from '../lib/kafka';

const producerOpts = {
  'metadata.broker.list': 'kafka:9092',
  'socket.nagle.disable': true,
  'socket.keepalive.enable': true,
  'log.connection.close': false,
};

const startProducer = async () => {
  const sr = new SchemaRegistry(
    'schema-registry:8081',
    'simple-producer-topic',
    'latest'
  );

  await sr.load();

  const message = {
    cat: 'dog',
  };

  const key = 'my-key';

  const producer = new AvroProducer(producerOpts, {}, sr.schemas);

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
