import { loadSchemas } from '../lib/schema-registry';
import { consumerStream, AvroMessage } from '../lib/kafka';

const consumerOpts = {
  'metadata.broker.list': 'kafka:9092',
  'group.id': 'my-group-id',
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
  topics: ['simple-consumer-topic'],
};

const startConsumer = async () => {
  const schemas = await loadSchemas(
    'schema-registry:8081',
    'simple-consumer-topic',
    'latest'
  );

  const stream = consumerStream(consumerOpts, consumerOffset, streamOptions, schemas);

  stream.on('avro', (data: AvroMessage) => {
    console.log(`Received Message! (Offset: ${data.offset})`);
    console.log(`Value: ${data.value}`);
    console.log(`Key: ${data.key}`);

    stream.consumer.commitMessage(data);
  });
};

startConsumer();
