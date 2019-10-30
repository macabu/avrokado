import { SchemaRegistry } from '../lib/schema-registry';
import { AvroConsumer, AvroMessage } from '../lib/kafka';

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
  const sr = new SchemaRegistry(
    'schema-registry:8081',
    'simple-consumer-topic',
    'latest'
  );

  await sr.load();

  const avroConsumer = new AvroConsumer(consumerOpts, consumerOffset, streamOptions, sr.schemas);

  avroConsumer.on('avro', (data: AvroMessage) => {
    console.log(`Received Message! (Offset: ${data.offset})`);
    console.log(`Value: ${data.value}`);
    console.log(`Key: ${data.key}`);

    // This is ugly.
    avroConsumer.stream.consumer.commitMessage(data);
  });
};

startConsumer();
