export const TOPIC_NAME = 'dogs';
export const SCHEMA_REGISTRY_URL = 'http://localhost:8081';
export const KAFKA_BROKER = 'localhost:9092';
export const TOPIC_VALUES = [
  {
    name: 'Hound',
    age: 4,
    breed: 'Beagle',
    owner: {
      string: 'Alex',
    },
    createdAt: new Date().getTime(),
  },
  {
    name: 'Kiara',
    age: 2,
    breed: 'Beagle',
    owner: null,
    createdAt: new Date().getTime(),
  },
];
export const TOPIC_KEYS = [
  'my-first-key',
  'my-second-key',
];
