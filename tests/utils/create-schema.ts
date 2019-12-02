import got from 'got';

import { SCHEMA_REGISTRY_URL, TOPIC_NAME } from './constant';
import { keySchema } from './schemas/mocktopic-key';
import { valueSchema } from './schemas/mocktopic-value';

export const loadSchemasForTopic = async (topic: string = TOPIC_NAME) => {
  const resultValue =
    await uploadSchema(SCHEMA_REGISTRY_URL, topic, JSON.stringify(valueSchema), 'value');
  const resultKey =
    await uploadSchema(SCHEMA_REGISTRY_URL, topic, JSON.stringify(keySchema), 'key');

  return (resultValue && resultKey);
};

export const cleanupSchemas = async (topic: string = TOPIC_NAME) => {
  const resultValue = await deleteSchema(topic, 'value');
  const resultKey = await deleteSchema(topic, 'key');

  return (resultValue && resultKey);
};

export const uploadSchema = async (
  schemaRegistry: string,
  topic: string,
  schema: string,
  type: 'key' | 'value'
) => {
  try {
    await got.post(`${schemaRegistry}/subjects/${topic}-${type}/versions`, {
      json:{
        schema,
      },
      headers: {
        'Content-Type': 'application/vnd.schemaregistry.v1+json',
      },
    });

    return true;
  } catch (err) {
    return false;
  }
};

const deleteSchema = async (
  topic: string,
  type: 'key' | 'value'
) => {
  try {
    await got.delete(`${SCHEMA_REGISTRY_URL}/subjects/${topic}-${type}`);

    return true;
  } catch (err) {
    return false;
  }
};
